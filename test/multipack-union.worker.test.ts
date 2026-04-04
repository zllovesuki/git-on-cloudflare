import { it, expect } from "vitest";
import { env, SELF } from "cloudflare:test";
import type { RepoDurableObject } from "@/index";
import { asTypedStorage } from "@/do/repo/repoState.ts";
import type { RepoStateSchema } from "@/do/repo/repoState.ts";
import {
  concatChunks,
  delimPkt,
  encodeGitObject,
  encodeObjHeader,
  flushPkt,
  pktLine,
  decodePktLines,
} from "@/git";
import { uniqueRepoId, runDOWithRetry } from "./util/test-helpers.ts";
import { getDb, upsertPackCatalogRow } from "@/do/repo/db/index.ts";
import { asBufferSource, deflate } from "@/common/index.ts";
import { doPrefix, r2PackKey } from "@/keys.ts";
import { indexTestPack } from "./util/test-indexer.ts";
import { bytesToHex } from "@/common/hex.ts";

async function buildPack(objs: { type: string; payload: Uint8Array }[]): Promise<Uint8Array> {
  const hdr = new Uint8Array(12);
  hdr.set(new TextEncoder().encode("PACK"), 0);
  const dv = new DataView(hdr.buffer);
  dv.setUint32(4, 2);
  dv.setUint32(8, objs.length);
  const parts: Uint8Array[] = [hdr];
  for (const o of objs) {
    const typeCode = o.type === "commit" ? 1 : o.type === "tree" ? 2 : o.type === "blob" ? 3 : 4;
    parts.push(encodeObjHeader(typeCode, o.payload.byteLength));
    parts.push(await deflate(o.payload));
  }
  const body = concatChunks(parts);
  const sha = new Uint8Array(await crypto.subtle.digest("SHA-1", asBufferSource(body)));
  const out = new Uint8Array(body.byteLength + 20);
  out.set(body, 0);
  out.set(sha, body.byteLength);
  return out;
}

function buildFetchBody({
  wants,
  haves,
  done,
}: {
  wants: string[];
  haves?: string[];
  done?: boolean;
}) {
  const chunks: Uint8Array[] = [];
  chunks.push(pktLine("command=fetch\n"));
  chunks.push(delimPkt());
  for (const w of wants) chunks.push(pktLine(`want ${w}\n`));
  for (const h of haves || []) chunks.push(pktLine(`have ${h}\n`));
  if (done) chunks.push(pktLine("done\n"));
  chunks.push(flushPkt());
  return concatChunks(chunks);
}

it("multi-pack union assembles packfile from two R2 packs", async () => {
  const owner = "o";
  const repo = uniqueRepoId("r-multipack");
  const repoId = `${owner}/${repo}`;
  const id = env.REPO_DO.idFromName(repoId);
  const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;

  // Create objects directly — no seedMinimalRepo needed
  const treePayload = new Uint8Array(0); // empty tree
  const { oid: treeOid } = await encodeGitObject("tree", treePayload);

  const author = "You <you@example.com> 0 +0000";
  const commitPayload = new TextEncoder().encode(
    `tree ${treeOid}\nauthor ${author}\ncommitter ${author}\n\ninitial\n`
  );
  const { oid: commitOid } = await encodeGitObject("commit", commitPayload);

  // Build two packs: A(commit), B(tree) — objects split across packs
  const packA = await buildPack([{ type: "commit", payload: commitPayload }]);
  const packB = await buildPack([{ type: "tree", payload: treePayload }]);

  // Upload packs, index with streaming indexer, register in pack_catalog
  await runDOWithRetry(getStub, async (_instance, state: DurableObjectState) => {
    const prefix = doPrefix(state.id.toString());
    const store = asTypedStorage<RepoStateSchema>(state.storage);
    const db = getDb(state.storage);

    const keyA = r2PackKey(prefix, "pack-a.pack");
    const keyB = r2PackKey(prefix, "pack-b.pack");
    await env.REPO_BUCKET.put(keyA, packA);
    await env.REPO_BUCKET.put(keyB, packB);

    const resolveA = await indexTestPack(env, keyA, packA.byteLength);
    const resolveB = await indexTestPack(env, keyB, packB.byteLength);

    await upsertPackCatalogRow(db, {
      packKey: keyA,
      kind: "receive",
      state: "active",
      tier: 0,
      seqLo: 1,
      seqHi: 1,
      objectCount: resolveA.objectCount,
      packBytes: packA.byteLength,
      idxBytes: resolveA.idxBytes,
      createdAt: Date.now(),
      supersededBy: null,
    });
    await upsertPackCatalogRow(db, {
      packKey: keyB,
      kind: "receive",
      state: "active",
      tier: 0,
      seqLo: 2,
      seqHi: 2,
      objectCount: resolveB.objectCount,
      packBytes: packB.byteLength,
      idxBytes: resolveB.idxBytes,
      createdAt: Date.now(),
      supersededBy: null,
    });

    await store.put("packsetVersion", 1);
    await store.put("nextPackSeq", 3);
    await store.put("refs", [{ name: "refs/heads/main", oid: commitOid }]);
    await store.put("head", { target: "refs/heads/main", oid: commitOid });
  });

  // Streaming v2: two-phase fetch. First negotiate (done=false)
  const url = `https://example.com/${owner}/${repo}/git-upload-pack`;
  const negotiate = await SELF.fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-git-upload-pack-request",
      "Git-Protocol": "version=2",
    },
    body: buildFetchBody({ wants: [commitOid], done: false }),
  } as any);
  expect(negotiate.status).toBe(200);
  const negoText = new TextDecoder().decode(new Uint8Array(await negotiate.arrayBuffer()));
  expect(negoText.includes("acknowledgments\n")).toBe(true);
  expect(negoText.includes("packfile\n")).toBe(false);

  // Final fetch (done=true) returns only packfile section
  const res = await SELF.fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-git-upload-pack-request",
      "Git-Protocol": "version=2",
    },
    body: buildFetchBody({ wants: [commitOid], done: true }),
  } as any);
  expect(res.status).toBe(200);
  const bytes = new Uint8Array(await res.arrayBuffer());

  // Extract sideband-encoded pack after the 'packfile' pkt-line
  const lines = decodePktLines(bytes);
  const packChunks: Uint8Array[] = [];
  let inPackfile = false;
  for (const line of lines) {
    if (line.type === "line" && line.text === "packfile\n") {
      inPackfile = true;
      continue;
    }
    if (inPackfile && line.type === "line" && line.raw && line.raw[0] === 0x01) {
      packChunks.push(line.raw.subarray(1));
    }
  }
  const packOut = concatChunks(packChunks);

  // Basic checks on assembled pack
  const td = new TextDecoder();
  expect(td.decode(packOut.subarray(0, 4))).toBe("PACK");
  const dv = new DataView(packOut.buffer, packOut.byteOffset, packOut.byteLength);
  expect(dv.getUint32(4, false)).toBe(2);
  expect(dv.getUint32(8, false)).toBe(2); // commit + tree

  // Strong validation: upload the returned pack to R2 and index it to verify OIDs
  const verifyKey = `verify/multipack-resp-${Date.now()}.pack`;
  await env.REPO_BUCKET.put(verifyKey, packOut);
  const verifyResult = await indexTestPack(env, verifyKey, packOut.byteLength);
  expect(verifyResult.objectCount).toBe(2);

  // Extract OIDs from the idx view's rawNames and check both objects are present
  const oidSet = new Set<string>();
  for (let i = 0; i < verifyResult.idxView.count; i++) {
    const oidBytes = verifyResult.idxView.rawNames.subarray(i * 20, (i + 1) * 20);
    oidSet.add(bytesToHex(oidBytes));
  }
  expect(oidSet.has(commitOid)).toBe(true);
  expect(oidSet.has(treeOid)).toBe(true);
});
