import { it, expect } from "vitest";
import { env, SELF } from "cloudflare:test";
import type { RepoDurableObject } from "@/index";
import * as git from "isomorphic-git";
import { asTypedStorage, RepoStateSchema } from "@/do/repo/repoState.ts";
import {
  concatChunks,
  createMemPackFs,
  delimPkt,
  encodeObjHeader,
  flushPkt,
  pktLine,
  decodePktLines,
} from "@/git";
import { uniqueRepoId, runDOWithRetry, callStubWithRetry } from "./util/test-helpers.ts";
import { getDb, insertPackOids } from "@/do/repo/db/index.ts";
import { asBufferSource, deflate, inflate } from "@/common/index.ts";
import { doPrefix } from "@/keys.ts";

async function readLoose(
  getStub: () => DurableObjectStub<RepoDurableObject>,
  oid: string
): Promise<{ type: string; payload: Uint8Array }> {
  const obj = await callStubWithRetry<ArrayBuffer | Uint8Array | null>(getStub, (s) =>
    s.getObject(oid)
  );
  if (!obj) throw new Error("missing loose " + oid);
  const z = obj instanceof Uint8Array ? obj : new Uint8Array(obj);
  const raw = await inflate(z);
  // parse header: "type size\0"
  let p = 0;
  while (p < raw.length && raw[p] !== 0x20) p++;
  const type = new TextDecoder().decode(raw.subarray(0, p));
  let nul = p + 1;
  while (nul < raw.length && raw[nul] !== 0x00) nul++;
  const payload = raw.subarray(nul + 1);
  return { type, payload };
}

async function buildPack(objs: { type: string; payload: Uint8Array }[]): Promise<Uint8Array> {
  // PACK header
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

// Use the repo-side mem fs that implements the full interface isomorphic-git expects

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

  // Seed DO repo with a commit + empty tree as loose objects only
  const { commitOid, treeOid } = await runDOWithRetry(
    getStub,
    async (instance: RepoDurableObject) => {
      return instance.seedMinimalRepo(false); // Don't create a pack
    }
  );

  // Read loose objects
  const commit = await readLoose(getStub, commitOid);
  const tree = await readLoose(getStub, treeOid);

  // Build two packs: A(commit), B(tree)
  const packA = await buildPack([commit]);
  const packB = await buildPack([tree]);

  // Create idx for both using isomorphic-git indexPack
  const filesA = new Map<string, Uint8Array>();
  const fsA = createMemPackFs(filesA);
  await fsA.promises.writeFile("/git/objects/pack/pack-a.pack", packA);
  await git.indexPack({ fs: fsA as any, dir: "/git", filepath: "objects/pack/pack-a.pack" } as any);
  const idxA = filesA.get("/git/objects/pack/pack-a.idx");
  if (!idxA) throw new Error("failed to create idxA");

  const filesB = new Map<string, Uint8Array>();
  const fsB = createMemPackFs(filesB);
  await fsB.promises.writeFile("/git/objects/pack/pack-b.pack", packB);
  await git.indexPack({ fs: fsB as any, dir: "/git", filepath: "objects/pack/pack-b.pack" } as any);
  const idxB = filesB.get("/git/objects/pack/pack-b.idx");
  if (!idxB) throw new Error("failed to create idxB");

  let prefix = "";
  await runDOWithRetry(getStub, async (_instance, state: DurableObjectState) => {
    prefix = doPrefix(state.id.toString());
  });

  // Upload pack+idx to R2 under the repo prefix so catalog backfill can
  // seed `pack_catalog` from the same R2 layout used in production.
  const keyA = `${prefix}/objects/pack/pack-a.pack`;
  const keyB = `${prefix}/objects/pack/pack-b.pack`;
  await env.REPO_BUCKET.put(keyA, packA);
  await env.REPO_BUCKET.put(keyA.replace(/\.pack$/, ".idx"), idxA);
  await env.REPO_BUCKET.put(keyB, packB);
  await env.REPO_BUCKET.put(keyB.replace(/\.pack$/, ".idx"), idxB);

  // Register pack metadata in DO storage
  await runDOWithRetry(getStub, async (_instance, state: DurableObjectState) => {
    const store = asTypedStorage<RepoStateSchema>(state.storage);
    await store.put("packList", [keyA, keyB]);

    // Insert pack OIDs into SQLite via DAL
    const db = getDb(state.storage);
    await insertPackOids(db, keyA, [commitOid]);
    await insertPackOids(db, keyB, [treeOid]);
  });

  await callStubWithRetry(getStub, (stub) => stub.getActivePackCatalog());

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

  // Strong validation: index the returned pack and assert expected OIDs are present
  const filesC = new Map<string, Uint8Array>();
  const fsC = createMemPackFs(filesC);
  await fsC.promises.writeFile("/git/objects/pack/resp.pack", packOut);
  const { oids: outOids } = await git.indexPack({
    fs: fsC as any,
    dir: "/git",
    filepath: "objects/pack/resp.pack",
  } as any);
  expect(outOids).toContain(commitOid);
  expect(outOids).toContain(treeOid);
});
