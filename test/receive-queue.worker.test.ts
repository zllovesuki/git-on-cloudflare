import { it, expect } from "vitest";
import { env, SELF } from "cloudflare:test";
import { decodePktLines, pktLine, flushPkt, concatChunks } from "@/git";
import { makeCommit, makeTree, zero40, encodeObjHeader } from "./util/test-helpers";
import { uniqueRepoId, runDOWithRetry, callStubWithRetry } from "./util/test-helpers";
import type { UnpackProgress } from "@/common";
import { asBufferSource, deflate } from "@/common/index.ts";

async function buildPack(
  objects: { type: "commit" | "tree" | "blob" | "tag"; payload: Uint8Array }[]
): Promise<Uint8Array> {
  const hdr = new Uint8Array(12);
  hdr.set(new TextEncoder().encode("PACK"), 0);
  const dv = new DataView(hdr.buffer);
  dv.setUint32(4, 2);
  dv.setUint32(8, objects.length);
  const parts: Uint8Array[] = [hdr];
  for (const o of objects) {
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

it("receive-pack: one-deep queue, third push blocked with 503", async () => {
  // Slow background unpack to ensure the window where unpackWork exists
  env.REPO_UNPACK_CHUNK_SIZE = "1";
  env.REPO_UNPACK_MAX_MS = "50";
  env.REPO_UNPACK_DELAY_MS = "10";
  env.REPO_UNPACK_BACKOFF_MS = "50";

  const owner = "o";
  const repo = uniqueRepoId("r-queue-503");
  const url = `https://example.com/${owner}/${repo}/git-receive-pack`;

  // Put the repo in legacy mode so pushes go through the buffered DO receive path
  const repoIdForMode = `${owner}/${repo}`;
  const getStubForMode = () => env.REPO_DO.get(env.REPO_DO.idFromName(repoIdForMode));
  await callStubWithRetry(getStubForMode as any, (s: any) => s.setRepoStorageMode("legacy"));

  // Build objects for three independent pushes
  const { oid: treeOid, payload: treePayload } = await makeTree();

  const firstPushCommits = await Promise.all(
    Array.from({ length: 128 }, (_, i) => makeCommit(treeOid, `A-${i}\n`))
  );
  const c1 = firstPushCommits[firstPushCommits.length - 1];
  const p1 = await buildPack([
    { type: "tree", payload: treePayload },
    ...firstPushCommits.map((commit) => ({ type: "commit" as const, payload: commit.payload })),
  ]);
  const body1 = concatChunks([
    pktLine(`${zero40()} ${c1.oid} refs/heads/a1\0 report-status ofs-delta agent=test\n`),
    flushPkt(),
    p1,
  ]);
  const res1 = await SELF.fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/x-git-receive-pack-request" },
    body: body1,
  } as any);
  expect(res1.status).toBe(200);
  const lines1 = decodePktLines(new Uint8Array(await res1.arrayBuffer()))
    .filter((i) => i.type === "line")
    .map((i: any) => i.text.trim());
  expect(lines1.some((l) => l.startsWith("unpack ok"))).toBe(true);

  // Second push should be accepted and staged as unpackNext
  const c2 = await makeCommit(treeOid, "B\n");
  const p2 = await buildPack([
    { type: "tree", payload: treePayload },
    { type: "commit", payload: c2.payload },
  ]);
  const body2 = concatChunks([
    pktLine(`${zero40()} ${c2.oid} refs/heads/a2\0 report-status ofs-delta agent=test\n`),
    flushPkt(),
    p2,
  ]);
  const res2 = await SELF.fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/x-git-receive-pack-request" },
    body: body2,
  } as any);
  expect(res2.status).toBe(200);

  // Ensure state reflects: unpacking in progress and exactly one queued
  const repoId = `${owner}/${repo}`;
  const id = env.REPO_DO.idFromName(repoId);
  const getStub = () => env.REPO_DO.get(id);
  const pre3 = await callStubWithRetry<UnpackProgress>(getStub as any, (s: any) =>
    s.getUnpackProgress()
  );
  let queued = pre3.unpacking === true && Number(pre3.queuedCount || 0) === 1;
  let current = pre3;
  for (let i = 0; i < 20 && !queued; i++) {
    await new Promise((resolve) => setTimeout(resolve, 5));
    current = await callStubWithRetry<UnpackProgress>(getStub as any, (s: any) =>
      s.getUnpackProgress()
    );
    queued = current.unpacking === true && Number(current.queuedCount || 0) === 1;
  }
  expect(queued).toBe(true);

  // Third push should be blocked by preflight (or DO guard) with 503
  const c3 = await makeCommit(treeOid, "C\n");
  const p3 = await buildPack([
    { type: "tree", payload: treePayload },
    { type: "commit", payload: c3.payload },
  ]);
  const body3 = concatChunks([
    pktLine(`${zero40()} ${c3.oid} refs/heads/a3\0 report-status ofs-delta agent=test\n`),
    flushPkt(),
    p3,
  ]);
  const res3 = await SELF.fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/x-git-receive-pack-request" },
    body: body3,
  } as any);
  expect(res3.status).toBe(503);

  // Drive alarm until everything completes
  // Drive until done

  const progress = async (): Promise<UnpackProgress> => {
    return callStubWithRetry<UnpackProgress>(getStub as any, (s: any) => s.getUnpackProgress());
  };

  let guard = 300;
  while (guard-- > 0) {
    await runDOWithRetry(getStub as any, async (instance: any) => {
      await instance.alarm();
    });
    const cur = await progress();
    if (!cur.unpacking) break;
  }
  const done = await progress();
  expect(done.unpacking).toBe(false);
});
