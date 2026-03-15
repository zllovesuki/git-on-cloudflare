import { it, expect } from "vitest";
import { env } from "cloudflare:test";
import { computeNeeded } from "@/git";
import type { RepoDurableObject } from "@/index";
import { uniqueRepoId, runDOWithRetry, withEnvOverrides } from "./util/test-helpers.ts";

async function deflateRaw(data: Uint8Array): Promise<Uint8Array> {
  const cs: any = new (globalThis as any).CompressionStream("deflate");
  const stream = new Blob([data]).stream().pipeThrough(cs);
  const buf = await new Response(stream).arrayBuffer();
  return new Uint8Array(buf);
}

async function encodeGitObjectAndDeflate(
  type: "blob" | "tree" | "commit" | "tag",
  payload: Uint8Array
) {
  const header = new TextEncoder().encode(`${type} ${payload.byteLength}\0`);
  const raw = new Uint8Array(header.byteLength + payload.byteLength);
  raw.set(header, 0);
  raw.set(payload, header.byteLength);
  const hash = await crypto.subtle.digest("SHA-1", raw);
  const oid = Array.from(new Uint8Array(hash))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
  const zdata = await deflateRaw(raw);
  return { oid, zdata };
}

async function putObj(
  getStub: () => DurableObjectStub<RepoDurableObject>,
  oid: string,
  z: Uint8Array
) {
  await runDOWithRetry(getStub, async (instance: RepoDurableObject) => {
    await instance.putLooseObject(oid, z);
  });
}

it("computeNeeded prunes by have closure in merge DAG and trims haves > 128", async () => {
  await withEnvOverrides(env as Env, { LOG_LEVEL: "error" }, async () => {
    const owner = "o";
    const repo = uniqueRepoId("r-compute-merge");
    const repoId = `${owner}/${repo}`;
    const id = env.REPO_DO.idFromName(repoId);
    const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;

    // Build a shared empty tree T
    const treePayload = new Uint8Array(0);
    const { oid: treeOid, zdata: treeZ } = await encodeGitObjectAndDeflate("tree", treePayload);
    await putObj(getStub, treeOid, treeZ);

    // Root commit R
    const author = `You <you@example.com> 0 +0000`;
    const committer = author;
    const msg = "root\n";
    const rootPayload = new TextEncoder().encode(
      `tree ${treeOid}\n` + `author ${author}\n` + `committer ${committer}\n\n${msg}`
    );
    const { oid: rootOid, zdata: rootZ } = await encodeGitObjectAndDeflate("commit", rootPayload);
    await putObj(getStub, rootOid, rootZ);

    // Branch A: commit A -> parent R, same tree T
    const aPayload = new TextEncoder().encode(
      `tree ${treeOid}\n` +
        `parent ${rootOid}\n` +
        `author ${author}\n` +
        `committer ${committer}\n\nA\n`
    );
    const { oid: aOid, zdata: aZ } = await encodeGitObjectAndDeflate("commit", aPayload);
    await putObj(getStub, aOid, aZ);

    // Branch B: commit B -> parent R, same tree T
    const bPayload = new TextEncoder().encode(
      `tree ${treeOid}\n` +
        `parent ${rootOid}\n` +
        `author ${author}\n` +
        `committer ${committer}\n\nB\n`
    );
    const { oid: bOid, zdata: bZ } = await encodeGitObjectAndDeflate("commit", bPayload);
    await putObj(getStub, bOid, bZ);

    // Merge M: parents A and B, same tree T
    const mPayload = new TextEncoder().encode(
      `tree ${treeOid}\n` +
        `parent ${aOid}\n` +
        `parent ${bOid}\n` +
        `author ${author}\n` +
        `committer ${committer}\n\nmerge\n`
    );
    const { oid: mOid, zdata: mZ } = await encodeGitObjectAndDeflate("commit", mPayload);
    await putObj(getStub, mOid, mZ);

    // Case 1: want B, have A -> need only B (tree/root pruned via have closure)
    const need1 = await computeNeeded(env as any, repoId, [bOid], [aOid]);
    expect(need1).toContain(bOid);
    expect(need1).not.toContain(treeOid);
    expect(need1).not.toContain(rootOid);

    // Case 2: want M, have A -> need M and B (A is known; B not in have closure; tree/root pruned)
    const need2 = await computeNeeded(env as any, repoId, [mOid], [aOid]);
    expect(need2).toContain(mOid);
    expect(need2).toContain(bOid);
    expect(need2).not.toContain(treeOid);
    expect(need2).not.toContain(rootOid);

    // Case 3: have-list trimming: if B is past index 128, it is ignored and still needed
    const dummies: string[] = Array.from(
      { length: 128 },
      (_, i) => "deadbeefdeadbeefdeadbeefdeadbeefdeadbee" + (i % 10)
    );
    const need3 = await computeNeeded(env as any, repoId, [bOid], [...dummies, bOid]);
    expect(need3).toContain(bOid);
  });
}, 15000);
