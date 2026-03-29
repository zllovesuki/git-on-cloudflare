import { describe, expect, it } from "vitest";
import { env, SELF } from "cloudflare:test";
import type { RepoDurableObject } from "@/index";
import type { RepoStateSchema } from "@/do/repo/repoState.ts";

import { buildPack, callStubWithRetry, runDOWithRetry, uniqueRepoId } from "./util/test-helpers.ts";
import { asTypedStorage, objKey } from "@/do/repo/repoState.ts";
import { encodeGitObject, concatChunks } from "@/git/core/index.ts";
import { indexPackOnly } from "@/git/pack/index.ts";
import { doPrefix, r2LooseKey, r2PackKey } from "@/keys.ts";
import { buildTreePayload } from "./util/packed-repo.ts";
import { buildFetchBody, findBytes } from "./util/fetch-protocol.ts";

async function seedPackedOnlyRepo(repoId: string) {
  const id = env.REPO_DO.idFromName(repoId);
  const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;
  const encoder = new TextEncoder();
  const author = "You <you@example.com> 0 +0000";

  const blob1Payload = encoder.encode("hello from v1\n");
  const blob1 = await encodeGitObject("blob", blob1Payload);
  const tree1Payload = buildTreePayload([{ mode: "100644", name: "hello.txt", oid: blob1.oid }]);
  const tree1 = await encodeGitObject("tree", tree1Payload);
  const commit1Payload = encoder.encode(
    `tree ${tree1.oid}\n` + `author ${author}\n` + `committer ${author}\n\n` + `first commit\n`
  );
  const commit1 = await encodeGitObject("commit", commit1Payload);

  const blob2Payload = encoder.encode("hello from v2\n");
  const blob2 = await encodeGitObject("blob", blob2Payload);
  const tree2Payload = buildTreePayload([{ mode: "100644", name: "hello.txt", oid: blob2.oid }]);
  const tree2 = await encodeGitObject("tree", tree2Payload);
  const commit2Payload = encoder.encode(
    `tree ${tree2.oid}\n` +
      `parent ${commit1.oid}\n` +
      `author ${author}\n` +
      `committer ${author}\n\n` +
      `second commit\n`
  );
  const commit2 = await encodeGitObject("commit", commit2Payload);

  const looseObjects = [blob1, tree1, commit1, blob2, tree2, commit2];
  const packs = [
    {
      name: "pack-receive-0002.pack",
      packBytes: await buildPack([
        { type: "blob", payload: blob2Payload },
        { type: "tree", payload: tree2Payload },
        { type: "commit", payload: commit2Payload },
      ]),
    },
    {
      name: "pack-receive-0001.pack",
      packBytes: await buildPack([
        { type: "blob", payload: blob1Payload },
        { type: "tree", payload: tree1Payload },
        { type: "commit", payload: commit1Payload },
      ]),
    },
  ];

  await runDOWithRetry(getStub, async (_instance, state) => {
    const prefix = doPrefix(state.id.toString());
    const store = asTypedStorage<RepoStateSchema>(state.storage);
    const packKeys: string[] = [];
    let lastPackOids: string[] = [];

    for (const obj of looseObjects) {
      await store.put(objKey(obj.oid), obj.zdata);
      await env.REPO_BUCKET.put(r2LooseKey(prefix, obj.oid), obj.zdata);
    }

    for (let i = 0; i < packs.length; i++) {
      const pack = packs[i];
      const packKey = r2PackKey(prefix, pack.name);
      packKeys.push(packKey);
      await env.REPO_BUCKET.put(packKey, pack.packBytes);
      const oids = await indexPackOnly(pack.packBytes, env, packKey, state, prefix);
      if (i === 0) lastPackOids = oids;
    }

    await store.put("lastPackKey", packKeys[0]);
    await store.put("lastPackOids", lastPackOids);
    await store.put("packList", packKeys);
    await store.put("refs", [{ name: "refs/heads/main", oid: commit2.oid }]);
    await store.put("head", { target: "refs/heads/main", oid: commit2.oid });
  });

  await callStubWithRetry(getStub, (stub) => stub.getActivePackCatalog());

  await runDOWithRetry(getStub, async (_instance, state) => {
    const prefix = doPrefix(state.id.toString());
    for (const obj of looseObjects) {
      await state.storage.delete(objKey(obj.oid));
      await env.REPO_BUCKET.delete(r2LooseKey(prefix, obj.oid));
    }
  });

  return {
    commit1,
    commit2,
    blob2,
    getStub,
  };
}

describe("pack-first fetch and UI", () => {
  it("serves UI routes from packs after loose copies are deleted", async () => {
    const owner = "o";
    const repo = uniqueRepoId("pack-first-ui");
    const repoId = `${owner}/${repo}`;
    const { commit2, blob2 } = await seedPackedOnlyRepo(repoId);

    const rawRes = await SELF.fetch(
      `https://example.com/${owner}/${repo}/raw?oid=${encodeURIComponent(blob2.oid)}&name=hello.txt`
    );
    expect(rawRes.status).toBe(200);
    expect(await rawRes.text()).toBe("hello from v2\n");

    const treeRes = await SELF.fetch(`https://example.com/${owner}/${repo}/tree?ref=main`);
    expect(treeRes.status).toBe(200);
    expect(await treeRes.text()).toContain("hello.txt");

    const blobRes = await SELF.fetch(
      `https://example.com/${owner}/${repo}/blob?ref=main&path=${encodeURIComponent("hello.txt")}`
    );
    expect(blobRes.status).toBe(200);
    expect(await blobRes.text()).toContain("hello from v2");

    const commitRes = await SELF.fetch(
      `https://example.com/${owner}/${repo}/commit/${commit2.oid}`
    );
    expect(commitRes.status).toBe(200);
    expect(await commitRes.text()).toContain("second commit");

    const diffRes = await SELF.fetch(
      `https://example.com/${owner}/${repo}/commit/${commit2.oid}/diff?path=${encodeURIComponent("hello.txt")}`
    );
    expect(diffRes.status).toBe(200);
    expect(await diffRes.text()).toContain("hello.txt");
  });

  it("streams fetches from multiple active packs after loose copies are deleted", async () => {
    const owner = "o";
    const repo = uniqueRepoId("pack-first-fetch");
    const repoId = `${owner}/${repo}`;
    const { commit2 } = await seedPackedOnlyRepo(repoId);

    const res = await SELF.fetch(`https://example.com/${owner}/${repo}/git-upload-pack`, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-git-upload-pack-request",
        "Git-Protocol": "version=2",
      },
      body: buildFetchBody({ wants: [commit2.oid], done: true, agent: false }),
    } as RequestInit);

    expect(res.status).toBe(200);
    const bytes = new Uint8Array(await res.arrayBuffer());
    const packStart = findBytes(bytes, new TextEncoder().encode("PACK"));
    expect(packStart).toBeGreaterThan(-1);
  });
});
