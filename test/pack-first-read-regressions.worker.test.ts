import { describe, expect, it } from "vitest";
import { env, SELF } from "cloudflare:test";
import type { RepoDurableObject } from "@/index";
import { encodeGitObject } from "@/git/core/index.ts";
import {
  buildPack,
  callStubWithRetry,
  deleteLooseObjectCopies,
  seedLegacyPackedRepo,
  seedPackedRepo,
  toRequestBody,
  uniqueRepoId,
} from "./util/test-helpers.ts";
import { buildFetchBody, findBytes } from "./util/fetch-protocol.ts";

describe("pack-first read-path regressions", () => {
  it("serves UI routes from packs after all loose copies are deleted", async () => {
    const owner = "o";
    const repo = uniqueRepoId("pack-first-ui");
    const repoId = `${owner}/${repo}`;
    const id = env.REPO_DO.idFromName(repoId);
    const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;

    const seeded = await seedPackedRepo(env, repoId, getStub, { mirrorLooseToR2: true });
    await callStubWithRetry(getStub, (stub) => stub.getActivePackCatalog());
    await deleteLooseObjectCopies(env, getStub, seeded.objectOids);

    const treeRes = await SELF.fetch(`https://example.com/${owner}/${repo}/tree?ref=main`);
    expect(treeRes.status).toBe(200);
    expect(await treeRes.text()).toContain("hello.txt");

    const blobRes = await SELF.fetch(
      `https://example.com/${owner}/${repo}/blob?ref=main&path=${encodeURIComponent("hello.txt")}`
    );
    expect(blobRes.status).toBe(200);
    expect(await blobRes.text()).toContain("hello from packed storage");

    const rawRes = await SELF.fetch(
      `https://example.com/${owner}/${repo}/raw?oid=${encodeURIComponent(seeded.blob.oid)}&name=hello.txt`
    );
    expect(rawRes.status).toBe(200);
    expect(await rawRes.text()).toBe("hello from packed storage\n");

    const rawPathRes = await SELF.fetch(
      `https://example.com/${owner}/${repo}/rawpath?ref=main&path=${encodeURIComponent("hello.txt")}&name=hello.txt`,
      {
        headers: {
          referer: `https://example.com/${owner}/${repo}/blob?ref=main&path=hello.txt`,
        },
      }
    );
    expect(rawPathRes.status).toBe(200);
    expect(await rawPathRes.text()).toBe("hello from packed storage\n");

    const commitRes = await SELF.fetch(
      `https://example.com/${owner}/${repo}/commit/${encodeURIComponent(seeded.commit.oid)}`
    );
    expect(commitRes.status).toBe(200);
    expect(await commitRes.text()).toContain("packed commit");

    const diffRes = await SELF.fetch(
      `https://example.com/${owner}/${repo}/commit/${encodeURIComponent(seeded.commit.oid)}/diff?path=${encodeURIComponent("hello.txt")}`
    );
    expect(diffRes.status).toBe(200);
    const diff = (await diffRes.json()) as {
      changeType: string;
      patch?: string;
    };
    expect(diff.changeType).toBe("A");
    expect(diff.patch).toContain("+++ b/hello.txt");
    expect(diff.patch).toContain("+hello from packed storage");
  });

  it("serves fetch from packs after all loose copies are deleted", async () => {
    const owner = "o";
    const repo = uniqueRepoId("pack-first-fetch");
    const repoId = `${owner}/${repo}`;
    const id = env.REPO_DO.idFromName(repoId);
    const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;

    const seeded = await seedPackedRepo(env, repoId, getStub, { mirrorLooseToR2: true });
    await callStubWithRetry(getStub, (stub) => stub.getActivePackCatalog());
    await deleteLooseObjectCopies(env, getStub, seeded.objectOids);

    const res = await SELF.fetch(`https://example.com/${owner}/${repo}/git-upload-pack`, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-git-upload-pack-request",
        "Git-Protocol": "version=2",
      },
      body: toRequestBody(buildFetchBody({ wants: [seeded.commit.oid], done: true })),
    });

    expect(res.status).toBe(200);
    const bytes = new Uint8Array(await res.arrayBuffer());
    const packOffset = findBytes(bytes, new TextEncoder().encode("PACK"));
    expect(packOffset).toBeGreaterThan(-1);
    expect(new TextDecoder().decode(bytes.subarray(packOffset, packOffset + 4))).toBe("PACK");
  });

  it("serves commits and merge fragments from packs after all loose copies are deleted", async () => {
    const owner = "o";
    const repo = uniqueRepoId("pack-first-commits");
    const repoId = `${owner}/${repo}`;
    const id = env.REPO_DO.idFromName(repoId);
    const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;

    const author = "You <you@example.com> 0 +0000";
    const tree = await encodeGitObject("tree", new Uint8Array(0));

    const baseCommitPayload = new TextEncoder().encode(
      `tree ${tree.oid}\n` + `author ${author}\n` + `committer ${author}\n\nbase\n`
    );
    const baseCommit = await encodeGitObject("commit", baseCommitPayload);

    const mainCommitPayload = new TextEncoder().encode(
      `tree ${tree.oid}\n` +
        `parent ${baseCommit.oid}\n` +
        `author ${author}\n` +
        `committer ${author}\n\nmainline\n`
    );
    const mainCommit = await encodeGitObject("commit", mainCommitPayload);

    const sideCommitPayload = new TextEncoder().encode(
      `tree ${tree.oid}\n` +
        `parent ${baseCommit.oid}\n` +
        `author ${author}\n` +
        `committer ${author}\n\nside branch\n`
    );
    const sideCommit = await encodeGitObject("commit", sideCommitPayload);

    const mergeCommitPayload = new TextEncoder().encode(
      `tree ${tree.oid}\n` +
        `parent ${mainCommit.oid}\n` +
        `parent ${sideCommit.oid}\n` +
        `author ${author}\n` +
        `committer ${author}\n\nmerge commit\n`
    );
    const mergeCommit = await encodeGitObject("commit", mergeCommitPayload);

    const packBytes = await buildPack([
      { type: "tree", payload: new Uint8Array(0) },
      { type: "commit", payload: baseCommitPayload },
      { type: "commit", payload: mainCommitPayload },
      { type: "commit", payload: sideCommitPayload },
      { type: "commit", payload: mergeCommitPayload },
    ]);

    await seedLegacyPackedRepo({
      env,
      repoId,
      getStub,
      packs: [{ name: "pack-merge.pack", packBytes }],
      refs: [{ name: "refs/heads/main", oid: mergeCommit.oid }],
      head: { target: "refs/heads/main", oid: mergeCommit.oid },
      looseObjects: [tree, baseCommit, mainCommit, sideCommit, mergeCommit],
      mirrorLooseToR2: true,
    });

    await callStubWithRetry(getStub, (stub) => stub.getActivePackCatalog());
    await deleteLooseObjectCopies(env, getStub, [
      tree.oid,
      baseCommit.oid,
      mainCommit.oid,
      sideCommit.oid,
      mergeCommit.oid,
    ]);

    const commitsRes = await SELF.fetch(`https://example.com/${owner}/${repo}/commits?ref=main`);
    expect(commitsRes.status).toBe(200);
    const commitsHtml = await commitsRes.text();
    expect(commitsHtml).toContain("merge commit");
    expect(commitsHtml).toContain("mainline");

    const fragmentsRes = await SELF.fetch(
      `https://example.com/${owner}/${repo}/commits/fragments/${encodeURIComponent(mergeCommit.oid)}?limit=10`
    );
    expect(fragmentsRes.status).toBe(200);
    const payload = (await fragmentsRes.json()) as {
      commits: Array<{ firstLine: string }>;
    };
    expect(payload.commits.some((commit) => commit.firstLine === "side branch")).toBe(true);
  });
});
