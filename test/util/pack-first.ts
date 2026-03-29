import type { CacheContext } from "@/cache";
import type { RepoDurableObject } from "@/index";

import { buildPack } from "./git-pack.ts";
import { buildTreePayload, seedLegacyPackedRepo } from "./packed-repo.ts";
import { encodeGitObject } from "@/git/core/index.ts";
import { env } from "cloudflare:test";

export function createTestCacheContext(url: string, subreqBudget?: number): CacheContext {
  return {
    req: new Request(url),
    ctx: {
      props: undefined,
      waitUntil(_promise: Promise<unknown>) {},
      passThroughOnException() {},
    },
    memo: typeof subreqBudget === "number" ? { subreqBudget } : {},
  };
}

export async function seedPackFirstRepo(repoId: string) {
  const id = env.REPO_DO.idFromName(repoId);
  const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;
  const author = "You <you@example.com> 0 +0000";
  const baseBlobPayload = new TextEncoder().encode("version one\n");
  const nextBlobPayload = new TextEncoder().encode("version two\n");

  const baseBlob = await encodeGitObject("blob", baseBlobPayload);
  const nextBlob = await encodeGitObject("blob", nextBlobPayload);

  const baseTreePayload = buildTreePayload([
    { mode: "100644", name: "README.md", oid: baseBlob.oid },
  ]);
  const nextTreePayload = buildTreePayload([
    { mode: "100644", name: "README.md", oid: nextBlob.oid },
  ]);
  const baseTree = await encodeGitObject("tree", baseTreePayload);
  const nextTree = await encodeGitObject("tree", nextTreePayload);

  const baseCommitPayload = new TextEncoder().encode(
    `tree ${baseTree.oid}\n` + `author ${author}\n` + `committer ${author}\n\n` + `first commit\n`
  );
  const baseCommit = await encodeGitObject("commit", baseCommitPayload);

  const nextCommitPayload = new TextEncoder().encode(
    `tree ${nextTree.oid}\n` +
      `parent ${baseCommit.oid}\n` +
      `author ${author}\n` +
      `committer ${author}\n\n` +
      `second commit\n`
  );
  const nextCommit = await encodeGitObject("commit", nextCommitPayload);

  const packBytes = await buildPack([
    { type: "blob", payload: baseBlobPayload },
    { type: "tree", payload: baseTreePayload },
    { type: "commit", payload: baseCommitPayload },
    { type: "blob", payload: nextBlobPayload },
    { type: "tree", payload: nextTreePayload },
    { type: "commit", payload: nextCommitPayload },
  ]);

  const seeded = await seedLegacyPackedRepo({
    env,
    repoId,
    getStub,
    packs: [{ name: "pack-read-path.pack", packBytes }],
    refs: [{ name: "refs/heads/main", oid: nextCommit.oid }],
    head: { target: "refs/heads/main", oid: nextCommit.oid },
    looseObjects: [baseBlob, nextBlob, baseTree, nextTree, baseCommit, nextCommit],
    mirrorLooseToR2: true,
  });

  return {
    ...seeded,
    baseBlob,
    nextBlob,
    baseTree,
    nextTree,
    baseCommit,
    nextCommit,
    objectOids: [
      baseBlob.oid,
      nextBlob.oid,
      baseTree.oid,
      nextTree.oid,
      baseCommit.oid,
      nextCommit.oid,
    ],
  };
}
