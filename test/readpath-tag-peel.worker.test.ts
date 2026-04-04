import { it, expect } from "vitest";
import { env } from "cloudflare:test";
import type { RepoDurableObject } from "@/index";
import { asTypedStorage, type RepoStateSchema } from "@/do/repo/repoState.ts";
import { uniqueRepoId, runDOWithRetry } from "./util/test-helpers.ts";
import { readPath } from "@/git/operations/read.ts";
import { encodeGitObject } from "@/git/core/objects.ts";
import { seedPackFirstRepo } from "./util/pack-first.ts";
import { registerTestPack } from "./util/packed-repo.ts";

it("readPath resolves tag to its target commit tree (tag peel)", async () => {
  const owner = "o";
  const repo = uniqueRepoId("r-readpath-tag");
  const repoId = `${owner}/${repo}`;
  const seeded = await seedPackFirstRepo(repoId);
  const id = env.REPO_DO.idFromName(repoId);
  const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;

  // Create an annotated tag object pointing to the seeded commit
  const commitOid = seeded.nextCommit.oid;
  const tagPayload = new TextEncoder().encode(
    `object ${commitOid}\n` +
      `type commit\n` +
      `tag v1\n` +
      `tagger Test <test@example.com> 0 +0000\n` +
      `\n` +
      `message\n`
  );
  const { oid: tagOid } = await encodeGitObject("tag", tagPayload);

  // Pack the tag and register it
  await registerTestPack({
    env,
    repoId,
    getStub,
    packName: "pack-tag.pack",
    objects: [{ type: "tag", payload: tagPayload }],
  });

  // Add the tag ref
  await runDOWithRetry(getStub, async (_instance, state) => {
    const store = asTypedStorage<RepoStateSchema>(state.storage);
    const refs = ((await store.get("refs")) || []) as { name: string; oid: string }[];
    refs.push({ name: "refs/tags/v1", oid: tagOid });
    await store.put("refs", refs);
  });

  // Call readPath with the tag ref; expect a tree result (root tree of the tagged commit)
  const result = await readPath(env as unknown as Env, repoId, "refs/tags/v1");
  expect(result.type).toBe("tree");
  if (result.type === "tree") {
    expect(Array.isArray(result.entries)).toBe(true);
  }
});
