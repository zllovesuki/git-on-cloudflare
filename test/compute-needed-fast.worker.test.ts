import { it, expect, describe } from "vitest";
import { env } from "cloudflare:test";
import type { RepoDurableObject } from "@/index";
import type { CacheContext } from "@/cache/index.ts";
import { computeNeededFast } from "@/git/operations/fetch/neededFast.ts";
import { uniqueRepoId, runDOWithRetry } from "./util/test-helpers.ts";

describe("computeNeededFast", () => {
  it("computes minimal closure with stop set", async () => {
    const owner = "o";
    const repo = uniqueRepoId("closure-fast");
    const repoId = `${owner}/${repo}`;

    // Create a repo with a commit
    const id = env.REPO_DO.idFromName(repoId);
    const { commitOid, treeOid } = await runDOWithRetry(
      () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>,
      async (instance: RepoDurableObject) => {
        return await instance.seedMinimalRepo();
      }
    );

    // Test 1: With no haves, should include commit and tree
    const needed1 = await computeNeededFast(
      env,
      repoId,
      [commitOid], // want commit
      [], // no haves
      undefined
    );

    // Should include the commit and its tree
    expect(needed1).toContain(commitOid);
    expect(needed1).toContain(treeOid);
    expect(needed1.length).toBe(2); // Just commit and tree

    // Test 2: With commit as have, should return empty (nothing needed)
    const needed2 = await computeNeededFast(
      env,
      repoId,
      [commitOid], // want commit
      [commitOid], // already have it
      undefined
    );

    // Should be empty since we already have what we want
    expect(needed2.length).toBe(0);

    // Test 3: With non-existent have, should include everything
    const fakeOid = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef";
    const needed3 = await computeNeededFast(
      env,
      repoId,
      [commitOid], // want commit
      [fakeOid], // have something that doesn't exist
      undefined
    );

    // Should include everything since the have doesn't exist
    expect(needed3).toContain(commitOid);
    expect(needed3).toContain(treeOid);
    expect(needed3.length).toBe(2);
  });

  it("returns the partial closure when the traversal times out", async () => {
    const owner = "o";
    const repo = uniqueRepoId("closure-timeout");
    const repoId = `${owner}/${repo}`;

    const cacheCtx: CacheContext = {
      req: new Request("http://test"),
      ctx: { waitUntil: () => {}, passThroughOnException: () => {} } as any,
      memo: {
        flags: new Set<string>(),
      },
    };

    // Create a large chain of commits
    const id = env.REPO_DO.idFromName(repoId);
    const { commitOid } = await runDOWithRetry(
      () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>,
      async (instance: RepoDurableObject) => instance.seedMinimalRepo()
    );

    const realNow = Date.now;
    let nowCalls = 0;
    Date.now = () => {
      nowCalls++;
      return nowCalls === 1 ? 0 : 50_000;
    };

    try {
      const needed = await computeNeededFast(env, repoId, [commitOid], [], cacheCtx);
      expect(cacheCtx.memo!.flags!.has("closure-timeout")).toBe(true);
      expect(Array.isArray(needed)).toBe(true);
    } finally {
      Date.now = realNow;
    }
  });

  it("uses memoization effectively", async () => {
    const owner = "o";
    const repo = uniqueRepoId("memo");
    const repoId = `${owner}/${repo}`;

    const id = env.REPO_DO.idFromName(repoId);
    const { commitOid } = await runDOWithRetry(
      () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>,
      async (instance: RepoDurableObject) => instance.seedMinimalRepo()
    );

    const cacheCtx: CacheContext = {
      req: new Request("http://test"),
      ctx: { waitUntil: () => {}, passThroughOnException: () => {} } as any,
      memo: {
        refs: new Map<string, string[]>(),
        flags: new Set<string>(),
      },
    };

    // First call should populate memo
    const needed1 = await computeNeededFast(env, repoId, [commitOid], [], cacheCtx);

    expect(needed1.length).toBeGreaterThan(0);
    expect(cacheCtx.memo!.refs!.size).toBeGreaterThan(0);

    // Second call should use memo (faster)
    const startTime = Date.now();
    const needed2 = await computeNeededFast(env, repoId, [commitOid], [], cacheCtx);
    const elapsed = Date.now() - startTime;

    expect(needed2).toEqual(needed1);
    // Should be faster due to memoization
    expect(elapsed).toBeLessThan(100);
  });
});
