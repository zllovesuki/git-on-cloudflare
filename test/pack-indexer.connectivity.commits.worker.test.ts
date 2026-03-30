import { describe, expect, it } from "vitest";
import { env } from "cloudflare:test";

import { buildPack, makeCommit, makeTree } from "./util/git-pack.ts";
import { createTestCacheContext } from "./util/pack-first.ts";
import { uniqueRepoId } from "./util/test-helpers.ts";
import {
  makeActiveCatalogRow,
  makeLimiter,
  makeTracingLimiter,
  packIndexerLog as log,
} from "./util/pack-indexer.helpers.ts";

import {
  runPackConnectivityCheck,
  scanPack,
  resolveDeltasAndWriteIdx,
} from "@/git/pack/indexer/index.ts";
import { computeOid } from "@/git/core/objects.ts";
import { findObject } from "@/git/object-store/store.ts";

type ConnectivityStatus = { ref: string; ok: boolean; msg?: string };

async function indexPack(packKey: string, packBytes: Uint8Array, repoId: string) {
  await env.REPO_BUCKET.put(packKey, packBytes);
  const head = await env.REPO_BUCKET.head(packKey);

  const scanResult = await scanPack({
    env,
    packKey,
    packSize: head!.size,
    limiter: makeLimiter(),
    countSubrequest: () => {},
    log,
  });

  const resolveResult = await resolveDeltasAndWriteIdx({
    env,
    packKey,
    packSize: head!.size,
    limiter: makeLimiter(),
    countSubrequest: () => {},
    log,
    scanResult,
    repoId,
  });

  return {
    head: head!,
    scanResult,
    resolveResult,
  };
}

describe("runPackConnectivityCheck commit reachability", () => {
  it("accepts a valid push with commit pointing to an existing tree", async () => {
    const tree = await makeTree();
    const commit = await makeCommit(tree.oid, "valid commit");

    const packBytes = await buildPack([
      { type: "tree", payload: tree.payload },
      { type: "commit", payload: commit.payload },
    ]);

    const packKey = "test/conn-valid.pack";
    await env.REPO_BUCKET.put(packKey, packBytes);
    const head = await env.REPO_BUCKET.head(packKey);

    const scanResult = await scanPack({
      env,
      packKey,
      packSize: head!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
    });

    const repoId = uniqueRepoId();
    const resolveResult = await resolveDeltasAndWriteIdx({
      env,
      packKey,
      packSize: head!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
      scanResult,
      repoId,
    });

    const statuses: ConnectivityStatus[] = [{ ref: "refs/heads/main", ok: true }];
    const cacheCtx = createTestCacheContext("http://localhost/test");

    await runPackConnectivityCheck({
      env,
      repoId,
      newPackKey: packKey,
      newIdxView: resolveResult.idxView,
      newPackSize: head!.size,
      activeCatalog: [],
      commands: [{ oldOid: "0".repeat(40), newOid: commit.oid, ref: "refs/heads/main" }],
      statuses,
      log,
      cacheCtx,
    });

    expect(statuses[0].ok).toBe(true);
  });

  it("rejects a push with commit pointing to a missing tree", async () => {
    const fakeTreeOid = "a".repeat(40);
    const commitPayload = new TextEncoder().encode(
      `tree ${fakeTreeOid}\nauthor You <you@example.com> 0 +0000\ncommitter You <you@example.com> 0 +0000\n\nbad commit\n`
    );
    const commitOid = await computeOid("commit", commitPayload);

    const packBytes = await buildPack([{ type: "commit", payload: commitPayload }]);

    const packKey = "test/conn-missing-tree.pack";
    await env.REPO_BUCKET.put(packKey, packBytes);
    const head = await env.REPO_BUCKET.head(packKey);

    const scanResult = await scanPack({
      env,
      packKey,
      packSize: head!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
    });

    const repoId = uniqueRepoId();
    const resolveResult = await resolveDeltasAndWriteIdx({
      env,
      packKey,
      packSize: head!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
      scanResult,
      repoId,
    });

    const statuses: ConnectivityStatus[] = [{ ref: "refs/heads/main", ok: true }];
    const cacheCtx = createTestCacheContext("http://localhost/test");

    await runPackConnectivityCheck({
      env,
      repoId,
      newPackKey: packKey,
      newIdxView: resolveResult.idxView,
      newPackSize: head!.size,
      activeCatalog: [],
      commands: [{ oldOid: "0".repeat(40), newOid: commitOid, ref: "refs/heads/main" }],
      statuses,
      log,
      cacheCtx,
    });

    expect(statuses[0].ok).toBe(false);
    expect(statuses[0].msg).toBe("missing-objects");
  });

  it("rejects a push when a commit parent is missing", async () => {
    const tree = await makeTree();
    const missingParentOid = "b".repeat(40);
    const commitPayload = new TextEncoder().encode(
      `tree ${tree.oid}\nparent ${missingParentOid}\nauthor You <you@example.com> 0 +0000\ncommitter You <you@example.com> 0 +0000\n\nmissing parent\n`
    );
    const commitOid = await computeOid("commit", commitPayload);

    const packBytes = await buildPack([
      { type: "tree", payload: tree.payload },
      { type: "commit", payload: commitPayload },
    ]);

    const packKey = "test/conn-missing-parent.pack";
    await env.REPO_BUCKET.put(packKey, packBytes);
    const head = await env.REPO_BUCKET.head(packKey);

    const scanResult = await scanPack({
      env,
      packKey,
      packSize: head!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
    });

    const repoId = uniqueRepoId();
    const resolveResult = await resolveDeltasAndWriteIdx({
      env,
      packKey,
      packSize: head!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
      scanResult,
      repoId,
    });

    const statuses: ConnectivityStatus[] = [{ ref: "refs/heads/main", ok: true }];
    const cacheCtx = createTestCacheContext("http://localhost/test");

    await runPackConnectivityCheck({
      env,
      repoId,
      newPackKey: packKey,
      newIdxView: resolveResult.idxView,
      newPackSize: head!.size,
      activeCatalog: [],
      commands: [{ oldOid: "0".repeat(40), newOid: commitOid, ref: "refs/heads/main" }],
      statuses,
      log,
      cacheCtx,
    });

    expect(statuses[0].ok).toBe(false);
    expect(statuses[0].msg).toBe("missing-objects");
  });

  it("uses the caller cache context for limiter and subrequest accounting", async () => {
    const tree = await makeTree();
    const commit = await makeCommit(tree.oid, "budgeted connectivity commit");

    const packBytes = await buildPack([
      { type: "tree", payload: tree.payload },
      { type: "commit", payload: commit.payload },
    ]);

    const packKey = "test/conn-budget.pack";
    await env.REPO_BUCKET.put(packKey, packBytes);
    const head = await env.REPO_BUCKET.head(packKey);

    const scanResult = await scanPack({
      env,
      packKey,
      packSize: head!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
    });

    const repoId = uniqueRepoId();
    const resolveResult = await resolveDeltasAndWriteIdx({
      env,
      packKey,
      packSize: head!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
      scanResult,
      repoId,
    });

    const statuses: ConnectivityStatus[] = [{ ref: "refs/heads/main", ok: true }];
    const labels: string[] = [];
    const cacheCtx = createTestCacheContext("http://localhost/test", 10);
    cacheCtx.memo = {
      ...(cacheCtx.memo || {}),
      limiter: makeTracingLimiter(labels),
    };

    await runPackConnectivityCheck({
      env,
      repoId,
      newPackKey: packKey,
      newIdxView: resolveResult.idxView,
      newPackSize: head!.size,
      activeCatalog: [],
      commands: [{ oldOid: "0".repeat(40), newOid: commit.oid, ref: "refs/heads/main" }],
      statuses,
      log,
      cacheCtx,
    });

    expect(statuses[0].ok).toBe(true);
    expect(labels).toContain("r2:get-range");
    expect(cacheCtx.memo?.subreqBudget).toBeLessThan(10);
  });

  it("accepts a new commit whose tree and parent exist only in the active catalog snapshot", async () => {
    const repoId = uniqueRepoId();

    const baseTree = await makeTree();
    const parentCommit = await makeCommit(baseTree.oid, "base commit");
    const basePackKey = "test/conn-active-base.pack";
    const basePack = await buildPack([
      { type: "tree", payload: baseTree.payload },
      { type: "commit", payload: parentCommit.payload },
    ]);
    const baseIndexed = await indexPack(basePackKey, basePack, repoId);

    const childCommitPayload = new TextEncoder().encode(
      `tree ${baseTree.oid}\nparent ${parentCommit.oid}\nauthor You <you@example.com> 0 +0000\ncommitter You <you@example.com> 0 +0000\n\nchild commit\n`
    );
    const childCommitOid = await computeOid("commit", childCommitPayload);
    const newPackKey = "test/conn-active-new.pack";
    const newPack = await buildPack([{ type: "commit", payload: childCommitPayload }]);
    const newIndexed = await indexPack(newPackKey, newPack, repoId);

    const statuses: ConnectivityStatus[] = [{ ref: "refs/heads/main", ok: true }];
    const cacheCtx = createTestCacheContext("http://localhost/test");

    await runPackConnectivityCheck({
      env,
      repoId,
      newPackKey,
      newIdxView: newIndexed.resolveResult.idxView,
      newPackSize: newIndexed.head.size,
      activeCatalog: [
        makeActiveCatalogRow({
          packKey: basePackKey,
          packBytes: baseIndexed.head.size,
          objectCount: baseIndexed.scanResult.objectCount,
          idxBytes: baseIndexed.resolveResult.idxBytes,
        }),
      ],
      commands: [{ oldOid: "0".repeat(40), newOid: childCommitOid, ref: "refs/heads/main" }],
      statuses,
      log,
      cacheCtx,
    });

    expect(statuses[0].ok).toBe(true);
  });

  it("does not leak the staged pack back into the caller cache memo", async () => {
    const tree = await makeTree();
    const commit = await makeCommit(tree.oid, "scoped commit");
    const repoId = uniqueRepoId();

    const packKey = "test/conn-scoped-memo.pack";
    const packBytes = await buildPack([
      { type: "tree", payload: tree.payload },
      { type: "commit", payload: commit.payload },
    ]);
    const indexed = await indexPack(packKey, packBytes, repoId);

    const statuses: ConnectivityStatus[] = [{ ref: "refs/heads/main", ok: true }];
    const cacheCtx = createTestCacheContext("http://localhost/test", 20);

    await runPackConnectivityCheck({
      env,
      repoId,
      newPackKey: packKey,
      newIdxView: indexed.resolveResult.idxView,
      newPackSize: indexed.head.size,
      activeCatalog: [],
      commands: [{ oldOid: "0".repeat(40), newOid: commit.oid, ref: "refs/heads/main" }],
      statuses,
      log,
      cacheCtx,
    });

    expect(statuses[0].ok).toBe(true);
    expect(cacheCtx.memo?.packCatalog).toBeUndefined();
    expect(cacheCtx.memo?.idxViews?.has(packKey)).toBeFalsy();
    await expect(findObject(env, repoId, commit.oid, cacheCtx)).resolves.toBeUndefined();
  });
});
