import { describe, expect, it } from "vitest";
import { env } from "cloudflare:test";

import { buildPack, makeCommit, makeTree } from "./util/git-pack.ts";
import { createTestCacheContext } from "./util/pack-first.ts";
import { uniqueRepoId } from "./util/test-helpers.ts";
import { makeLimiter, packIndexerLog as log } from "./util/pack-indexer.helpers.ts";

import {
  runPackConnectivityCheck,
  scanPack,
  resolveDeltasAndWriteIdx,
} from "@/git/pack/indexer/index.ts";
import { computeOid } from "@/git/core/objects.ts";

type ConnectivityStatus = { ref: string; ok: boolean; msg?: string };

describe("runPackConnectivityCheck annotated tags", () => {
  it("unwraps annotated tags and validates the final target", async () => {
    const tree = await makeTree();
    const commit = await makeCommit(tree.oid, "tagged commit");
    const tagPayload = new TextEncoder().encode(
      `object ${commit.oid}\ntype commit\ntag v1.0\ntagger You <you@example.com> 0 +0000\n\nrelease\n`
    );
    const tagOid = await computeOid("tag", tagPayload);

    const packBytes = await buildPack([
      { type: "tree", payload: tree.payload },
      { type: "commit", payload: commit.payload },
      { type: "tag", payload: tagPayload },
    ]);

    const packKey = "test/conn-tag.pack";
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

    const statuses: ConnectivityStatus[] = [{ ref: "refs/tags/v1.0", ok: true }];
    const cacheCtx = createTestCacheContext("http://localhost/test");

    await runPackConnectivityCheck({
      env,
      repoId,
      newPackKey: packKey,
      newIdxView: resolveResult.idxView,
      newPackSize: head!.size,
      activeCatalog: [],
      commands: [{ oldOid: "0".repeat(40), newOid: tagOid, ref: "refs/tags/v1.0" }],
      statuses,
      log,
      cacheCtx,
    });

    expect(statuses[0].ok).toBe(true);
  });

  it("accepts annotated tag chains up to the hard limit", async () => {
    const tree = await makeTree();
    const commit = await makeCommit(tree.oid, "tag depth limit");
    const tagPayloads: Uint8Array[] = [];
    let targetOid = commit.oid;
    for (let depth = 0; depth < 8; depth++) {
      const payload = new TextEncoder().encode(
        `object ${targetOid}\ntype ${depth === 0 ? "commit" : "tag"}\ntag v${depth}\ntagger You <you@example.com> 0 +0000\n\ndepth ${depth}\n`
      );
      tagPayloads.push(payload);
      targetOid = await computeOid("tag", payload);
    }

    const packEntries = [
      { type: "tree" as const, payload: tree.payload },
      { type: "commit" as const, payload: commit.payload },
      ...tagPayloads.map((payload) => ({ type: "tag" as const, payload })),
    ];
    const packBytes = await buildPack(packEntries);

    const packKey = "test/conn-tag-depth-limit.pack";
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

    const statuses: ConnectivityStatus[] = [{ ref: "refs/tags/limit", ok: true }];
    const cacheCtx = createTestCacheContext("http://localhost/test");

    await runPackConnectivityCheck({
      env,
      repoId,
      newPackKey: packKey,
      newIdxView: resolveResult.idxView,
      newPackSize: head!.size,
      activeCatalog: [],
      commands: [{ oldOid: "0".repeat(40), newOid: targetOid, ref: "refs/tags/limit" }],
      statuses,
      log,
      cacheCtx,
    });

    expect(statuses[0].ok).toBe(true);
  });

  it("rejects annotated tag chains deeper than the hard limit", async () => {
    const tree = await makeTree();
    const commit = await makeCommit(tree.oid, "deep tag commit");
    const tagPayloads: Uint8Array[] = [];
    let targetOid = commit.oid;
    for (let depth = 0; depth < 9; depth++) {
      const payload = new TextEncoder().encode(
        `object ${targetOid}\ntype ${depth === 0 ? "commit" : "tag"}\ntag v${depth}\ntagger You <you@example.com> 0 +0000\n\ndepth ${depth}\n`
      );
      tagPayloads.push(payload);
      targetOid = await computeOid("tag", payload);
    }

    const packEntries = [
      { type: "tree" as const, payload: tree.payload },
      { type: "commit" as const, payload: commit.payload },
      ...tagPayloads.map((payload) => ({ type: "tag" as const, payload })),
    ];
    const packBytes = await buildPack(packEntries);

    const packKey = "test/conn-tag-depth.pack";
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

    const statuses: ConnectivityStatus[] = [{ ref: "refs/tags/deep", ok: true }];
    const cacheCtx = createTestCacheContext("http://localhost/test");

    await runPackConnectivityCheck({
      env,
      repoId,
      newPackKey: packKey,
      newIdxView: resolveResult.idxView,
      newPackSize: head!.size,
      activeCatalog: [],
      commands: [{ oldOid: "0".repeat(40), newOid: targetOid, ref: "refs/tags/deep" }],
      statuses,
      log,
      cacheCtx,
    });

    expect(statuses[0].ok).toBe(false);
    expect(statuses[0].msg).toBe("missing-objects");
  });
});
