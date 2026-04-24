import { describe, expect, it } from "vitest";
import { env } from "cloudflare:test";

import { buildPack, buildAppendOnlyDelta, buildCopyPrefixDelta } from "./util/git-pack.ts";
import { createTestCacheContext } from "./util/pack-first.ts";
import { uniqueRepoId } from "./util/test-helpers.ts";
import {
  makeActiveCatalogRow,
  makeCountSubrequest,
  makeLimiter,
  makeTracingLimiter,
  packIndexerLog as log,
} from "./util/pack-indexer.helpers.ts";

import { scanPack, resolveDeltasAndWriteIdx } from "@/git/pack/indexer/index.ts";
import { computeOid, encodeGitObject } from "@/git/core/objects.ts";
import { bytesToHex } from "@/common/hex.ts";
import { findOidIndex } from "@/git/object-store/idxView.ts";
import { packIndexKey } from "@/keys.ts";

describe("resolveDeltasAndWriteIdx REF_DELTA", () => {
  it("resolves REF_DELTA with external base from the provided active catalog snapshot", async () => {
    const baseBlobPayload = new TextEncoder().encode("external base content\n");
    const baseBlob = await encodeGitObject("blob", baseBlobPayload);
    const basePack = await buildPack([{ type: "blob", payload: baseBlobPayload }]);

    const basePackKey = "test/ref-delta-base.pack";
    await env.REPO_BUCKET.put(basePackKey, basePack);
    const baseHead = await env.REPO_BUCKET.head(basePackKey);

    const baseScan = await scanPack({
      env,
      packKey: basePackKey,
      packSize: baseHead!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
    });
    const repoId = uniqueRepoId();
    const baseResolve = await resolveDeltasAndWriteIdx({
      env,
      packKey: basePackKey,
      packSize: baseHead!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
      scanResult: baseScan,
      repoId,
    });

    const suffix = new TextEncoder().encode("extra data\n");
    const delta = buildAppendOnlyDelta(baseBlobPayload, suffix);
    const thinPack = await buildPack([{ type: "ref-delta", baseOid: baseBlob.oid, delta }]);

    const thinPackKey = "test/ref-delta-thin.pack";
    await env.REPO_BUCKET.put(thinPackKey, thinPack);
    const thinHead = await env.REPO_BUCKET.head(thinPackKey);

    const thinScan = await scanPack({
      env,
      packKey: thinPackKey,
      packSize: thinHead!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
    });

    const cacheCtx = createTestCacheContext("http://localhost/test");
    const thinResolve = await resolveDeltasAndWriteIdx({
      env,
      packKey: thinPackKey,
      packSize: thinHead!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
      scanResult: thinScan,
      activeCatalog: [
        makeActiveCatalogRow({
          packKey: basePackKey,
          packBytes: baseHead!.size,
          objectCount: baseScan.objectCount,
          idxBytes: baseResolve.idxBytes,
        }),
      ],
      cacheCtx,
      repoId,
    });

    expect(thinScan.table.resolved[0]).toBe(1);
    expect(thinResolve.objectCount).toBe(1);

    const expectedPayload = new Uint8Array(baseBlobPayload.length + suffix.length);
    expectedPayload.set(baseBlobPayload, 0);
    expectedPayload.set(suffix, baseBlobPayload.length);
    const expectedOid = await computeOid("blob", expectedPayload);
    expect(bytesToHex(thinScan.table.oids.subarray(0, 20))).toBe(expectedOid);
  });

  it("rejects a thin REF_DELTA when the external base is missing", async () => {
    const fakeBaseOid = "ab".repeat(20);
    const delta = buildAppendOnlyDelta(
      new TextEncoder().encode("base\n"),
      new TextEncoder().encode("next\n")
    );
    const thinPack = await buildPack([{ type: "ref-delta", baseOid: fakeBaseOid, delta }]);

    const packKey = "test/ref-delta-missing-base.pack";
    await env.REPO_BUCKET.put(packKey, thinPack);
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
    const cacheCtx = createTestCacheContext("http://localhost/test");
    await expect(
      resolveDeltasAndWriteIdx({
        env,
        packKey,
        packSize: head!.size,
        limiter: makeLimiter(),
        countSubrequest: () => {},
        log,
        scanResult,
        repoId,
        cacheCtx,
      })
    ).rejects.toThrow(/could not be resolved/);

    const idxObj = await env.REPO_BUCKET.get(packIndexKey(packKey));
    expect(idxObj).toBeNull();
  });

  it("prefers the explicit active catalog snapshot over stale memoized pack-catalog state", async () => {
    const baseBlobPayload = new TextEncoder().encode("external base content\n");
    const baseBlob = await encodeGitObject("blob", baseBlobPayload);
    const basePack = await buildPack([{ type: "blob", payload: baseBlobPayload }]);

    const basePackKey = "test/ref-delta-stale-memo-base.pack";
    await env.REPO_BUCKET.put(basePackKey, basePack);
    const baseHead = await env.REPO_BUCKET.head(basePackKey);

    const baseScan = await scanPack({
      env,
      packKey: basePackKey,
      packSize: baseHead!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
    });
    const repoId = uniqueRepoId();
    const baseResolve = await resolveDeltasAndWriteIdx({
      env,
      packKey: basePackKey,
      packSize: baseHead!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
      scanResult: baseScan,
      repoId,
    });

    const suffix = new TextEncoder().encode("active catalog wins\n");
    const thinPack = await buildPack([
      {
        type: "ref-delta",
        baseOid: baseBlob.oid,
        delta: buildAppendOnlyDelta(baseBlobPayload, suffix),
      },
    ]);

    const thinPackKey = "test/ref-delta-stale-memo-thin.pack";
    await env.REPO_BUCKET.put(thinPackKey, thinPack);
    const thinHead = await env.REPO_BUCKET.head(thinPackKey);

    const thinScan = await scanPack({
      env,
      packKey: thinPackKey,
      packSize: thinHead!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
    });

    const cacheCtx = createTestCacheContext("http://localhost/test");
    cacheCtx.memo = {
      ...(cacheCtx.memo || {}),
      packCatalog: [],
    };

    const thinResolve = await resolveDeltasAndWriteIdx({
      env,
      packKey: thinPackKey,
      packSize: thinHead!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
      scanResult: thinScan,
      activeCatalog: [
        makeActiveCatalogRow({
          packKey: basePackKey,
          packBytes: baseHead!.size,
          objectCount: baseScan.objectCount,
          idxBytes: baseResolve.idxBytes,
        }),
      ],
      cacheCtx,
      repoId,
    });

    expect(thinResolve.objectCount).toBe(1);
    expect(thinScan.table.resolved[0]).toBe(1);
    expect(cacheCtx.memo?.packCatalog?.map((row) => row.packKey)).toEqual([basePackKey]);
  });

  it("resolves same-pack REF_DELTA chains whose bases appear later in the pack", async () => {
    const baseBlobPayload = new TextEncoder().encode("base\n");
    const midSuffix = new TextEncoder().encode("mid\n");
    const finalSuffix = new TextEncoder().encode("final\n");

    const baseBlob = await encodeGitObject("blob", baseBlobPayload);

    const midPayload = new Uint8Array(baseBlobPayload.length + midSuffix.length);
    midPayload.set(baseBlobPayload, 0);
    midPayload.set(midSuffix, baseBlobPayload.length);
    const midOid = await computeOid("blob", midPayload);

    const finalPayload = new Uint8Array(midPayload.length + finalSuffix.length);
    finalPayload.set(midPayload, 0);
    finalPayload.set(finalSuffix, midPayload.length);
    const finalOid = await computeOid("blob", finalPayload);

    const midDelta = buildAppendOnlyDelta(baseBlobPayload, midSuffix);
    const finalDelta = buildAppendOnlyDelta(midPayload, finalSuffix);

    const packBytes = await buildPack([
      { type: "ref-delta", baseOid: midOid, delta: finalDelta },
      { type: "ref-delta", baseOid: baseBlob.oid, delta: midDelta },
      { type: "blob", payload: baseBlobPayload },
    ]);

    const packKey = "test/resolve-forward-ref-chain.pack";
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
    await resolveDeltasAndWriteIdx({
      env,
      packKey,
      packSize: head!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
      scanResult,
      repoId,
    });

    expect(scanResult.table.resolved.reduce((sum, value) => sum + value, 0)).toBe(3);
    expect(bytesToHex(scanResult.table.oids.subarray(0, 20))).toBe(finalOid);
    expect(bytesToHex(scanResult.table.oids.subarray(20, 40))).toBe(midOid);
    expect(bytesToHex(scanResult.table.oids.subarray(40, 60))).toBe(baseBlob.oid);
  });

  it("uses an existing idx to map same-pack REF_DELTA bases during refs-only backfill", async () => {
    const baseBlobPayload = new TextEncoder().encode("base\n");
    const midSuffix = new TextEncoder().encode("mid\n");
    const finalSuffix = new TextEncoder().encode("final\n");

    const baseBlob = await encodeGitObject("blob", baseBlobPayload);

    const midPayload = new Uint8Array(baseBlobPayload.length + midSuffix.length);
    midPayload.set(baseBlobPayload, 0);
    midPayload.set(midSuffix, baseBlobPayload.length);
    const midOid = await computeOid("blob", midPayload);

    const finalPayload = new Uint8Array(midPayload.length + finalSuffix.length);
    finalPayload.set(midPayload, 0);
    finalPayload.set(finalSuffix, midPayload.length);
    const finalOid = await computeOid("blob", finalPayload);

    const packBytes = await buildPack([
      {
        type: "ref-delta",
        baseOid: midOid,
        delta: buildAppendOnlyDelta(midPayload, finalSuffix),
      },
      {
        type: "ref-delta",
        baseOid: baseBlob.oid,
        delta: buildAppendOnlyDelta(baseBlobPayload, midSuffix),
      },
      { type: "blob", payload: baseBlobPayload },
    ]);

    const packKey = "test/resolve-ref-chain-existing-idx.pack";
    await env.REPO_BUCKET.put(packKey, packBytes);
    const head = await env.REPO_BUCKET.head(packKey);

    const initialScan = await scanPack({
      env,
      packKey,
      packSize: head!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
    });
    const repoId = uniqueRepoId();
    const initialResolve = await resolveDeltasAndWriteIdx({
      env,
      packKey,
      packSize: head!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
      scanResult: initialScan,
      repoId,
    });

    const midOidIndex = findOidIndex(initialResolve.idxView, midOid);
    expect(midOidIndex).toBeGreaterThanOrEqual(0);
    expect(midOidIndex).not.toBe(1);
    expect(initialResolve.idxView.offsets[midOidIndex]).toBe(initialScan.table.offsets[1]);

    const backfillScan = await scanPack({
      env,
      packKey,
      packSize: head!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
    });

    const backfillResolve = await resolveDeltasAndWriteIdx({
      env,
      packKey,
      packSize: head!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
      scanResult: backfillScan,
      repoId,
      activeCatalog: [],
      cacheCtx: createTestCacheContext("http://localhost/test"),
      writeIdx: false,
      existingIdxView: initialResolve.idxView,
    });

    expect(backfillResolve.idxBytes).toBe(0);
    expect(backfillScan.table.resolved.reduce((sum, value) => sum + value, 0)).toBe(3);
    expect(bytesToHex(backfillScan.table.oids.subarray(0, 20))).toBe(finalOid);
    expect(bytesToHex(backfillScan.table.oids.subarray(20, 40))).toBe(midOid);
    expect(bytesToHex(backfillScan.table.oids.subarray(40, 60))).toBe(baseBlob.oid);
  });

  it("uses another duplicate idx row for identity same-pack REF_DELTA backfill", async () => {
    const baseBlobPayload = new TextEncoder().encode("duplicate oid base\n");
    const baseBlob = await encodeGitObject("blob", baseBlobPayload);
    const identityDelta = buildAppendOnlyDelta(baseBlobPayload, new Uint8Array(0));

    const packBytes = await buildPack([
      { type: "ref-delta", baseOid: baseBlob.oid, delta: identityDelta },
      { type: "blob", payload: baseBlobPayload },
    ]);

    const packKey = "test/resolve-ref-identity-existing-idx.pack";
    await env.REPO_BUCKET.put(packKey, packBytes);
    const head = await env.REPO_BUCKET.head(packKey);

    const initialScan = await scanPack({
      env,
      packKey,
      packSize: head!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
    });
    const repoId = uniqueRepoId();
    const initialResolve = await resolveDeltasAndWriteIdx({
      env,
      packKey,
      packSize: head!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
      scanResult: initialScan,
      repoId,
    });

    expect(findOidIndex(initialResolve.idxView, baseBlob.oid)).toBe(0);

    const backfillScan = await scanPack({
      env,
      packKey,
      packSize: head!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
    });

    const backfillResolve = await resolveDeltasAndWriteIdx({
      env,
      packKey,
      packSize: head!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
      scanResult: backfillScan,
      repoId,
      activeCatalog: [],
      cacheCtx: createTestCacheContext("http://localhost/test"),
      writeIdx: false,
      existingIdxView: initialResolve.idxView,
    });

    expect(backfillResolve.idxBytes).toBe(0);
    expect(backfillScan.table.resolved.reduce((sum, value) => sum + value, 0)).toBe(2);
    expect(bytesToHex(backfillScan.table.oids.subarray(0, 20))).toBe(baseBlob.oid);
    expect(bytesToHex(backfillScan.table.oids.subarray(20, 40))).toBe(baseBlob.oid);
  });

  it("falls back to an external base for target-pack identity REF_DELTA duplicates", async () => {
    const baseBlobPayload = new TextEncoder().encode("external duplicate oid base\n");
    const baseBlob = await encodeGitObject("blob", baseBlobPayload);
    const repoId = uniqueRepoId();

    const basePackKey = "test/ref-delta-identity-external-base.pack";
    const basePack = await buildPack([{ type: "blob", payload: baseBlobPayload }]);
    await env.REPO_BUCKET.put(basePackKey, basePack);
    const baseHead = await env.REPO_BUCKET.head(basePackKey);
    const baseScan = await scanPack({
      env,
      packKey: basePackKey,
      packSize: baseHead!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
    });
    const baseResolve = await resolveDeltasAndWriteIdx({
      env,
      packKey: basePackKey,
      packSize: baseHead!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
      scanResult: baseScan,
      repoId,
    });

    const activeCatalog = [
      makeActiveCatalogRow({
        packKey: basePackKey,
        packBytes: baseHead!.size,
        objectCount: baseScan.objectCount,
        idxBytes: baseResolve.idxBytes,
      }),
    ];
    const identityDelta = buildAppendOnlyDelta(baseBlobPayload, new Uint8Array(0));
    const targetPack = await buildPack([
      { type: "ref-delta", baseOid: baseBlob.oid, delta: identityDelta },
      { type: "ref-delta", baseOid: baseBlob.oid, delta: identityDelta },
    ]);

    const targetPackKey = "test/ref-delta-identity-external-target.pack";
    await env.REPO_BUCKET.put(targetPackKey, targetPack);
    const targetHead = await env.REPO_BUCKET.head(targetPackKey);
    const initialScan = await scanPack({
      env,
      packKey: targetPackKey,
      packSize: targetHead!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
    });
    const initialResolve = await resolveDeltasAndWriteIdx({
      env,
      packKey: targetPackKey,
      packSize: targetHead!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
      scanResult: initialScan,
      activeCatalog,
      cacheCtx: createTestCacheContext("http://localhost/test"),
      repoId,
    });

    const backfillScan = await scanPack({
      env,
      packKey: targetPackKey,
      packSize: targetHead!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
    });
    const backfillResolve = await resolveDeltasAndWriteIdx({
      env,
      packKey: targetPackKey,
      packSize: targetHead!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
      scanResult: backfillScan,
      activeCatalog,
      cacheCtx: createTestCacheContext("http://localhost/test"),
      repoId,
      writeIdx: false,
      existingIdxView: initialResolve.idxView,
    });

    expect(backfillResolve.idxBytes).toBe(0);
    expect(backfillScan.table.resolved.reduce((sum, value) => sum + value, 0)).toBe(2);
    expect(bytesToHex(backfillScan.table.oids.subarray(0, 20))).toBe(baseBlob.oid);
    expect(bytesToHex(backfillScan.table.oids.subarray(20, 40))).toBe(baseBlob.oid);
  });

  it("tries older external duplicate bases when the newest duplicate points back to the target pack", async () => {
    const targetPayload = new TextEncoder().encode("target prefix\n");
    const duplicateSuffix = new TextEncoder().encode("duplicate suffix\n");
    const duplicatePayload = new Uint8Array(targetPayload.length + duplicateSuffix.length);
    duplicatePayload.set(targetPayload, 0);
    duplicatePayload.set(duplicateSuffix, targetPayload.length);

    const targetOid = await computeOid("blob", targetPayload);
    const duplicateOid = await computeOid("blob", duplicatePayload);
    const repoId = uniqueRepoId();

    const olderPackKey = "test/ref-delta-cycle-older-base.pack";
    const olderPack = await buildPack([{ type: "blob", payload: duplicatePayload }]);
    await env.REPO_BUCKET.put(olderPackKey, olderPack);
    const olderHead = await env.REPO_BUCKET.head(olderPackKey);
    const olderScan = await scanPack({
      env,
      packKey: olderPackKey,
      packSize: olderHead!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
    });
    const olderResolve = await resolveDeltasAndWriteIdx({
      env,
      packKey: olderPackKey,
      packSize: olderHead!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
      scanResult: olderScan,
      repoId,
    });
    const olderRow = makeActiveCatalogRow({
      packKey: olderPackKey,
      packBytes: olderHead!.size,
      objectCount: olderScan.objectCount,
      idxBytes: olderResolve.idxBytes,
    });

    const targetPackKey = "test/ref-delta-cycle-target.pack";
    const targetPack = await buildPack([
      {
        type: "ref-delta",
        baseOid: duplicateOid,
        delta: buildCopyPrefixDelta(duplicatePayload, targetPayload.length),
      },
    ]);
    await env.REPO_BUCKET.put(targetPackKey, targetPack);
    const targetHead = await env.REPO_BUCKET.head(targetPackKey);
    const targetScan = await scanPack({
      env,
      packKey: targetPackKey,
      packSize: targetHead!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
    });
    const targetResolve = await resolveDeltasAndWriteIdx({
      env,
      packKey: targetPackKey,
      packSize: targetHead!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
      scanResult: targetScan,
      activeCatalog: [olderRow],
      cacheCtx: createTestCacheContext("http://localhost/test"),
      repoId,
    });
    const targetRow = makeActiveCatalogRow({
      packKey: targetPackKey,
      packBytes: targetHead!.size,
      objectCount: targetScan.objectCount,
      idxBytes: targetResolve.idxBytes,
    });

    const newerPackKey = "test/ref-delta-cycle-newer-duplicate.pack";
    const newerPack = await buildPack([
      {
        type: "ref-delta",
        baseOid: targetOid,
        delta: buildAppendOnlyDelta(targetPayload, duplicateSuffix),
      },
    ]);
    await env.REPO_BUCKET.put(newerPackKey, newerPack);
    const newerHead = await env.REPO_BUCKET.head(newerPackKey);
    const newerScan = await scanPack({
      env,
      packKey: newerPackKey,
      packSize: newerHead!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
    });
    const newerResolve = await resolveDeltasAndWriteIdx({
      env,
      packKey: newerPackKey,
      packSize: newerHead!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
      scanResult: newerScan,
      activeCatalog: [targetRow, olderRow],
      cacheCtx: createTestCacheContext("http://localhost/test"),
      repoId,
    });
    const newerRow = makeActiveCatalogRow({
      packKey: newerPackKey,
      packBytes: newerHead!.size,
      objectCount: newerScan.objectCount,
      idxBytes: newerResolve.idxBytes,
    });

    const backfillScan = await scanPack({
      env,
      packKey: targetPackKey,
      packSize: targetHead!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
    });
    const backfillResolve = await resolveDeltasAndWriteIdx({
      env,
      packKey: targetPackKey,
      packSize: targetHead!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
      scanResult: backfillScan,
      activeCatalog: [newerRow, olderRow],
      cacheCtx: createTestCacheContext("http://localhost/test"),
      repoId,
      writeIdx: false,
      existingIdxView: targetResolve.idxView,
    });

    expect(backfillResolve.idxBytes).toBe(0);
    expect(backfillScan.table.resolved.reduce((sum, value) => sum + value, 0)).toBe(1);
    expect(bytesToHex(backfillScan.table.oids.subarray(0, 20))).toBe(targetOid);
  });

  it("wakes deferred OFS_DELTA children after an external-base REF_DELTA resolves", async () => {
    const baseBlobPayload = new TextEncoder().encode("base\n");
    const baseBlob = await encodeGitObject("blob", baseBlobPayload);
    const basePack = await buildPack([{ type: "blob", payload: baseBlobPayload }]);

    const basePackKey = "test/ref-delta-ofs-base.pack";
    await env.REPO_BUCKET.put(basePackKey, basePack);
    const baseHead = await env.REPO_BUCKET.head(basePackKey);

    const baseScan = await scanPack({
      env,
      packKey: basePackKey,
      packSize: baseHead!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
    });
    const repoId = uniqueRepoId();
    const baseResolve = await resolveDeltasAndWriteIdx({
      env,
      packKey: basePackKey,
      packSize: baseHead!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
      scanResult: baseScan,
      repoId,
    });

    const midSuffix = new TextEncoder().encode("mid\n");
    const finalSuffix = new TextEncoder().encode("final\n");

    const midPayload = new Uint8Array(baseBlobPayload.length + midSuffix.length);
    midPayload.set(baseBlobPayload, 0);
    midPayload.set(midSuffix, baseBlobPayload.length);

    const finalPayload = new Uint8Array(midPayload.length + finalSuffix.length);
    finalPayload.set(midPayload, 0);
    finalPayload.set(finalSuffix, midPayload.length);
    const finalOid = await computeOid("blob", finalPayload);

    const packBytes = await buildPack([
      {
        type: "ref-delta",
        baseOid: baseBlob.oid,
        delta: buildAppendOnlyDelta(baseBlobPayload, midSuffix),
      },
      {
        type: "ofs-delta",
        baseIndex: 0,
        delta: buildAppendOnlyDelta(midPayload, finalSuffix),
      },
    ]);

    const packKey = "test/resolve-ref-delta-ofs-dependent.pack";
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

    await resolveDeltasAndWriteIdx({
      env,
      packKey,
      packSize: head!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
      scanResult,
      activeCatalog: [
        makeActiveCatalogRow({
          packKey: basePackKey,
          packBytes: baseHead!.size,
          objectCount: baseScan.objectCount,
          idxBytes: baseResolve.idxBytes,
        }),
      ],
      cacheCtx: createTestCacheContext("http://localhost/test"),
      repoId,
    });

    expect(scanResult.table.resolved.reduce((sum, value) => sum + value, 0)).toBe(2);
    expect(bytesToHex(scanResult.table.oids.subarray(20, 40))).toBe(finalOid);
  });

  it("uses the caller cache context for thin-pack external-base reads", async () => {
    const baseBlobPayload = new TextEncoder().encode("external base content\n");
    const baseBlob = await encodeGitObject("blob", baseBlobPayload);
    const basePackKey = "test/ref-delta-budget-base.pack";
    const basePack = await buildPack([{ type: "blob", payload: baseBlobPayload }]);
    await env.REPO_BUCKET.put(basePackKey, basePack);
    const baseHead = await env.REPO_BUCKET.head(basePackKey);

    const baseScan = await scanPack({
      env,
      packKey: basePackKey,
      packSize: baseHead!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
    });

    const repoId = uniqueRepoId();
    const baseResolve = await resolveDeltasAndWriteIdx({
      env,
      packKey: basePackKey,
      packSize: baseHead!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
      scanResult: baseScan,
      repoId,
    });

    const suffix = new TextEncoder().encode("with shared limiter\n");
    const thinPackKey = "test/ref-delta-budget-thin.pack";
    const thinPack = await buildPack([
      {
        type: "ref-delta",
        baseOid: baseBlob.oid,
        delta: buildAppendOnlyDelta(baseBlobPayload, suffix),
      },
    ]);
    await env.REPO_BUCKET.put(thinPackKey, thinPack);
    const thinHead = await env.REPO_BUCKET.head(thinPackKey);

    const labels: string[] = [];
    const limiter = makeTracingLimiter(labels);
    const counter = { count: 0 };
    const cacheCtx = createTestCacheContext("http://localhost/test", 10);
    cacheCtx.memo = {
      ...(cacheCtx.memo || {}),
      limiter,
    };

    const thinScan = await scanPack({
      env,
      packKey: thinPackKey,
      packSize: thinHead!.size,
      limiter,
      countSubrequest: makeCountSubrequest(counter),
      log,
    });

    await resolveDeltasAndWriteIdx({
      env,
      packKey: thinPackKey,
      packSize: thinHead!.size,
      limiter,
      countSubrequest: makeCountSubrequest(counter),
      log,
      scanResult: thinScan,
      activeCatalog: [
        makeActiveCatalogRow({
          packKey: basePackKey,
          packBytes: baseHead!.size,
          objectCount: baseScan.objectCount,
          idxBytes: baseResolve.idxBytes,
        }),
      ],
      cacheCtx,
      repoId,
    });

    expect(labels).toContain("r2:get-pack-idx");
    expect(labels).toContain("r2:get-range");
    expect(cacheCtx.memo?.subreqBudget).toBeLessThan(10);
  });
});
