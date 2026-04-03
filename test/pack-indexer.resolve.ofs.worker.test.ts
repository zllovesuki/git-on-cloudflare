import { describe, expect, it } from "vitest";
import { env } from "cloudflare:test";

import { buildPack, buildAppendOnlyDelta } from "./util/git-pack.ts";
import { uniqueRepoId } from "./util/test-helpers.ts";
import {
  makeCountSubrequest,
  makeLimiter,
  makeTracingLimiter,
  packIndexerLog as log,
} from "./util/pack-indexer.helpers.ts";

import {
  allocateEntryTable,
  isResolveAbortedError,
  scanPack,
  resolveDeltasAndWriteIdx,
} from "@/git/pack/indexer/index.ts";
import { computeOid } from "@/git/core/objects.ts";
import { bytesToHex } from "@/common/hex.ts";
import { DEFAULT_SUBREQUEST_BUDGET } from "@/git/operations/limits.ts";
import { getOidHexAt, parseIdxView } from "@/git/object-store/index.ts";
import { packIndexKey } from "@/keys.ts";
import { getBasePayload } from "@/git/pack/indexer/resolve/materialize.ts";
import { PayloadLRU } from "@/git/pack/indexer/resolve/payloadCache.ts";
import { SequentialReader } from "@/git/pack/indexer/resolve/reader.ts";

async function expectResolveAborted(promise: Promise<unknown>): Promise<void> {
  try {
    await promise;
  } catch (error) {
    expect(isResolveAbortedError(error)).toBe(true);
    return;
  }
  throw new Error("expected resolve to abort");
}

describe("resolveDeltasAndWriteIdx OFS_DELTA", () => {
  it("resolves OFS_DELTA and writes valid idx", async () => {
    const baseBlobPayload = new TextEncoder().encode("base content here\n");
    const suffix = new TextEncoder().encode("appended text\n");
    const delta = buildAppendOnlyDelta(baseBlobPayload, suffix);

    const expectedPayload = new Uint8Array(baseBlobPayload.length + suffix.length);
    expectedPayload.set(baseBlobPayload, 0);
    expectedPayload.set(suffix, baseBlobPayload.length);
    const expectedOid = await computeOid("blob", expectedPayload);

    const packBytes = await buildPack([
      { type: "blob", payload: baseBlobPayload },
      { type: "ofs-delta", baseIndex: 0, delta },
    ]);

    const packKey = "test/resolve-ofs.pack";
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

    expect(scanResult.objectCount).toBe(2);
    expect(scanResult.table.resolved[0]).toBe(1);
    expect(scanResult.table.resolved[1]).toBe(0);

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

    expect(scanResult.table.resolved[1]).toBe(1);
    expect(bytesToHex(scanResult.table.oids.subarray(20, 40))).toBe(expectedOid);

    const idxObj = await env.REPO_BUCKET.get(packIndexKey(packKey));
    expect(idxObj).not.toBeNull();

    const idxBuf = new Uint8Array(await idxObj!.arrayBuffer());
    const idxView = parseIdxView(packKey, idxBuf, head!.size);
    expect(idxView).not.toBeUndefined();
    expect(idxView!.count).toBe(2);
    expect([getOidHexAt(idxView!, 0), getOidHexAt(idxView!, 1)]).toContain(expectedOid);

    expect(resolveResult.idxView.count).toBe(2);
    expect(resolveResult.objectCount).toBe(2);
  });

  it("rejects an already-aborted resolve before writing idx", async () => {
    const baseBlobPayload = new TextEncoder().encode("abort base\n");
    const suffix = new TextEncoder().encode("abort tail\n");
    const packKey = "test/resolve-aborted-before-start.pack";
    const packBytes = await buildPack([
      { type: "blob", payload: baseBlobPayload },
      { type: "ofs-delta", baseIndex: 0, delta: buildAppendOnlyDelta(baseBlobPayload, suffix) },
    ]);
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

    const abortController = new AbortController();
    abortController.abort();

    await expectResolveAborted(
      resolveDeltasAndWriteIdx({
        env,
        packKey,
        packSize: head!.size,
        limiter: makeLimiter(),
        countSubrequest: () => {},
        log,
        scanResult,
        repoId: uniqueRepoId(),
        signal: abortController.signal,
      })
    );

    expect(await env.REPO_BUCKET.get(packIndexKey(packKey))).toBeNull();
  });

  it("re-materializes evicted bases when the LRU budget is tiny", async () => {
    const baseBlobPayload = new TextEncoder().encode("base\n");
    const midSuffix = new TextEncoder().encode("mid\n");
    const finalSuffix = new TextEncoder().encode("final\n");
    const midDelta = buildAppendOnlyDelta(baseBlobPayload, midSuffix);

    const midPayload = new Uint8Array(baseBlobPayload.length + midSuffix.length);
    midPayload.set(baseBlobPayload, 0);
    midPayload.set(midSuffix, baseBlobPayload.length);
    const finalDelta = buildAppendOnlyDelta(midPayload, finalSuffix);

    const finalPayload = new Uint8Array(midPayload.length + finalSuffix.length);
    finalPayload.set(midPayload, 0);
    finalPayload.set(finalSuffix, midPayload.length);
    const finalOid = await computeOid("blob", finalPayload);

    const packBytes = await buildPack([
      { type: "blob", payload: baseBlobPayload },
      { type: "ofs-delta", baseIndex: 0, delta: midDelta },
      { type: "ofs-delta", baseIndex: 1, delta: finalDelta },
    ]);

    const packKey = "test/resolve-lru-rematerialize.pack";
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
      lruBudget: 1,
    });

    expect(bytesToHex(scanResult.table.oids.subarray(40, 60))).toBe(finalOid);
  });

  it("routes idx writes through the shared limiter and subrequest counter", async () => {
    const baseBlobPayload = new TextEncoder().encode("budget test\n");
    const packBytes = await buildPack([{ type: "blob", payload: baseBlobPayload }]);

    const packKey = "test/subreq-budget.pack";
    await env.REPO_BUCKET.put(packKey, packBytes);
    const head = await env.REPO_BUCKET.head(packKey);

    const labels: string[] = [];
    const limiter = makeTracingLimiter(labels);
    const counter = { count: 0 };
    const scanResult = await scanPack({
      env,
      packKey,
      packSize: head!.size,
      limiter,
      countSubrequest: makeCountSubrequest(counter),
      log,
    });

    const repoId = uniqueRepoId();
    await resolveDeltasAndWriteIdx({
      env,
      packKey,
      packSize: head!.size,
      limiter,
      countSubrequest: makeCountSubrequest(counter),
      log,
      scanResult,
      repoId,
    });

    expect(labels).toContain("r2:get-range");
    expect(labels).toContain("r2:put-pack-idx");
    expect(counter.count).toBeGreaterThan(0);
    expect(counter.count).toBeLessThan(DEFAULT_SUBREQUEST_BUDGET);
  });

  it("streams pass-2 inflates in multiple range reads when the resolve chunk size is tiny", async () => {
    const baseBlobPayload = new Uint8Array(8 * 1024);
    for (let i = 0; i < baseBlobPayload.length; i++) {
      baseBlobPayload[i] = (i * 31) & 0xff;
    }

    const suffix = new Uint8Array(4 * 1024);
    for (let i = 0; i < suffix.length; i++) {
      suffix[i] = (255 - i * 17) & 0xff;
    }

    const expectedPayload = new Uint8Array(baseBlobPayload.length + suffix.length);
    expectedPayload.set(baseBlobPayload, 0);
    expectedPayload.set(suffix, baseBlobPayload.length);
    const expectedOid = await computeOid("blob", expectedPayload);

    const packKey = "test/resolve-streamed-pass2.pack";
    const packBytes = await buildPack([
      { type: "blob", payload: baseBlobPayload },
      { type: "ofs-delta", baseIndex: 0, delta: buildAppendOnlyDelta(baseBlobPayload, suffix) },
    ]);
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

    const labels: string[] = [];
    const limiter = makeTracingLimiter(labels);
    const counter = { count: 0 };

    await resolveDeltasAndWriteIdx({
      env,
      packKey,
      packSize: head!.size,
      chunkSize: 32,
      limiter,
      countSubrequest: makeCountSubrequest(counter),
      log,
      scanResult,
      repoId: uniqueRepoId(),
    });

    expect(bytesToHex(scanResult.table.oids.subarray(20, 40))).toBe(expectedOid);
    expect(labels.filter((label) => label === "r2:get-range").length).toBeGreaterThan(1);
    expect(counter.count).toBeLessThan(DEFAULT_SUBREQUEST_BUDGET);
  });

  it("aborts mid-resolve without writing an idx", async () => {
    const entries: (
      | { type: "blob"; payload: Uint8Array }
      | { type: "ofs-delta"; baseIndex: number; delta: Uint8Array }
    )[] = [];
    let currentPayload = new TextEncoder().encode("base\n");
    entries.push({ type: "blob", payload: currentPayload });

    for (let i = 0; i < 96; i++) {
      const suffix = new TextEncoder().encode(String.fromCharCode(97 + (i % 26)));
      entries.push({
        type: "ofs-delta",
        baseIndex: i,
        delta: buildAppendOnlyDelta(currentPayload, suffix),
      });

      const nextPayload = new Uint8Array(currentPayload.length + suffix.length);
      nextPayload.set(currentPayload, 0);
      nextPayload.set(suffix, currentPayload.length);
      currentPayload = nextPayload;
    }

    const packKey = "test/resolve-mid-abort.pack";
    const packBytes = await buildPack(entries);
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

    const abortController = new AbortController();
    const counter = { count: 0 };
    await expectResolveAborted(
      resolveDeltasAndWriteIdx({
        env,
        packKey,
        packSize: head!.size,
        chunkSize: 16,
        limiter: makeLimiter(),
        countSubrequest: (n = 1) => {
          counter.count += n;
          if (counter.count >= 8 && !abortController.signal.aborted) {
            abortController.abort();
          }
        },
        log,
        scanResult,
        repoId: uniqueRepoId(),
        lruBudget: 1,
        signal: abortController.signal,
      })
    );

    expect(counter.count).toBeGreaterThanOrEqual(8);
    expect(await env.REPO_BUCKET.get(packIndexKey(packKey))).toBeNull();
  });

  it("re-materializes a longer OFS chain when the LRU budget is tiny", async () => {
    const entries: (
      | { type: "blob"; payload: Uint8Array }
      | { type: "ofs-delta"; baseIndex: number; delta: Uint8Array }
    )[] = [];
    let currentPayload = new TextEncoder().encode("base\n");
    entries.push({ type: "blob", payload: currentPayload });

    for (let i = 0; i < 128; i++) {
      const suffix = new TextEncoder().encode(String.fromCharCode(97 + (i % 26)));
      entries.push({
        type: "ofs-delta",
        baseIndex: i,
        delta: buildAppendOnlyDelta(currentPayload, suffix),
      });

      const nextPayload = new Uint8Array(currentPayload.length + suffix.length);
      nextPayload.set(currentPayload, 0);
      nextPayload.set(suffix, currentPayload.length);
      currentPayload = nextPayload;
    }

    const packKey = "test/resolve-long-ofs-rematerialize.pack";
    const packBytes = await buildPack(entries);
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
      repoId: uniqueRepoId(),
      lruBudget: 1,
    });

    const expectedOid = await computeOid("blob", currentPayload);
    const lastStart = scanResult.table.oids.length - 20;
    expect(bytesToHex(scanResult.table.oids.subarray(lastStart, lastStart + 20))).toBe(expectedOid);
  });

  it("fails fast when rematerialization encounters a cyclic base chain", async () => {
    const table = allocateEntryTable(2);
    table.types[0] = 6;
    table.types[1] = 6;

    const reader = new SequentialReader(
      env,
      "test/materialize-cycle.pack",
      0,
      1,
      makeLimiter(),
      () => {},
      log
    );
    const baseIndex = new Int32Array([1, 0]);

    await expect(
      getBasePayload(
        {
          env,
          packKey: "test/materialize-cycle.pack",
          packSize: 0,
          limiter: makeLimiter(),
          countSubrequest: () => {},
          log,
          scanResult: {
            table,
            refBaseOids: new Uint8Array(40),
            refDeltaCount: 0,
            resolvedCount: 2,
            objectCount: 2,
            packChecksum: new Uint8Array(20),
          },
          repoId: uniqueRepoId(),
        },
        0,
        new PayloadLRU(1, 2),
        reader,
        table,
        baseIndex
      )
    ).rejects.toThrow(/cycle or runaway traversal/);
  });
});
