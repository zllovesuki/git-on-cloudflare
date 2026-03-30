/**
 * Delta resolution and idx writing (Pass 2).
 *
 * Processes entries from the scan result in pack-offset order, resolves delta
 * chains, computes OIDs, and writes a standard Git idx v2 file to R2.
 *
 * Processing in offset order naturally satisfies the OFS_DELTA dependency
 * ordering (bases are always at lower offsets). REF_DELTA entries are first
 * given every chance to resolve against later in-pack deltas; only the
 * remaining unresolved roots fall back to the active pack snapshot.
 *
 * Base payloads are held in a byte-budgeted LRU cache. On eviction, bases can
 * be recomputed from the pack via buffered pack reads. The typical case
 * requires zero extra reads beyond the sequential pass.
 */

import type { CacheContext } from "@/cache/index.ts";
import { bytesToHex } from "@/common/hex.ts";
import { computeOidBytes, objTypeCode } from "@/git/core/objects.ts";
import { applyGitDelta } from "@/git/object-store/delta.ts";
import { parseIdxView } from "@/git/object-store/idxView.ts";
import { readObject } from "@/git/object-store/store.ts";
import { ensureMemo, typeCodeToObjectType } from "@/git/object-store/support.ts";
import { packIndexKey } from "@/keys.ts";

import { searchOffsetIndex, getRefBaseOidAt } from "../types.ts";
import type { ResolveOptions, ResolveResult } from "../types.ts";
import { writeIdxV2 } from "../writeIdx.ts";

import { drainReadyDeferredQueue } from "./deferred.ts";
import {
  createInPackDependencyQueue,
  enqueueReadyDeferred,
  promoteReadyInPackDependents,
  registerInPackDependency,
} from "./dependencies.ts";
import { throwIfAborted } from "./errors.ts";
import { resolveDeltaEntry, storeOid } from "./helpers.ts";
import { PayloadLRU } from "./payloadCache.ts";
import { inflateFromReader, SequentialReader } from "./reader.ts";
import {
  createRefBaseLookup,
  enqueueWaitingRefDelta,
  getResolvedBaseEntry,
  noteResolvedEntry,
  promoteWaitingRefDeltas,
  type RefBaseLookup,
} from "./refLookup.ts";
import type { InPackDependencyQueue } from "./dependencies.ts";

const DEFAULT_LRU_BUDGET = 32 * 1024 * 1024; // 32 MiB
const DEFAULT_CHUNK_SIZE = 4_194_304; // 4 MiB — larger chunks reduce R2 reads during resolve

export async function resolveDeltasAndWriteIdx(opts: ResolveOptions): Promise<ResolveResult> {
  const { env, packKey, packSize, log, scanResult, repoId, lruBudget } = opts;
  const { table, objectCount, packChecksum } = scanResult;
  const initialResolvedCount = scanResult.resolvedCount;
  const unresolvedCount = objectCount - initialResolvedCount;

  const resolveCacheCtx = ensureResolveCacheContext(
    opts.cacheCtx,
    repoId,
    opts.activeCatalog,
    opts.limiter
  );
  const resolveOpts =
    opts.cacheCtx === resolveCacheCtx ? opts : { ...opts, cacheCtx: resolveCacheCtx };

  log.info("resolve:start", { objectCount, unresolvedCount });
  throwIfAborted(opts.signal, log, "resolve:start");
  if (unresolvedCount === 0) {
    return await writeAndParseIdx(resolveOpts, packKey, packSize, table, objectCount, packChecksum);
  }

  const lru = new PayloadLRU(lruBudget ?? DEFAULT_LRU_BUDGET, objectCount);
  const chunkSize = opts.chunkSize ?? DEFAULT_CHUNK_SIZE;
  // Two readers: one for the main sequential pass, one for rematerialization
  // and deferred work so cache misses do not trash the main pass window.
  const seqReader = new SequentialReader(
    env,
    packKey,
    packSize,
    chunkSize,
    opts.limiter,
    opts.countSubrequest,
    log,
    opts.signal
  );
  const auxReader = new SequentialReader(
    env,
    packKey,
    packSize,
    chunkSize,
    opts.limiter,
    opts.countSubrequest,
    log,
    opts.signal
  );

  const isBase = new Uint8Array(objectCount);
  const baseIndex = new Int32Array(objectCount).fill(-1);
  const deadlines = new Uint32Array(objectCount);
  const resolvedTypeCodes = new Uint8Array(objectCount);

  for (let i = 0; i < objectCount; i++) {
    if (table.resolved[i]) {
      resolvedTypeCodes[i] = table.types[i];
    }
  }

  if (scanResult.refDeltaCount === 0) {
    buildOfsDependencies(table, objectCount, isBase, baseIndex, deadlines);
    propagateDeadlines(table, objectCount, baseIndex, deadlines);
    lru.setDeadlines(deadlines);
    await seqReader.preload(table.offsets[0]);

    let resolved = initialResolvedCount;
    for (let i = 0; i < objectCount; i++) {
      throwIfAborted(opts.signal, log, "resolve:ofs-main");
      lru.setCurrentOffset(table.offsets[i]);
      if (table.resolved[i]) {
        await cacheResolvedBaseIfNeeded(table, i, isBase, resolvedTypeCodes, lru, seqReader);
        continue;
      }
      const bi = baseIndex[i];
      if (bi < 0 || !table.resolved[bi]) {
        throw new Error(`resolve: OFS_DELTA base is not ready for entry ${i}`);
      }
      await resolveDeltaEntry({
        index: i,
        resolveOpts,
        table,
        lru,
        deltaReader: seqReader,
        baseReader: auxReader,
        baseIndex,
        resolvedTypeCodes,
        isBase,
        deadlines,
        refLookup: null,
      });
      resolved++;
      logResolveProgress(log, resolved, initialResolvedCount, unresolvedCount);
    }

    if (resolved !== objectCount) {
      throw new Error(`resolve: ${objectCount - resolved} objects could not be resolved`);
    }
    return await writeAndParseIdx(resolveOpts, packKey, packSize, table, objectCount, packChecksum);
  }

  const refLookup = createRefBaseLookup(scanResult);
  if (!refLookup) {
    throw new Error("resolve: expected REF_DELTA lookup state for mixed pack");
  }

  for (let i = 0; i < objectCount; i++) {
    if (table.resolved[i]) {
      noteResolvedEntry(refLookup, table.oids, i);
    }
  }

  // Mixed packs need two kinds of dependency bookkeeping:
  // 1) REF_DELTA entries whose base OID is not mapped to an in-pack entry yet.
  //    Those stay in `refLookup` until some later resolution publishes the OID.
  // 2) Deltas whose in-pack base entry is already known but not resolved yet.
  //    Those register in the dependency queue so a late base can wake them in
  //    O(children) time without rescanning the whole deferred set.
  const dependencyQueue = createInPackDependencyQueue(objectCount);
  buildMixedDependencies(scanResult, isBase, baseIndex, deadlines, refLookup, dependencyQueue);
  propagateDeadlines(table, objectCount, baseIndex, deadlines);
  lru.setDeadlines(deadlines);
  await seqReader.preload(table.offsets[0]);

  const deferred: number[] = [];
  const deferredQueued = new Uint8Array(objectCount);
  const readyDeferred: number[] = [];
  let resolved = initialResolvedCount;

  for (let i = 0; i < objectCount; i++) {
    throwIfAborted(opts.signal, log, "resolve:mixed-main");
    lru.setCurrentOffset(table.offsets[i]);

    if (table.resolved[i]) {
      await cacheResolvedBaseIfNeeded(table, i, isBase, resolvedTypeCodes, lru, seqReader);
      continue;
    }

    const bi = baseIndex[i];
    if (table.types[i] === 7 && bi < 0) {
      // A missing base OID at first sight is ambiguous: it may be a true thin
      // pack external, or a later in-pack delta/base whose OID is not known
      // yet. Defer external fallback until the in-pack promotion path is done.
      deferred.push(i);
      continue;
    }
    if (bi < 0 || !table.resolved[bi]) {
      deferred.push(i);
      continue;
    }

    await resolveDeltaEntry({
      index: i,
      resolveOpts,
      table,
      lru,
      deltaReader: seqReader,
      baseReader: auxReader,
      baseIndex,
      resolvedTypeCodes,
      isBase,
      deadlines,
      refLookup,
      dependencyQueue,
      readyDeferred,
      deferredQueued,
    });
    resolved++;
    logResolveProgress(log, resolved, initialResolvedCount, unresolvedCount);
  }

  for (const index of deferred) {
    enqueueReadyDeferred(readyDeferred, deferredQueued, table, baseIndex, index);
  }

  resolved = await drainReadyDeferredQueue({
    readyDeferred,
    deferredQueued,
    resolved,
    initialResolvedCount,
    totalUnresolved: unresolvedCount,
    log,
    resolveOpts,
    table,
    lru,
    reader: auxReader,
    resolvedTypeCodes,
    refLookup,
    dependencyQueue,
    baseIndex,
    isBase,
    deadlines,
  });

  for (const index of deferred) {
    throwIfAborted(opts.signal, log, "resolve:external-fallback");
    if (table.resolved[index]) continue;

    const bi = baseIndex[index];
    if (bi >= 0) {
      // This entry now has an in-pack base and just needs the shared deferred
      // drain to pick it up. Re-enqueue it instead of treating it as a thin
      // pack external-base case.
      enqueueReadyDeferred(readyDeferred, deferredQueued, table, baseIndex, index);
      continue;
    }

    // Still no in-pack base after all promotions. At this point a REF_DELTA is
    // a true thin-pack external lookup, so fall back to the active catalog.
    const baseOid = bytesToHex(getRefBaseOidAt(scanResult.refBaseOids, index));
    const baseObj = await readObject(env, repoId, baseOid, resolveCacheCtx);
    if (!baseObj) continue;

    lru.setCurrentOffset(table.offsets[index]);
    const deltaPayload = await inflateFromReader(auxReader, table, index);
    throwIfAborted(opts.signal, log, "resolve:external-fallback");
    const result = applyGitDelta(baseObj.payload, deltaPayload);
    if (result.length !== table.decompressedSizes[index]) {
      throw new Error(
        `resolve: deferred delta result size mismatch at offset ${table.offsets[index]} (expected ${table.decompressedSizes[index]}, got ${result.length})`
      );
    }

    storeOid(table, index, await computeOidBytes(baseObj.type, result));
    resolvedTypeCodes[index] = objTypeCode(baseObj.type);
    promoteWaitingRefDeltas(
      refLookup,
      index,
      table,
      baseIndex,
      isBase,
      deadlines,
      readyDeferred,
      deferredQueued
    );
    promoteReadyInPackDependents(
      dependencyQueue,
      index,
      readyDeferred,
      deferredQueued,
      table,
      baseIndex
    );
    if (isBase[index]) {
      lru.set(index, { type: baseObj.type, payload: result });
    }
    resolved++;
    logResolveProgress(log, resolved, initialResolvedCount, unresolvedCount);
    resolved = await drainReadyDeferredQueue({
      readyDeferred,
      deferredQueued,
      resolved,
      initialResolvedCount,
      totalUnresolved: unresolvedCount,
      log,
      resolveOpts,
      table,
      lru,
      reader: auxReader,
      resolvedTypeCodes,
      refLookup,
      dependencyQueue,
      baseIndex,
      isBase,
      deadlines,
    });
  }

  resolved = await drainReadyDeferredQueue({
    readyDeferred,
    deferredQueued,
    resolved,
    initialResolvedCount,
    totalUnresolved: unresolvedCount,
    log,
    resolveOpts,
    table,
    lru,
    reader: auxReader,
    resolvedTypeCodes,
    refLookup,
    dependencyQueue,
    baseIndex,
    isBase,
    deadlines,
  });

  if (resolved !== objectCount) {
    throw new Error(`resolve: ${objectCount - resolved} objects could not be resolved`);
  }

  return await writeAndParseIdx(resolveOpts, packKey, packSize, table, objectCount, packChecksum);
}

function buildOfsDependencies(
  table: ResolveOptions["scanResult"]["table"],
  objectCount: number,
  isBase: Uint8Array,
  baseIndex: Int32Array,
  deadlines: Uint32Array
): void {
  for (let i = 0; i < objectCount; i++) {
    if (table.resolved[i]) continue;
    const baseOff = table.ofsBaseOffsets[i];
    const bi = searchOffsetIndex(table.offsets, baseOff);
    if (bi < 0) {
      throw new Error(
        `resolve: OFS_DELTA at offset ${table.offsets[i]} references unknown base offset ${baseOff}`
      );
    }
    baseIndex[i] = bi;
    isBase[bi] = 1;
    deadlines[bi] = Math.max(deadlines[bi], table.offsets[i]);
  }
}

function buildMixedDependencies(
  scanResult: ResolveOptions["scanResult"],
  isBase: Uint8Array,
  baseIndex: Int32Array,
  deadlines: Uint32Array,
  refLookup: RefBaseLookup,
  dependencyQueue: InPackDependencyQueue
): void {
  const { table, objectCount } = scanResult;
  for (let i = 0; i < objectCount; i++) {
    if (table.resolved[i]) continue;

    if (table.types[i] === 6) {
      const baseOff = table.ofsBaseOffsets[i];
      const bi = searchOffsetIndex(table.offsets, baseOff);
      if (bi < 0) {
        throw new Error(
          `resolve: OFS_DELTA at offset ${table.offsets[i]} references unknown base offset ${baseOff}`
        );
      }
      baseIndex[i] = bi;
      isBase[bi] = 1;
      deadlines[bi] = Math.max(deadlines[bi], table.offsets[i]);
      if (!table.resolved[bi]) {
        // OFS_DELTA entries know their base slot up front, so record the edge
        // now when that base is still unresolved. Once the base resolves later
        // in the pass, the dependency queue can wake this child immediately.
        registerInPackDependency(dependencyQueue, baseIndex, i);
      }
      continue;
    }

    if (table.types[i] !== 7) continue;
    const bi = getResolvedBaseEntry(refLookup, i);
    if (bi >= 0) {
      // This REF_DELTA already points at an in-pack entry whose OID was known
      // during scan or was published by an earlier resolved base.
      baseIndex[i] = bi;
      isBase[bi] = 1;
      deadlines[bi] = Math.max(deadlines[bi], table.offsets[i]);
      continue;
    }
    // The base OID is not mapped to an in-pack entry yet. Keep it in the
    // REF lookup so a later resolution can promote it, or the caller can treat
    // it as a thin-pack external base once in-pack promotion is exhausted.
    enqueueWaitingRefDelta(refLookup, i);
  }
}

function propagateDeadlines(
  table: ResolveOptions["scanResult"]["table"],
  objectCount: number,
  baseIndex: Int32Array,
  deadlines: Uint32Array
): void {
  // The initial dependency graph only knows about OFS_DELTA edges and REF_DELTA
  // edges whose base already resolved during the scan pass. A reverse sweep is
  // enough to push each entry's last-needed offset into its in-pack base chain
  // without rewalking ancestors for every node.
  for (let i = objectCount - 1; i >= 0; i--) {
    if (table.resolved[i]) continue;
    const bi = baseIndex[i];
    if (bi < 0) continue;
    const neededUntil = Math.max(table.offsets[i], deadlines[i]);
    if (deadlines[bi] < neededUntil) deadlines[bi] = neededUntil;
  }
}

async function cacheResolvedBaseIfNeeded(
  table: ResolveOptions["scanResult"]["table"],
  index: number,
  isBase: Uint8Array,
  resolvedTypeCodes: Uint8Array,
  lru: PayloadLRU,
  reader: SequentialReader
): Promise<void> {
  if (!isBase[index] || lru.get(index)) return;
  const t = typeCodeToObjectType(resolvedTypeCodes[index]);
  if (!t) return;
  const payload = await inflateFromReader(reader, table, index);
  lru.set(index, { type: t, payload });
}

function logResolveProgress(
  log: ResolveOptions["log"],
  resolved: number,
  initialResolvedCount: number,
  unresolvedCount: number
): void {
  const newlyResolved = resolved - initialResolvedCount;
  if (newlyResolved > 0 && newlyResolved % 10000 === 0) {
    log.debug("resolve:progress", { resolved: newlyResolved, total: unresolvedCount });
  }
}

function ensureResolveCacheContext(
  cacheCtx: CacheContext | undefined,
  repoId: string,
  activeCatalog: ResolveOptions["activeCatalog"],
  limiter: ResolveOptions["limiter"]
): CacheContext {
  const resolvedCacheCtx =
    cacheCtx ??
    ({
      req: new Request("http://localhost"),
      ctx: {
        waitUntil() {},
        passThroughOnException() {},
      } as unknown as ExecutionContext,
      memo: {},
    } satisfies CacheContext);

  ensureMemo(resolvedCacheCtx, repoId);
  resolvedCacheCtx.memo = resolvedCacheCtx.memo || {};
  if (!resolvedCacheCtx.memo.limiter) {
    // External-base reads must share the caller's limiter/budget. If they
    // silently allocate their own memo state here, the receive path can exceed
    // platform limits without the main request noticing.
    resolvedCacheCtx.memo.limiter = limiter;
  }
  if (activeCatalog) {
    // The caller-supplied snapshot is the authority for this resolve pass. If
    // the memo already holds older pack-catalog state, thin-pack validation
    // must not drift back to it when external bases are resolved.
    resolvedCacheCtx.memo.packCatalog = activeCatalog;
  }
  return resolvedCacheCtx;
}

async function putPackIdx(opts: ResolveOptions, idxBuf: Uint8Array): Promise<void> {
  const idxKey = packIndexKey(opts.packKey);
  throwIfAborted(opts.signal, opts.log, "resolve:put-pack-idx");
  // Count the idx write under the same request budget as the pack reads. R2
  // writes are subject to the same platform request limits in practice, so
  // skipping them here makes the validation numbers too optimistic.
  opts.countSubrequest();
  await opts.limiter.run("r2:put-pack-idx", async () => {
    await opts.env.REPO_BUCKET.put(idxKey, idxBuf);
  });
}

async function writeAndParseIdx(
  opts: ResolveOptions,
  packKey: string,
  packSize: number,
  table: ResolveOptions["scanResult"]["table"],
  objectCount: number,
  packChecksum: Uint8Array
): Promise<ResolveResult> {
  throwIfAborted(opts.signal, opts.log, "resolve:write-idx");
  const idxBuf = await writeIdxV2(table, objectCount, packChecksum);
  throwIfAborted(opts.signal, opts.log, "resolve:write-idx");
  await putPackIdx(opts, idxBuf);
  const idxView = parseIdxView(packKey, idxBuf, packSize);
  if (!idxView) throw new Error("resolve: failed to parse generated idx");
  opts.log.info("resolve:done", { objectCount, idxBytes: idxBuf.byteLength });
  return { objectCount, idxBytes: idxBuf.byteLength, idxView };
}
