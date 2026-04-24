import type { CacheContext } from "@/cache/index.ts";
import type { IdxView } from "./types.ts";

import { bytesToHex, createLogger, hexToBytes } from "@/common/index.ts";
import { packIndexKey } from "@/keys.ts";
import { countSubrequest, getLimiter } from "@/git/operations/limits.ts";

const IDX_VIEW_CACHE_MAX_BYTES = 16 * 1024 * 1024;
const IDX_TRAILER_BYTES = 40;
const UINT32_SPAN = 0x1_0000_0000;
type CachedIdxView = {
  view: IdxView;
  bytes: number;
};

const idxViewCache = new Map<string, CachedIdxView>();
let idxViewCacheBytes = 0;

function getIdxViewCacheKey(packKey: string, packSize: number): string {
  // Hinted loads can be wrong if a stale catalog row slips through. Key the
  // isolate-shared cache by both pack key and pack size so a bad hint can only
  // waste cache space, not poison the correct entry for later requests.
  return `${packKey}\0${packSize}`;
}

function estimateIdxViewBytes(view: IdxView): number {
  return (
    view.fanout.byteLength +
    view.rawNames.byteLength +
    view.offsets.byteLength +
    view.nextOffsetByIndex.byteLength +
    view.sortedOffsets.byteLength +
    view.sortedOffsetIndices.byteLength +
    view.packChecksum.byteLength +
    view.idxChecksum.byteLength
  );
}

function touchIdxViewCache(packKey: string, packSize: number, value: IdxView) {
  const key = getIdxViewCacheKey(packKey, packSize);
  const existing = idxViewCache.get(key);
  if (existing) {
    idxViewCache.delete(key);
    idxViewCacheBytes -= existing.bytes;
  }

  const bytes = estimateIdxViewBytes(value);
  // The request-local memo already pins hot idx views for the lifetime of one
  // request. The global cache is only a cross-request accelerator, so cap it by
  // bytes instead of entry count to avoid a few large idx files crowding out the
  // worker heap.
  if (bytes > IDX_VIEW_CACHE_MAX_BYTES) return;

  idxViewCache.set(key, { view: value, bytes });
  idxViewCacheBytes += bytes;

  while (idxViewCacheBytes > IDX_VIEW_CACHE_MAX_BYTES) {
    const firstKey = idxViewCache.keys().next().value;
    if (!firstKey) break;
    const first = idxViewCache.get(firstKey);
    idxViewCache.delete(firstKey);
    idxViewCacheBytes -= first?.bytes ?? 0;
  }
}

function logOnce(cacheCtx: CacheContext | undefined, flag: string, fn: () => void) {
  if (!cacheCtx) {
    fn();
    return;
  }
  cacheCtx.memo = cacheCtx.memo || {};
  cacheCtx.memo.flags = cacheCtx.memo.flags || new Set<string>();
  if (cacheCtx.memo.flags.has(flag)) return;
  fn();
  cacheCtx.memo.flags.add(flag);
}

export function getOidHexAt(view: IdxView, index: number): string {
  const start = index * 20;
  return bytesToHex(view.rawNames.subarray(start, start + 20));
}

function compareOidAt(view: IdxView, index: number, needle: Uint8Array, needleStart = 0): number {
  const start = index * 20;
  for (let i = 0; i < 20; i++) {
    const diff = view.rawNames[start + i] - needle[needleStart + i];
    if (diff !== 0) return diff;
  }
  return 0;
}

function readUint64AsNumber(dv: DataView, pos: number): number {
  const hi = dv.getUint32(pos, false);
  const lo = dv.getUint32(pos + 4, false);
  const value = hi * UINT32_SPAN + lo;
  if (!Number.isSafeInteger(value)) {
    throw new Error(
      `idx: 64-bit offset 0x${hi.toString(16)}${lo.toString(16).padStart(8, "0")} exceeds safe integer support`
    );
  }
  return value;
}

/**
 * Binary search for the entry index at a given pack byte offset.
 * Returns the entry index, or undefined if not found.
 */
export function findOffsetIndex(view: IdxView, offset: number): number | undefined {
  const arr = view.sortedOffsets;
  let lo = 0;
  let hi = arr.length - 1;
  while (lo <= hi) {
    const mid = (lo + hi) >>> 1;
    const v = arr[mid];
    if (v === offset) return view.sortedOffsetIndices[mid];
    if (v < offset) lo = mid + 1;
    else hi = mid - 1;
  }
  return undefined;
}

/**
 * Get the next pack offset after the given offset (the start of the next entry),
 * or `packSize - 20` for the last entry. Returns undefined if offset is not found.
 */
export function getNextOffset(view: IdxView, offset: number): number | undefined {
  const index = findOffsetIndex(view, offset);
  return index === undefined ? undefined : view.nextOffsetByIndex[index];
}

export function getNextOffsetByIndex(view: IdxView, index: number): number | undefined {
  if (index < 0 || index >= view.count) return undefined;
  return view.nextOffsetByIndex[index];
}

export function findOidIndexFromBytes(view: IdxView, needle: Uint8Array, needleStart = 0): number {
  if (needleStart < 0 || needleStart + 20 > needle.byteLength) return -1;

  const first = needle[needleStart];
  const lo = first === 0 ? 0 : view.fanout[first - 1] || 0;
  const hi = (view.fanout[first] || 0) - 1;
  if (hi < lo) return -1;

  let left = lo;
  let right = hi;
  while (left <= right) {
    const mid = (left + right) >> 1;
    const cmp = compareOidAt(view, mid, needle, needleStart);
    if (cmp === 0) return mid;
    if (cmp < 0) left = mid + 1;
    else right = mid - 1;
  }
  return -1;
}

export function findOidIndex(view: IdxView, oid: string | Uint8Array): number {
  const needle = typeof oid === "string" ? hexToBytes(oid.toLowerCase()) : oid;
  if (needle.length !== 20) return -1;
  return findOidIndexFromBytes(view, needle);
}

export function parseIdxView(
  packKey: string,
  idxBuf: Uint8Array,
  packSize: number
): IdxView | undefined {
  if (!Number.isSafeInteger(packSize) || packSize < 20) {
    throw new Error(`idx: unsupported pack size ${packSize}`);
  }
  if (idxBuf.byteLength < 8 + 256 * 4 + IDX_TRAILER_BYTES) return undefined;
  if (!(idxBuf[0] === 0xff && idxBuf[1] === 0x74 && idxBuf[2] === 0x4f && idxBuf[3] === 0x63)) {
    return undefined;
  }
  const dv = new DataView(idxBuf.buffer, idxBuf.byteOffset, idxBuf.byteLength);
  const version = dv.getUint32(4, false);
  if (version !== 2 && version !== 3) return undefined;

  const fanout = new Uint32Array(256);
  let pos = 8;
  for (let i = 0; i < 256; i++) {
    fanout[i] = dv.getUint32(pos, false);
    pos += 4;
  }

  const count = fanout[255] || 0;
  const namesStart = pos;
  const namesEnd = namesStart + count * 20;
  const crcsStart = namesEnd;
  const crcsEnd = crcsStart + count * 4;
  const offsetsStart = crcsEnd;
  const offsetsEnd = offsetsStart + count * 4;
  const largeOffsetsStart = offsetsEnd;
  const largeOffsetsLimit = idxBuf.byteLength - IDX_TRAILER_BYTES;
  if (namesEnd > idxBuf.byteLength || offsetsEnd > largeOffsetsLimit) return undefined;

  // Copy the name table into its own buffer so the cache does not keep the
  // full idx object alive just to service OID binary searches.
  const rawNames = idxBuf.slice(namesStart, namesEnd);
  const offsets = new Float64Array(count);
  for (let i = 0; i < count; i++) {
    const u32 = dv.getUint32(offsetsStart + i * 4, false);
    if (u32 & 0x80000000) {
      const li = u32 & 0x7fffffff;
      const largeOffsetPos = largeOffsetsStart + li * 8;
      if (largeOffsetPos + 8 > largeOffsetsLimit) return undefined;
      offsets[i] = readUint64AsNumber(dv, largeOffsetPos);
    } else {
      offsets[i] = u32 >>> 0;
    }
  }

  // Build sorted offset arrays for binary-search lookups (replaces Maps,
  // saving ~24 MB of overhead for 97k entries).
  const sortedOffsetIndices = new Uint32Array(count);
  for (let i = 0; i < count; i++) sortedOffsetIndices[i] = i;
  sortedOffsetIndices.sort((a, b) => offsets[a] - offsets[b]);

  const sortedOffsets = new Float64Array(count);
  const nextOffsetByIndex = new Float64Array(count);
  for (let i = 0; i < count; i++) {
    sortedOffsets[i] = offsets[sortedOffsetIndices[i]];
    const nextOffset = i + 1 < count ? offsets[sortedOffsetIndices[i + 1]] : packSize - 20;
    nextOffsetByIndex[sortedOffsetIndices[i]] = nextOffset;
  }

  return {
    packKey,
    count,
    fanout,
    rawNames,
    offsets,
    nextOffsetByIndex,
    sortedOffsets,
    sortedOffsetIndices,
    packSize,
    packChecksum: idxBuf.slice(idxBuf.byteLength - IDX_TRAILER_BYTES, idxBuf.byteLength - 20),
    idxChecksum: idxBuf.slice(idxBuf.byteLength - 20),
  };
}

export async function loadIdxView(
  env: Env,
  packKey: string,
  cacheCtx?: CacheContext,
  packSizeHint?: number
): Promise<IdxView | undefined> {
  if (cacheCtx && !cacheCtx.memo) {
    cacheCtx.memo = {};
  }
  const cached =
    packSizeHint === undefined
      ? undefined
      : idxViewCache.get(getIdxViewCacheKey(packKey, packSizeHint));
  const memoView = cacheCtx?.memo?.idxViews?.get(packKey);
  if (memoView) {
    if (packSizeHint === undefined || memoView.packSize === packSizeHint) {
      return memoView;
    }
    if (cached) {
      // When the request-local memo was populated from a stale size hint, prefer
      // the size-matched isolate cache entry instead of re-fetching the idx.
      touchIdxViewCache(packKey, cached.view.packSize, cached.view);
      if (cacheCtx?.memo) {
        cacheCtx.memo.idxViews = cacheCtx.memo.idxViews || new Map();
        cacheCtx.memo.idxViews.set(packKey, cached.view);
      }
      return cached.view;
    }
    cacheCtx?.memo?.idxViews?.delete(packKey);
  }
  const promiseKey =
    packSizeHint === undefined ? packKey : getIdxViewCacheKey(packKey, packSizeHint);
  if (cacheCtx?.memo?.idxViewPromises?.has(promiseKey)) {
    return await cacheCtx.memo.idxViewPromises.get(promiseKey);
  }

  if (cached) {
    touchIdxViewCache(packKey, cached.view.packSize, cached.view);
    if (cacheCtx?.memo) {
      cacheCtx.memo.idxViews = cacheCtx.memo.idxViews || new Map();
      cacheCtx.memo.idxViews.set(packKey, cached.view);
    }
    return cached.view;
  }

  const log = createLogger(env.LOG_LEVEL, { service: "PackedIdxView" });
  const limiter = getLimiter(cacheCtx);
  const inflight = (async () => {
    // Coalesce concurrent cold loads for the same pack inside one request so
    // membership probes do not multiply the idx/head R2 traffic.
    const idxObj = await limiter.run("r2:get-pack-idx", async () => {
      if (!countSubrequest(cacheCtx)) {
        logOnce(cacheCtx, "packed-idx-soft-budget-warned", () => {
          log.warn("soft-budget-exhausted", {
            op: "r2:get-pack-idx",
          });
        });
      }
      return await env.REPO_BUCKET.get(packIndexKey(packKey));
    });
    if (!idxObj) return undefined;

    let packSize = packSizeHint;
    if (packSize === undefined) {
      const packHead = await limiter.run("r2:head-pack", async () => {
        if (!countSubrequest(cacheCtx)) {
          logOnce(cacheCtx, "packed-head-soft-budget-warned", () => {
            log.warn("soft-budget-exhausted", {
              op: "r2:head-pack",
            });
          });
        }
        return await env.REPO_BUCKET.head(packKey);
      });
      if (!packHead) return undefined;
      packSize = packHead.size;
    }

    const view = parseIdxView(packKey, new Uint8Array(await idxObj.arrayBuffer()), packSize);
    if (!view) return undefined;

    // `nextOffsetByIndex` for the final entry depends on the pack size, so the
    // isolate cache is keyed by both pack key and pack size. That lets normal
    // hinted reads reuse idx views across requests without allowing a stale hint
    // to overwrite the correct entry for the same pack key.
    touchIdxViewCache(packKey, packSize, view);
    if (cacheCtx?.memo) {
      cacheCtx.memo.idxViews = cacheCtx.memo.idxViews || new Map();
      cacheCtx.memo.idxViews.set(packKey, view);
    }
    return view;
  })();

  if (cacheCtx?.memo) {
    cacheCtx.memo.idxViewPromises = cacheCtx.memo.idxViewPromises || new Map();
    cacheCtx.memo.idxViewPromises.set(promiseKey, inflight);
  }
  try {
    return await inflight;
  } finally {
    cacheCtx?.memo?.idxViewPromises?.delete(promiseKey);
  }
}
