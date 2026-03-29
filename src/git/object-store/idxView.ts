import type { CacheContext } from "@/cache/index.ts";
import type { IdxView } from "./types.ts";

import { bytesToHex, hexToBytes } from "@/common/index.ts";
import { packIndexKey } from "@/keys.ts";

const IDX_VIEW_CACHE_MAX = 32;
const idxViewCache = new Map<string, IdxView>();

function touchIdxViewCache(key: string, value: IdxView) {
  if (idxViewCache.has(key)) idxViewCache.delete(key);
  idxViewCache.set(key, value);
  if (idxViewCache.size > IDX_VIEW_CACHE_MAX) {
    const first = idxViewCache.keys().next().value;
    if (first) idxViewCache.delete(first);
  }
}

export function getOidHexAt(view: IdxView, index: number): string {
  const start = index * 20;
  return bytesToHex(view.rawNames.subarray(start, start + 20));
}

function compareOidAt(view: IdxView, index: number, needle: Uint8Array): number {
  const start = index * 20;
  const haystack = view.rawNames.subarray(start, start + 20);
  for (let i = 0; i < 20; i++) {
    const diff = haystack[i] - needle[i];
    if (diff !== 0) return diff;
  }
  return 0;
}

export function findOidIndex(view: IdxView, oid: string | Uint8Array): number {
  const needle = typeof oid === "string" ? hexToBytes(oid.toLowerCase()) : oid;
  if (needle.length !== 20) return -1;

  const first = needle[0];
  const lo = first === 0 ? 0 : view.fanout[first - 1] || 0;
  const hi = (view.fanout[first] || 0) - 1;
  if (hi < lo) return -1;

  let left = lo;
  let right = hi;
  while (left <= right) {
    const mid = (left + right) >> 1;
    const cmp = compareOidAt(view, mid, needle);
    if (cmp === 0) return mid;
    if (cmp < 0) left = mid + 1;
    else right = mid - 1;
  }
  return -1;
}

function parseIdxView(packKey: string, idxBuf: Uint8Array, packSize: number): IdxView | undefined {
  if (idxBuf.byteLength < 8 + 256 * 4) return undefined;
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
  const offsetsStart = namesEnd + count * 4;
  const largeOffsetsStart = offsetsStart + count * 4;
  // We keep the raw name table plus the exact offset arrays from the idx so lookups
  // can stay worker-local without rebuilding an OID map for every request.
  if (idxBuf.byteLength < offsetsStart + count * 4) return undefined;

  const rawNames = idxBuf.subarray(namesStart, namesEnd);
  const offsets = new Array<number>(count);
  for (let i = 0; i < count; i++) {
    const u32 = dv.getUint32(offsetsStart + i * 4, false);
    if (u32 & 0x80000000) {
      const li = u32 & 0x7fffffff;
      const hi = dv.getUint32(largeOffsetsStart + li * 8, false);
      const lo = dv.getUint32(largeOffsetsStart + li * 8 + 4, false);
      offsets[i] = Number((BigInt(hi) << 32n) | BigInt(lo));
    } else {
      offsets[i] = u32 >>> 0;
    }
  }

  const offsetToIndex = new Map<number, number>();
  for (let i = 0; i < offsets.length; i++) offsetToIndex.set(offsets[i], i);
  const sortedOffsets = offsets.slice().sort((a, b) => a - b);
  const nextOffset = new Map<number, number>();
  for (let i = 0; i < sortedOffsets.length; i++) {
    const cur = sortedOffsets[i];
    // The final pack entry ends at the 20-byte trailing pack checksum.
    nextOffset.set(cur, i + 1 < sortedOffsets.length ? sortedOffsets[i + 1] : packSize - 20);
  }

  return {
    packKey,
    count,
    fanout,
    rawNames,
    offsets,
    offsetToIndex,
    nextOffset,
    packSize,
  };
}

export async function loadIdxView(
  env: Env,
  packKey: string,
  cacheCtx?: CacheContext
): Promise<IdxView | undefined> {
  if (cacheCtx?.memo?.idxViews?.has(packKey)) return cacheCtx.memo.idxViews.get(packKey);

  const cached = idxViewCache.get(packKey);
  if (cached) {
    touchIdxViewCache(packKey, cached);
    if (cacheCtx?.memo) {
      cacheCtx.memo.idxViews = cacheCtx.memo.idxViews || new Map();
      cacheCtx.memo.idxViews.set(packKey, cached);
    }
    return cached;
  }

  const [idxObj, packHead] = await Promise.all([
    env.REPO_BUCKET.get(packIndexKey(packKey)),
    env.REPO_BUCKET.head(packKey),
  ]);
  if (!idxObj || !packHead) return undefined;

  const view = parseIdxView(packKey, new Uint8Array(await idxObj.arrayBuffer()), packHead.size);
  if (!view) return undefined;

  touchIdxViewCache(packKey, view);
  if (cacheCtx?.memo) {
    cacheCtx.memo.idxViews = cacheCtx.memo.idxViews || new Map();
    cacheCtx.memo.idxViews.set(packKey, view);
  }
  return view;
}
