import type { CacheContext } from "@/cache/index.ts";
import type { GitObjectType } from "@/git/core/index.ts";
import type { IdxView } from "@/git/object-store/types.ts";

import { bytesEqual, bytesToHex, createLogger } from "@/common/index.ts";
import { typeCodeToObjectType } from "@/git/object-store/support.ts";
import { countSubrequest, getLimiter } from "@/git/operations/limits.ts";
import { packRefsKey } from "@/keys.ts";
import { buildOidSortedEntryIndices } from "./indexer/writeIdx.ts";
import type { PackEntryTable } from "./indexer/types.ts";

const PACK_REF_MAGIC = 0x50524546; // "PREF"
const PACK_REF_VERSION = 1;
const PACK_REF_HEADER_BYTES = 4 + 4 + 4 + 8 + 20 + 20;
const OID_BYTES = 20;
const OID_HEX_BYTES = OID_BYTES * 2;
const PACK_REF_VIEW_CACHE_MAX_BYTES = 16 * 1024 * 1024;
const UINT32_SPAN = 0x1_0000_0000;
const TREE_LINE_PREFIX = [0x74, 0x72, 0x65, 0x65, 0x20]; // "tree "
const PARENT_LINE_PREFIX = [0x70, 0x61, 0x72, 0x65, 0x6e, 0x74, 0x20]; // "parent "
const OBJECT_LINE_PREFIX = [0x6f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x20]; // "object "
const TYPE_LINE_PREFIX = [0x74, 0x79, 0x70, 0x65, 0x20]; // "type "

type PackRefValidationKind = "corrupt" | "stale";

type PackRefInvalidResult = {
  type: "Invalid";
  kind: PackRefValidationKind;
  reason: string;
};

export type PackRefView = {
  packKey: string;
  objectCount: number;
  packBytes: number;
  packChecksum: Uint8Array;
  idxChecksum: Uint8Array;
  /** Logical object type codes in idx OID order. */
  typeCodes: Uint8Array;
  /** Big-endian uint32 starts into `rawRefs`, measured in 20-byte OID slots. */
  refStartsBytes: Uint8Array;
  /** View over `refStartsBytes`; avoids decoding a second typed-array copy. */
  refStartsView: DataView;
  /** Flat raw 20-byte referenced OIDs in idx OID order. */
  rawRefs: Uint8Array;
};

export type PackRefBuildResult = {
  bytes: Uint8Array;
  objectCount: number;
  refCount: number;
  refIndexBytes: number;
};

export type PackRefSnapshotEntry = {
  packKey: string;
  packBytes: number;
  idx: IdxView;
  refs: PackRefView;
};

export type PackRefSnapshotLoadResult =
  | {
      type: "Ready";
      packs: PackRefSnapshotEntry[];
    }
  | {
      type: "Missing";
      packs: Array<{
        packKey: string;
        packBytes: number;
        reason: "missing" | PackRefValidationKind;
        detail?: string;
      }>;
    };

export type PackRefViewLoadResult =
  | {
      type: "Ready";
      view: PackRefView;
    }
  | {
      type: "Missing";
      reason: "missing";
    }
  | PackRefInvalidResult;

type CachedPackRefView = {
  view: PackRefView;
  bytes: number;
};

const packRefViewCache = new Map<string, CachedPackRefView>();
let packRefViewCacheBytes = 0;

function getPackRefViewCacheKey(packKey: string, idxChecksum: Uint8Array): string {
  return `${packKey}\0${bytesToHex(idxChecksum)}`;
}

function estimatePackRefViewBytes(view: PackRefView): number {
  return (
    view.typeCodes.byteLength +
    view.refStartsBytes.byteLength +
    view.rawRefs.byteLength +
    view.packChecksum.byteLength +
    view.idxChecksum.byteLength
  );
}

function touchPackRefViewCache(cacheKey: string, view: PackRefView): void {
  const existing = packRefViewCache.get(cacheKey);
  if (existing) {
    packRefViewCache.delete(cacheKey);
    packRefViewCacheBytes -= existing.bytes;
  }

  const bytes = estimatePackRefViewBytes(view);
  if (bytes > PACK_REF_VIEW_CACHE_MAX_BYTES) return;

  packRefViewCache.set(cacheKey, { view, bytes });
  packRefViewCacheBytes += bytes;

  while (packRefViewCacheBytes > PACK_REF_VIEW_CACHE_MAX_BYTES) {
    const firstKey = packRefViewCache.keys().next().value;
    if (!firstKey) break;
    const first = packRefViewCache.get(firstKey);
    packRefViewCache.delete(firstKey);
    packRefViewCacheBytes -= first?.bytes ?? 0;
  }
}

function writeUint64(dv: DataView, pos: number, value: number): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`ref-index: unsupported 64-bit value ${value}`);
  }
  const hi = Math.floor(value / UINT32_SPAN);
  const lo = value >>> 0;
  dv.setUint32(pos, hi, false);
  dv.setUint32(pos + 4, lo, false);
}

function readUint64(dv: DataView, pos: number): number {
  const hi = dv.getUint32(pos, false);
  const lo = dv.getUint32(pos + 4, false);
  const value = hi * UINT32_SPAN + lo;
  if (!Number.isSafeInteger(value)) {
    throw new Error("ref-index: pack byte count exceeds safe integer support");
  }
  return value;
}

function invalid(kind: PackRefValidationKind, reason: string): PackRefInvalidResult {
  return { type: "Invalid", kind, reason };
}

function isValidObjectTypeCode(typeCode: number): boolean {
  return typeCodeToObjectType(typeCode) !== null;
}

// Sidecar generation runs while receive/backfill already holds inflated object
// payloads. Parse ASCII Git headers directly from bytes so large packs do not
// allocate one hex string per recorded edge before writing the flat sidecar.
function hexNibble(value: number): number {
  if (value >= 0x30 && value <= 0x39) return value - 0x30;
  if (value >= 0x61 && value <= 0x66) return value - 0x61 + 10;
  if (value >= 0x41 && value <= 0x46) return value - 0x41 + 10;
  return -1;
}

function hasHexOidAt(payload: Uint8Array, hexStart: number, lineEnd: number): boolean {
  if (hexStart + OID_HEX_BYTES > lineEnd) return false;
  for (let index = 0; index < OID_HEX_BYTES; index++) {
    if (hexNibble(payload[hexStart + index]) < 0) return false;
  }
  return true;
}

function writeHexOidAt(
  payload: Uint8Array,
  hexStart: number,
  out: Uint8Array,
  outStart: number
): void {
  for (let index = 0; index < OID_BYTES; index++) {
    const hi = hexNibble(payload[hexStart + index * 2]);
    const lo = hexNibble(payload[hexStart + index * 2 + 1]);
    if (hi < 0 || lo < 0) {
      throw new Error("ref-index: invalid hex object id");
    }
    out[outStart + index] = (hi << 4) | lo;
  }
}

function lineHasPrefix(
  payload: Uint8Array,
  lineStart: number,
  lineEnd: number,
  prefix: readonly number[]
): boolean {
  if (lineStart + prefix.length > lineEnd) return false;
  for (let index = 0; index < prefix.length; index++) {
    if (payload[lineStart + index] !== prefix[index]) return false;
  }
  return true;
}

function valueStartsWithAscii(
  payload: Uint8Array,
  start: number,
  lineEnd: number,
  value: string
): boolean {
  if (start + value.length > lineEnd) return false;
  for (let index = 0; index < value.length; index++) {
    if (payload[start + index] !== value.charCodeAt(index)) return false;
  }
  return true;
}

function forEachHeaderLine(
  payload: Uint8Array,
  visit: (lineStart: number, lineEnd: number) => void
): void {
  let cursor = 0;
  while (cursor < payload.byteLength) {
    const lineStart = cursor;
    while (cursor < payload.byteLength && payload[cursor] !== 0x0a) cursor++;
    const lineEnd = cursor > lineStart && payload[cursor - 1] === 0x0d ? cursor - 1 : cursor;
    if (lineEnd === lineStart) break;
    visit(lineStart, lineEnd);
    cursor++;
  }
}

function parseCommitRefBytes(payload: Uint8Array): Uint8Array {
  let treeHexStart = -1;
  let parentCount = 0;

  forEachHeaderLine(payload, (lineStart, lineEnd) => {
    if (treeHexStart < 0 && lineHasPrefix(payload, lineStart, lineEnd, TREE_LINE_PREFIX)) {
      const hexStart = lineStart + TREE_LINE_PREFIX.length;
      if (hasHexOidAt(payload, hexStart, lineEnd)) treeHexStart = hexStart;
      return;
    }

    if (lineHasPrefix(payload, lineStart, lineEnd, PARENT_LINE_PREFIX)) {
      const hexStart = lineStart + PARENT_LINE_PREFIX.length;
      if (hasHexOidAt(payload, hexStart, lineEnd)) parentCount++;
    }
  });

  const treeCount = treeHexStart >= 0 ? 1 : 0;
  const refs = new Uint8Array((treeCount + parentCount) * OID_BYTES);
  let refsOffset = 0;
  if (treeHexStart >= 0) {
    writeHexOidAt(payload, treeHexStart, refs, refsOffset);
    refsOffset += OID_BYTES;
  }

  forEachHeaderLine(payload, (lineStart, lineEnd) => {
    if (!lineHasPrefix(payload, lineStart, lineEnd, PARENT_LINE_PREFIX)) return;
    const hexStart = lineStart + PARENT_LINE_PREFIX.length;
    if (!hasHexOidAt(payload, hexStart, lineEnd)) return;
    writeHexOidAt(payload, hexStart, refs, refsOffset);
    refsOffset += OID_BYTES;
  });

  return refs;
}

function isValidTagTypeLine(payload: Uint8Array, lineStart: number, lineEnd: number): boolean {
  if (!lineHasPrefix(payload, lineStart, lineEnd, TYPE_LINE_PREFIX)) return false;
  const valueStart = lineStart + TYPE_LINE_PREFIX.length;
  return (
    valueStartsWithAscii(payload, valueStart, lineEnd, "commit") ||
    valueStartsWithAscii(payload, valueStart, lineEnd, "tree") ||
    valueStartsWithAscii(payload, valueStart, lineEnd, "blob") ||
    valueStartsWithAscii(payload, valueStart, lineEnd, "tag")
  );
}

function parseTagRefBytes(payload: Uint8Array): Uint8Array {
  let objectHexStart = -1;
  let hasTargetType = false;

  forEachHeaderLine(payload, (lineStart, lineEnd) => {
    if (objectHexStart < 0 && lineHasPrefix(payload, lineStart, lineEnd, OBJECT_LINE_PREFIX)) {
      const hexStart = lineStart + OBJECT_LINE_PREFIX.length;
      if (hasHexOidAt(payload, hexStart, lineEnd)) objectHexStart = hexStart;
      return;
    }

    if (isValidTagTypeLine(payload, lineStart, lineEnd)) {
      hasTargetType = true;
    }
  });

  if (objectHexStart < 0 || !hasTargetType) return new Uint8Array(0);
  const refs = new Uint8Array(OID_BYTES);
  writeHexOidAt(payload, objectHexStart, refs, 0);
  return refs;
}

function visitTreeClosureRawRefs(
  payload: Uint8Array,
  visit: (rawRefs: Uint8Array, oidStart: number) => void
): void {
  let cursor = 0;

  while (cursor < payload.length) {
    const modeStart = cursor;
    while (cursor < payload.length && payload[cursor] !== 0x20) cursor++;
    if (cursor >= payload.length) break;

    const modeEnd = cursor;
    cursor++;
    while (cursor < payload.length && payload[cursor] !== 0x00) cursor++;

    const oidStart = cursor + 1;
    if (oidStart + OID_BYTES > payload.length) break;

    if (!isGitlinkMode(payload, modeStart, modeEnd)) {
      visit(payload, oidStart);
    }
    cursor = oidStart + OID_BYTES;
  }
}

function parseTreeClosureRefBytes(payload: Uint8Array): Uint8Array {
  let refCount = 0;
  visitTreeClosureRawRefs(payload, () => {
    refCount++;
  });

  const refs = new Uint8Array(refCount * OID_BYTES);
  let refsOffset = 0;
  visitTreeClosureRawRefs(payload, (rawRefs, oidStart) => {
    refs.set(rawRefs.subarray(oidStart, oidStart + OID_BYTES), refsOffset);
    refsOffset += OID_BYTES;
  });
  return refs;
}

function objectRefBytes(type: GitObjectType, payload: Uint8Array): Uint8Array {
  if (type === "commit") {
    return parseCommitRefBytes(payload);
  }

  if (type === "tree") {
    return parseTreeClosureRefBytes(payload);
  }

  if (type === "tag") {
    return parseTagRefBytes(payload);
  }

  return new Uint8Array(0);
}

/**
 * Parse tree child references used by fetch closure planning. Gitlinks are
 * mode 160000 entries and point at commits in another repository, so they must
 * not become required objects in the superproject's pack closure.
 */
export function parseTreeClosureRefs(payload: Uint8Array): string[] {
  const rawRefs = parseTreeClosureRefBytes(payload);
  const out: string[] = [];
  for (let offset = 0; offset < rawRefs.byteLength; offset += OID_BYTES) {
    out.push(bytesToHex(rawRefs.subarray(offset, offset + OID_BYTES)));
  }
  return out;
}

function isGitlinkMode(payload: Uint8Array, modeStart: number, modeEnd: number): boolean {
  const gitlinkMode = "160000";
  if (modeEnd - modeStart !== gitlinkMode.length) return false;
  for (let index = 0; index < gitlinkMode.length; index++) {
    if (payload[modeStart + index] !== gitlinkMode.charCodeAt(index)) return false;
  }
  return true;
}

export class PackRefsBuilder {
  private readonly rawRefsByEntry: Array<Uint8Array | undefined>;

  constructor(objectCount: number) {
    this.rawRefsByEntry = new Array<Uint8Array | undefined>(objectCount);
  }

  recordObject(index: number, type: GitObjectType, payload: Uint8Array): void {
    this.rawRefsByEntry[index] = objectRefBytes(type, payload);
  }

  recordBlob(index: number): void {
    this.rawRefsByEntry[index] = new Uint8Array(0);
  }

  build(args: {
    table: PackEntryTable;
    objectCount: number;
    packBytes: number;
    packChecksum: Uint8Array;
    idxChecksum: Uint8Array;
  }): PackRefBuildResult {
    const sortedIndices = buildOidSortedEntryIndices(args.table, args.objectCount);
    const refStarts = new Uint32Array(args.objectCount + 1);
    let refCount = 0;

    for (let sortedIndex = 0; sortedIndex < sortedIndices.length; sortedIndex++) {
      const entryIndex = sortedIndices[sortedIndex];
      const typeCode = args.table.objectTypes[entryIndex];
      if (!isValidObjectTypeCode(typeCode)) {
        throw new Error(`ref-index: unresolved object type for entry ${entryIndex}`);
      }
      const refs = this.rawRefsByEntry[entryIndex];
      if (refs === undefined) {
        throw new Error(`ref-index: missing logical refs for entry ${entryIndex}`);
      }
      if (refs.byteLength % OID_BYTES !== 0) {
        throw new Error(`ref-index: invalid logical refs for entry ${entryIndex}`);
      }

      refStarts[sortedIndex] = refCount;
      refCount += refs.byteLength / OID_BYTES;
    }
    refStarts[args.objectCount] = refCount;

    const totalBytes =
      PACK_REF_HEADER_BYTES + args.objectCount + (args.objectCount + 1) * 4 + refCount * OID_BYTES;
    const out = new Uint8Array(totalBytes);
    const dv = new DataView(out.buffer, out.byteOffset, out.byteLength);
    let pos = 0;

    dv.setUint32(pos, PACK_REF_MAGIC, false);
    pos += 4;
    dv.setUint32(pos, PACK_REF_VERSION, false);
    pos += 4;
    dv.setUint32(pos, args.objectCount, false);
    pos += 4;
    writeUint64(dv, pos, args.packBytes);
    pos += 8;
    out.set(args.packChecksum, pos);
    pos += OID_BYTES;
    out.set(args.idxChecksum, pos);
    pos += OID_BYTES;

    for (let sortedIndex = 0; sortedIndex < sortedIndices.length; sortedIndex++) {
      out[pos++] = args.table.objectTypes[sortedIndices[sortedIndex]];
    }

    for (let index = 0; index < refStarts.length; index++) {
      dv.setUint32(pos, refStarts[index], false);
      pos += 4;
    }

    for (let sortedIndex = 0; sortedIndex < sortedIndices.length; sortedIndex++) {
      const entryIndex = sortedIndices[sortedIndex];
      const refs = this.rawRefsByEntry[entryIndex];
      if (refs === undefined) {
        throw new Error(`ref-index: missing logical refs for entry ${entryIndex}`);
      }
      out.set(refs, pos);
      pos += refs.byteLength;
    }

    return {
      bytes: out,
      objectCount: args.objectCount,
      refCount,
      refIndexBytes: out.byteLength,
    };
  }
}

export function parsePackRefView(
  packKey: string,
  bytes: Uint8Array,
  idx: IdxView
): PackRefViewLoadResult {
  if (bytes.byteLength < PACK_REF_HEADER_BYTES) {
    return invalid("corrupt", "truncated-header");
  }

  const dv = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  let pos = 0;
  if (dv.getUint32(pos, false) !== PACK_REF_MAGIC) return invalid("corrupt", "bad-magic");
  pos += 4;
  if (dv.getUint32(pos, false) !== PACK_REF_VERSION) {
    return invalid("corrupt", "bad-version");
  }
  pos += 4;

  const objectCount = dv.getUint32(pos, false);
  pos += 4;
  if (objectCount !== idx.count) return invalid("stale", "object-count-mismatch");

  let packBytes: number;
  try {
    packBytes = readUint64(dv, pos);
  } catch {
    return invalid("corrupt", "pack-bytes-overflow");
  }
  pos += 8;
  if (packBytes !== idx.packSize) return invalid("stale", "pack-bytes-mismatch");

  const packChecksum = bytes.subarray(pos, pos + OID_BYTES);
  pos += OID_BYTES;
  if (!bytesEqual(packChecksum, idx.packChecksum)) {
    return invalid("stale", "pack-checksum-mismatch");
  }

  const idxChecksum = bytes.subarray(pos, pos + OID_BYTES);
  pos += OID_BYTES;
  if (!bytesEqual(idxChecksum, idx.idxChecksum)) {
    return invalid("stale", "idx-checksum-mismatch");
  }

  const typeCodesEnd = pos + objectCount;
  if (typeCodesEnd > bytes.byteLength) return invalid("corrupt", "truncated-type-codes");
  const typeCodes = bytes.subarray(pos, typeCodesEnd);
  pos = typeCodesEnd;
  for (let index = 0; index < typeCodes.length; index++) {
    if (!isValidObjectTypeCode(typeCodes[index])) {
      return invalid("corrupt", "invalid-type-code");
    }
  }

  const startsBytes = (objectCount + 1) * 4;
  if (pos + startsBytes > bytes.byteLength) return invalid("corrupt", "truncated-ref-starts");
  const refStartsBytes = bytes.subarray(pos, pos + startsBytes);
  const refStartsView = new DataView(
    refStartsBytes.buffer,
    refStartsBytes.byteOffset,
    refStartsBytes.byteLength
  );
  let previous = 0;
  for (let index = 0; index < objectCount + 1; index++) {
    const value = dv.getUint32(pos + index * 4, false);
    if (index > 0 && value < previous) {
      return invalid("corrupt", "non-monotonic-ref-starts");
    }
    previous = value;
  }
  pos += startsBytes;

  const rawRefs = bytes.subarray(pos);
  const finalRefOffset = refStartsView.getUint32(objectCount * 4, false);
  if (finalRefOffset * OID_BYTES !== rawRefs.byteLength) {
    return invalid("corrupt", "invalid-final-ref-offset");
  }

  return {
    type: "Ready",
    view: {
      packKey,
      objectCount,
      packBytes,
      packChecksum,
      idxChecksum,
      typeCodes,
      refStartsBytes,
      refStartsView,
      rawRefs,
    },
  };
}

export function getPackRefTypeCode(view: PackRefView, oidIndex: number): number | undefined {
  if (oidIndex < 0 || oidIndex >= view.objectCount) return undefined;
  return view.typeCodes[oidIndex];
}

export function getPackRefObjectType(
  view: PackRefView,
  oidIndex: number
): GitObjectType | undefined {
  const typeCode = getPackRefTypeCode(view, oidIndex);
  if (typeCode === undefined) return undefined;
  return typeCodeToObjectType(typeCode) ?? undefined;
}

function getPackRefStart(view: PackRefView, oidIndex: number): number {
  return view.refStartsView.getUint32(oidIndex * 4, false);
}

export function getPackRefRawRefAt(
  view: PackRefView,
  oidIndex: number,
  refOffset: number
): Uint8Array | undefined {
  if (oidIndex < 0 || oidIndex >= view.objectCount || refOffset < 0) return undefined;
  const refStart = getPackRefStart(view, oidIndex);
  const refEnd = getPackRefStart(view, oidIndex + 1);
  const refIndex = refStart + refOffset;
  if (refIndex >= refEnd) return undefined;
  const byteStart = refIndex * OID_BYTES;
  return view.rawRefs.subarray(byteStart, byteStart + OID_BYTES);
}

export function visitPackRefRawRefsAt(
  view: PackRefView,
  oidIndex: number,
  visit: (rawRefs: Uint8Array, oidStart: number) => void
): void {
  if (oidIndex < 0 || oidIndex >= view.objectCount) return;
  const refStart = getPackRefStart(view, oidIndex);
  const refEnd = getPackRefStart(view, oidIndex + 1);

  for (let refIndex = refStart; refIndex < refEnd; refIndex++) {
    visit(view.rawRefs, refIndex * OID_BYTES);
  }
}

export function getPackRefRefsAt(view: PackRefView, oidIndex: number): string[] {
  if (oidIndex < 0 || oidIndex >= view.objectCount) return [];
  const refs: string[] = [];

  visitPackRefRawRefsAt(view, oidIndex, (rawRefs, byteStart) => {
    refs.push(bytesToHex(rawRefs.subarray(byteStart, byteStart + OID_BYTES)));
  });

  return refs;
}

export async function loadPackRefView(
  env: Env,
  packKey: string,
  idx: IdxView,
  cacheCtx?: CacheContext
): Promise<PackRefViewLoadResult> {
  if (cacheCtx && !cacheCtx.memo) {
    cacheCtx.memo = {};
  }

  const cacheKey = getPackRefViewCacheKey(packKey, idx.idxChecksum);
  const memoView = cacheCtx?.memo?.packRefViews?.get(cacheKey);
  if (memoView) return { type: "Ready", view: memoView };

  const cached = packRefViewCache.get(cacheKey);
  if (cached) {
    touchPackRefViewCache(cacheKey, cached.view);
    if (cacheCtx?.memo) {
      cacheCtx.memo.packRefViews = cacheCtx.memo.packRefViews || new Map();
      cacheCtx.memo.packRefViews.set(cacheKey, cached.view);
    }
    return { type: "Ready", view: cached.view };
  }

  const inflight = cacheCtx?.memo?.packRefViewPromises?.get(cacheKey);
  if (inflight) return await inflight;

  const log = createLogger(env.LOG_LEVEL, { service: "PackRefIndex" });
  const limiter = getLimiter(cacheCtx);
  const promise = (async (): Promise<PackRefViewLoadResult> => {
    const obj = await limiter.run("r2:get-pack-refs", async () => {
      if (!countSubrequest(cacheCtx)) {
        log.warn("soft-budget-exhausted", { op: "r2:get-pack-refs" });
      }
      return await env.REPO_BUCKET.get(packRefsKey(packKey));
    });
    if (!obj) return { type: "Missing", reason: "missing" };

    const parsed = parsePackRefView(packKey, new Uint8Array(await obj.arrayBuffer()), idx);
    if (parsed.type !== "Ready") return parsed;

    touchPackRefViewCache(cacheKey, parsed.view);
    if (cacheCtx?.memo) {
      cacheCtx.memo.packRefViews = cacheCtx.memo.packRefViews || new Map();
      cacheCtx.memo.packRefViews.set(cacheKey, parsed.view);
    }
    return parsed;
  })();

  if (cacheCtx?.memo) {
    cacheCtx.memo.packRefViewPromises = cacheCtx.memo.packRefViewPromises || new Map();
    cacheCtx.memo.packRefViewPromises.set(cacheKey, promise);
  }

  try {
    return await promise;
  } finally {
    cacheCtx?.memo?.packRefViewPromises?.delete(cacheKey);
  }
}
