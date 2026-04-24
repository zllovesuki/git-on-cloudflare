/**
 * Types for the streaming pack indexer.
 *
 * The entry table uses typed arrays to keep per-object metadata compact.
 * OIDs are stored as raw 20-byte values in a flat Uint8Array; hex strings
 * are materialized lazily when needed.
 */

import type { CacheContext } from "@/cache/index.ts";
import type { Logger } from "@/common/logger.ts";
import type { IdxView, PackCatalogRow } from "@/git/object-store/types.ts";
import type { Limiter } from "@/git/operations/limits.ts";
import type { PackRefsBuilder } from "@/git/pack/refIndex.ts";

// ---------------------------------------------------------------------------
// Entry table – struct-of-arrays backed by typed arrays
// ---------------------------------------------------------------------------

export interface PackEntryTable {
  count: number;

  /** Byte offset in pack where each entry starts. */
  offsets: Uint32Array;

  /** Pack type code: 1=commit, 2=tree, 3=blob, 4=tag, 6=OFS_DELTA, 7=REF_DELTA. */
  types: Uint8Array;

  /**
   * Final logical object type code: 1=commit, 2=tree, 3=blob, 4=tag.
   * Delta entries retain their pack type in `types`; this array records the
   * resolved object type needed by closure sidecars and rematerialization.
   */
  objectTypes: Uint8Array;

  /** Header length in bytes (type varint + optional delta metadata). */
  headerLens: Uint16Array;

  /** Byte offset where the raw entry data (header + compressed) ends. */
  spanEnds: Uint32Array;

  /** CRC-32 of the raw entry bytes (header + compressed payload). */
  crc32s: Uint32Array;

  /** Raw 20-byte OIDs in a flat buffer (count * 20 bytes). Zeroed for unresolved deltas. */
  oids: Uint8Array;

  /**
   * Decompressed payload size for non-delta objects.
   * For delta objects this holds the delta *result* size (from the delta header).
   */
  decompressedSizes: Uint32Array;

  /** For OFS_DELTA: absolute byte offset of the base entry; 0 otherwise. */
  ofsBaseOffsets: Uint32Array;

  /** 1 when the OID has been computed, 0 when the entry is still pending resolution. */
  resolved: Uint8Array;
}

/**
 * Raw 20-byte REF_DELTA base OIDs stored in entry order.
 *
 * Only entries whose type is REF_DELTA read meaningful bytes from this buffer.
 * The flat layout avoids a sparse Map allocation per delta entry.
 */
export type RefBaseOids = Uint8Array;

// ---------------------------------------------------------------------------
// Allocator
// ---------------------------------------------------------------------------

export function allocateEntryTable(count: number): PackEntryTable {
  return {
    count,
    offsets: new Uint32Array(count),
    types: new Uint8Array(count),
    objectTypes: new Uint8Array(count),
    headerLens: new Uint16Array(count),
    spanEnds: new Uint32Array(count),
    crc32s: new Uint32Array(count),
    oids: new Uint8Array(count * 20),
    decompressedSizes: new Uint32Array(count),
    ofsBaseOffsets: new Uint32Array(count),
    resolved: new Uint8Array(count),
  };
}

// ---------------------------------------------------------------------------
// Result types
// ---------------------------------------------------------------------------

export interface ScanResult {
  table: PackEntryTable;
  refBaseOids: RefBaseOids;
  refDeltaCount: number;
  resolvedCount: number;
  objectCount: number;
  /** Trailing 20-byte SHA-1 from the pack file. */
  packChecksum: Uint8Array;
  /** Per-entry logical object reference builder populated as payloads resolve. */
  refsBuilder?: PackRefsBuilder;
}

// ---------------------------------------------------------------------------
// Binary search helpers for typed-array lookups (replaces Map<number, number>)
// ---------------------------------------------------------------------------

/**
 * Binary search for a pack byte offset in a sorted Uint32Array.
 * Returns the matching slot index within that sorted array, or -1 if not found.
 */
export function searchOffsetIndex(sortedOffsets: Uint32Array, target: number): number {
  let lo = 0;
  let hi = sortedOffsets.length - 1;
  while (lo <= hi) {
    const mid = (lo + hi) >>> 1;
    const v = sortedOffsets[mid];
    if (v === target) return mid;
    if (v < target) lo = mid + 1;
    else hi = mid - 1;
  }
  return -1;
}

export interface ResolveResult {
  objectCount: number;
  idxBytes: number;
  refIndexBytes: number;
  idxView: IdxView;
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

export interface IndexerOptions {
  env: Env;
  /** Full R2 key of the .pack file. */
  packKey: string;
  /** Total byte size of the .pack file in R2. */
  packSize: number;
  /** Bytes per sequential R2 range read (default 1 MiB). */
  chunkSize?: number;
  limiter: Limiter;
  countSubrequest: (n?: number) => void;
  log: Logger;
  signal?: AbortSignal;
  onProgress?: (message: string) => void;
}

export interface ResolveOptions extends IndexerOptions {
  scanResult: ScanResult;
  /** Active pack catalog snapshot for external (thin-pack) base resolution. */
  activeCatalog?: PackCatalogRow[];
  cacheCtx?: CacheContext;
  /** Repository identifier used by the object store. */
  repoId: string;
  /** Hard byte budget for the base-payload LRU cache (default 32 MiB). */
  lruBudget?: number;
  /**
   * Backfill can rebuild the derived `.refs` artifact from an already-active
   * pack without rewriting the deterministic `.idx` beside it.
   */
  writeIdx?: boolean;
  /** Existing idx view required when `writeIdx` is false. */
  existingIdxView?: IdxView;
}

export interface ConnectivityCheckOptions {
  env: Env;
  repoId: string;
  /** Full R2 key of the newly indexed .pack. */
  newPackKey: string;
  /** Pre-built IdxView for the new pack (avoids an extra R2 read). */
  newIdxView: IdxView;
  /** Byte size of the new .pack in R2. */
  newPackSize: number;
  /** Active pack catalog snapshot (existing packs). */
  activeCatalog: PackCatalogRow[];
  commands: { oldOid: string; newOid: string; ref: string }[];
  statuses: { ref: string; ok: boolean; msg?: string }[];
  log: Logger;
  cacheCtx: CacheContext;
}

export function getRefBaseOidAt(refBaseOids: RefBaseOids, index: number): Uint8Array {
  const start = index * 20;
  return refBaseOids.subarray(start, start + 20);
}
