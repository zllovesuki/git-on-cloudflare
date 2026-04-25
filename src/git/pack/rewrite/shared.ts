import type {
  OrderedPackSnapshot,
  OrderedPackSnapshotEntry,
} from "@/git/operations/fetch/types.ts";
import type { Logger } from "@/common/logger.ts";
import type { Limiter } from "@/git/operations/limits.ts";
import type { IdxView } from "@/git/object-store/types.ts";
import type { PackHeaderEx } from "../packMeta.ts";

import { createLogger } from "@/common/index.ts";
import { findFirstPackedObjectCandidate, getNextOffsetByIndex } from "@/git/object-store/index.ts";
import { SequentialReader } from "@/git/pack/indexer/resolve/reader.ts";
import { readPackHeaderExFromBuf, readPackRange } from "../packMeta.ts";

export const HEADER_READ_BYTES = 128;
export const DEFAULT_CHUNK_SIZE = 4_194_304;
export const WHOLE_PACK_MAX_BYTES = 8 * 1024 * 1024;
export const WHOLE_PACK_TOTAL_BUDGET = 32 * 1024 * 1024;
export const HEADER_STABILITY_CAP = 16;

export type RewriteFailure = {
  reason: string;
  retryable: boolean;
  details?: Record<string, unknown>;
};

export type RewriteFailureRecorder = {
  value?: RewriteFailure;
};

export type RewriteOptions = {
  signal?: AbortSignal;
  limiter?: Limiter;
  countSubrequest?: (n?: number) => boolean | void;
  onProgress?: (msg: string) => void;
  failure?: RewriteFailureRecorder;
};

/**
 * Flat typed-array representation of selected pack entries, modeled after
 * the `PackEntryTable` pattern in `src/git/pack/indexer/types.ts`.
 *
 * Every field is indexed by a selection slot (`sel`). The table grows on
 * demand when delta-base chasing discovers bases beyond the initial
 * `neededOids` set.
 */
export interface SelectionTable {
  count: number;
  capacity: number;

  /* identity — set during resolve */
  packSlots: Uint8Array;
  entryIndices: Uint32Array;
  offsets: Float64Array;
  nextOffsets: Float64Array;
  oidsRaw: Uint8Array; // capacity * 20, selected object OIDs

  /* header data — set during header-read pass */
  typeCodes: Uint8Array;
  headerLens: Uint16Array;
  payloadLens: Float64Array;
  sizeVarBuf: Uint8Array; // capacity * 5, concatenated varint bytes
  sizeVarLens: Uint8Array; // 1–5 per entry

  /* delta relationships */
  baseSlots: Int32Array; // sel of base, -1 = non-delta
  baseOidRaw: Uint8Array | null; // lazy; capacity * 20, for REF_DELTA
  queuedForHeader: Uint8Array; // 1 = already queued for header read
  ofsPinned: Uint8Array; // 1 = exact pack position is required by an OFS_DELTA child
  syntheticPayloads: Array<Uint8Array | undefined>; // compressed full-object payloads by sel

  /* output layout — set during convergence / topology sort */
  outputOffsets: Float64Array;
  outputHeaderLens: Uint16Array;
  outputOrder: Uint32Array; // topology-sorted selection indices
}

export function allocateSelectionTable(capacity: number): SelectionTable {
  return {
    count: 0,
    capacity,
    packSlots: new Uint8Array(capacity),
    entryIndices: new Uint32Array(capacity),
    offsets: new Float64Array(capacity),
    nextOffsets: new Float64Array(capacity),
    oidsRaw: new Uint8Array(capacity * 20),
    typeCodes: new Uint8Array(capacity),
    headerLens: new Uint16Array(capacity),
    payloadLens: new Float64Array(capacity),
    sizeVarBuf: new Uint8Array(capacity * 5),
    sizeVarLens: new Uint8Array(capacity),
    baseSlots: new Int32Array(capacity).fill(-1),
    baseOidRaw: null, // allocated lazily on first REF_DELTA
    queuedForHeader: new Uint8Array(capacity),
    ofsPinned: new Uint8Array(capacity),
    syntheticPayloads: new Array<Uint8Array | undefined>(capacity),
    outputOffsets: new Float64Array(capacity),
    outputHeaderLens: new Uint16Array(capacity),
    outputOrder: new Uint32Array(capacity),
  };
}

/** Double the table capacity, preserving existing data. */
export function growSelectionTable(table: SelectionTable): void {
  const next = Math.max(table.capacity * 2, 64);

  function grow<T extends ArrayLike<number> & { set(src: T): void }>(
    old: T,
    ctor: new (len: number) => T,
    len: number
  ): T {
    const arr = new ctor(len);
    arr.set(old);
    return arr;
  }

  table.packSlots = grow(table.packSlots, Uint8Array, next);
  table.entryIndices = grow(table.entryIndices, Uint32Array, next);
  table.offsets = grow(table.offsets, Float64Array, next);
  table.nextOffsets = grow(table.nextOffsets, Float64Array, next);
  table.typeCodes = grow(table.typeCodes, Uint8Array, next);
  table.headerLens = grow(table.headerLens, Uint16Array, next);
  table.payloadLens = grow(table.payloadLens, Float64Array, next);
  table.sizeVarLens = grow(table.sizeVarLens, Uint8Array, next);
  table.outputOffsets = grow(table.outputOffsets, Float64Array, next);
  table.outputHeaderLens = grow(table.outputHeaderLens, Uint16Array, next);
  table.outputOrder = grow(table.outputOrder, Uint32Array, next);

  table.queuedForHeader = grow(table.queuedForHeader, Uint8Array, next);
  table.ofsPinned = grow(table.ofsPinned, Uint8Array, next);
  table.syntheticPayloads.length = next;

  // sizeVarBuf is capacity * 5
  const oldSvBuf = table.sizeVarBuf;
  table.sizeVarBuf = new Uint8Array(next * 5);
  table.sizeVarBuf.set(oldSvBuf);

  const oldOidsRaw = table.oidsRaw;
  table.oidsRaw = new Uint8Array(next * 20);
  table.oidsRaw.set(oldOidsRaw);

  // baseSlots: new slots default to -1
  const oldBaseSlots = table.baseSlots;
  table.baseSlots = new Int32Array(next).fill(-1);
  table.baseSlots.set(oldBaseSlots);

  // baseOidRaw: grow only if already allocated
  if (table.baseOidRaw) {
    const oldRaw = table.baseOidRaw;
    table.baseOidRaw = new Uint8Array(next * 20);
    table.baseOidRaw.set(oldRaw);
  }

  table.capacity = next;
}

/** Pack (packSlot, entryIndex) into a single number for Map<number, number>. */
export function selectionKey(packSlot: number, entryIndex: number): number {
  return packSlot * 0x1_0000_0000 + entryIndex;
}

/**
 * Copy the pack-position identity for a row.
 *
 * This helper keeps the per-row identity fields together so add/replace flows
 * cannot forget to carry the raw OID bytes along with the new pack position.
 */
export function setSelectionEntryIdentity(
  table: SelectionTable,
  sel: number,
  packSlot: number,
  entryIndex: number,
  idx: IdxView
): void {
  table.packSlots[sel] = packSlot;
  table.entryIndices[sel] = entryIndex;
  table.offsets[sel] = idx.offsets[entryIndex];

  const nextOffset = getNextOffsetByIndex(idx, entryIndex);
  if (nextOffset === undefined) {
    throw new Error(`rewrite: missing next offset for pack#${packSlot} entry#${entryIndex}`);
  }
  table.nextOffsets[sel] = nextOffset;
  table.oidsRaw.set(idx.rawNames.subarray(entryIndex * 20, entryIndex * 20 + 20), sel * 20);
  table.syntheticPayloads[sel] = undefined;
}

/** Store the parsed pack header into the selection row. */
export function storeSelectionHeader(
  table: SelectionTable,
  sel: number,
  offset: number,
  nextOffset: number,
  header: PackHeaderEx
): boolean {
  table.typeCodes[sel] = header.type;
  table.headerLens[sel] = header.headerLen;

  const payloadLength = nextOffset - offset - header.headerLen;
  if (payloadLength < 0) {
    return false;
  }
  table.payloadLens[sel] = payloadLength;

  const svStart = sel * 5;
  table.sizeVarBuf.set(header.sizeVarBytes, svStart);
  table.sizeVarLens[sel] = header.sizeVarBytes.length;
  return true;
}

/**
 * Compare two selection slots by source-pack traversal order.
 *
 * Rewrite header reads and payload streaming both favor `(packSlot, offset)`
 * ordering so the `SequentialReader` stays on a mostly forward path.
 */
export function compareSelectionSlots(
  table: SelectionTable,
  leftSel: number,
  rightSel: number
): number {
  const packDiff = table.packSlots[leftSel] - table.packSlots[rightSel];
  if (packDiff !== 0) return packDiff;
  return table.offsets[leftSel] - table.offsets[rightSel];
}

/** Sort selection slots in-place using source-pack traversal order. */
export function sortSelectionSlots(
  table: SelectionTable,
  slots: Uint32Array | number[]
): Uint32Array | number[] {
  slots.sort((leftSel, rightSel) => compareSelectionSlots(table, leftSel, rightSel));
  return slots;
}

/**
 * Selection dependencies are a single base chain per row, so checking whether
 * `startSel` depends on `targetSel` is a bounded linked-list walk. The helper
 * intentionally follows only `baseSlots`; callers use it in hot paths before
 * any output graph has been allocated.
 */
export function selectionDependsOn(
  table: SelectionTable,
  startSel: number,
  targetSel: number
): boolean {
  let cur = startSel;
  for (let depth = 0; depth < table.count; depth++) {
    const baseSel = table.baseSlots[cur];
    if (baseSel < 0) return false;
    if (baseSel === targetSel) return true;
    cur = baseSel;
  }
  return false;
}

type CopySelectionRowOptions = {
  preserveTargetOfsPinned?: boolean;
};

/**
 * Copy all planner-phase row fields from one selection slot to another.
 *
 * This intentionally excludes layout outputs (`outputOffsets`,
 * `outputHeaderLens`, `outputOrder`) because every row rewrite happens before
 * topology and output sizing run.
 *
 * `baseSlots` and `baseOidRaw` move with the row because they are part of the
 * row's resolved delta wiring, not derived output state. Callers that need
 * extra semantics such as queue clearing or OFS pin merging still apply those
 * adjustments explicitly after the copy.
 */
export function copySelectionRow(
  table: SelectionTable,
  targetSel: number,
  sourceSel: number,
  options?: CopySelectionRowOptions
): void {
  const targetPinned = table.ofsPinned[targetSel];

  table.packSlots[targetSel] = table.packSlots[sourceSel];
  table.entryIndices[targetSel] = table.entryIndices[sourceSel];
  table.offsets[targetSel] = table.offsets[sourceSel];
  table.nextOffsets[targetSel] = table.nextOffsets[sourceSel];
  table.oidsRaw.set(table.oidsRaw.subarray(sourceSel * 20, sourceSel * 20 + 20), targetSel * 20);
  table.typeCodes[targetSel] = table.typeCodes[sourceSel];
  table.headerLens[targetSel] = table.headerLens[sourceSel];
  table.payloadLens[targetSel] = table.payloadLens[sourceSel];
  table.sizeVarLens[targetSel] = table.sizeVarLens[sourceSel];
  table.baseSlots[targetSel] = table.baseSlots[sourceSel];
  table.sizeVarBuf.set(table.sizeVarBuf.subarray(sourceSel * 5, sourceSel * 5 + 5), targetSel * 5);
  table.queuedForHeader[targetSel] = table.queuedForHeader[sourceSel];
  table.ofsPinned[targetSel] = options?.preserveTargetOfsPinned
    ? targetPinned
    : table.ofsPinned[sourceSel];
  table.syntheticPayloads[targetSel] = table.syntheticPayloads[sourceSel];

  if (table.baseOidRaw) {
    table.baseOidRaw.set(
      table.baseOidRaw.subarray(sourceSel * 20, sourceSel * 20 + 20),
      targetSel * 20
    );
  }
}

export function recordRewriteFailure(
  options: RewriteOptions | undefined,
  failure: RewriteFailure
): void {
  if (!options?.failure || options.failure.value) return;
  options.failure.value = failure;
}

export type PackReadState = {
  pack: OrderedPackSnapshotEntry;
  reader: SequentialReader;
  wholePack?: Uint8Array;
};

export function buildPackHeader(objectCount: number): Uint8Array {
  const header = new Uint8Array(12);
  header.set(new TextEncoder().encode("PACK"), 0);
  const view = new DataView(header.buffer);
  view.setUint32(4, 2);
  view.setUint32(8, objectCount);
  return header;
}

export function countRewriteSubrequest(
  log: Logger,
  warnedFlags: Set<string>,
  options: RewriteOptions | undefined,
  flag: string,
  details: Record<string, unknown>,
  n?: number
): boolean | void {
  const withinBudget = options?.countSubrequest?.(n);
  if (withinBudget === false && !warnedFlags.has(flag)) {
    warnedFlags.add(flag);
    log.warn("soft-budget-exhausted", details);
  }
  return withinBudget;
}

function getRequiredLimiter(options?: RewriteOptions): Limiter {
  if (!options?.limiter) {
    throw new Error("rewrite: limiter required");
  }
  return options.limiter;
}

async function loadWholePack(
  env: Env,
  pack: OrderedPackSnapshotEntry,
  log: Logger,
  warnedFlags: Set<string>,
  options?: RewriteOptions
): Promise<Uint8Array | undefined> {
  return await readPackRange(env, pack.packKey, 0, pack.packBytes, {
    limiter: getRequiredLimiter(options),
    signal: options?.signal,
    countSubrequest: (n?: number) =>
      countRewriteSubrequest(
        log,
        warnedFlags,
        options,
        `rewrite-whole-pack:${pack.packKey}`,
        { op: "r2:get-range", packKey: pack.packKey },
        n
      ),
  });
}

function createPackReadState(
  env: Env,
  pack: OrderedPackSnapshotEntry,
  log: Logger,
  warnedFlags: Set<string>,
  options?: RewriteOptions
): PackReadState {
  const readerLog = createLogger(env.LOG_LEVEL, { service: "RewritePackReader" });
  return {
    pack,
    reader: new SequentialReader(
      env,
      pack.packKey,
      pack.packBytes,
      DEFAULT_CHUNK_SIZE,
      getRequiredLimiter(options),
      (n?: number) =>
        countRewriteSubrequest(
          log,
          warnedFlags,
          options,
          `rewrite-range:${pack.packKey}`,
          { op: "r2:get-range", packKey: pack.packKey },
          n
        ),
      readerLog,
      options?.signal
    ),
  };
}

export async function ensurePackReadState(
  env: Env,
  pack: OrderedPackSnapshotEntry,
  packSlot: number,
  readerStates: Map<number, PackReadState>,
  log: Logger,
  warnedFlags: Set<string>,
  options?: RewriteOptions
): Promise<PackReadState> {
  const existing = readerStates.get(packSlot);
  if (existing) return existing;

  const state = createPackReadState(env, pack, log, warnedFlags, options);
  if (pack.packBytes <= WHOLE_PACK_MAX_BYTES) {
    let loaded = 0;
    for (const s of readerStates.values()) {
      if (s.wholePack) loaded += s.wholePack.length;
    }
    if (loaded + pack.packBytes <= WHOLE_PACK_TOTAL_BUDGET) {
      state.wholePack = await loadWholePack(env, pack, log, warnedFlags, options);
    }
  }
  readerStates.set(packSlot, state);
  return state;
}

/**
 * Read a pack entry header at the given byte offset.
 * For the wholePack path the subarray is stable; for the SequentialReader
 * path the returned `sizeVarBytes` references the reader buffer and must
 * be copied before the next preload.
 */
export async function readSelectedHeader(
  state: PackReadState,
  offset: number
): Promise<PackHeaderEx | undefined> {
  if (state.wholePack) {
    return readPackHeaderExFromBuf(state.wholePack, offset);
  }

  const bytesLeft = Math.max(0, state.pack.packBytes - offset);
  const headerBytes = await state.reader.readRange(offset, Math.min(HEADER_READ_BYTES, bytesLeft));
  return readPackHeaderExFromBuf(headerBytes, 0);
}

/** Search packs in snapshot order; first match wins (duplicate selection). */
export function resolveOrderedEntryByOid(
  snapshot: OrderedPackSnapshot,
  oid: string | Uint8Array
): { packSlot: number; pack: OrderedPackSnapshotEntry; entryIndex: number } | undefined {
  const candidate = findFirstPackedObjectCandidate(snapshot.packs, oid);
  if (!candidate) return undefined;

  return {
    packSlot: candidate.packSlot,
    pack: snapshot.packs[candidate.packSlot]!,
    entryIndex: candidate.objectIndex,
  };
}
