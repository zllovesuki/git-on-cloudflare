import type { OrderedPackSnapshot } from "@/git/operations/fetch/types.ts";
import type { IdxView } from "@/git/object-store/types.ts";
import type { Logger } from "@/common/logger.ts";

import { BinaryHeap, hexToBytes } from "@/common/index.ts";
import { findOffsetIndex, getNextOffsetByIndex } from "@/git/object-store/index.ts";
import { ofsDeltaDistanceLength } from "../packMeta.ts";
import {
  HEADER_STABILITY_CAP,
  allocateSelectionTable,
  ensurePackReadState,
  growSelectionTable,
  readSelectedHeader,
  resolveOrderedEntryByOid,
  selectionKey,
  type PackReadState,
  type RewriteOptions,
  type SelectionTable,
} from "./shared.ts";

// ---------------------------------------------------------------------------
// Public result type
// ---------------------------------------------------------------------------

export type BuildSelectionResult = {
  table: SelectionTable;
  readerStates: Map<number, PackReadState>;
  addedDeltaBases: number;
};

// ---------------------------------------------------------------------------
// Selection: resolve → sorted header scan → secondary base chase
// ---------------------------------------------------------------------------

export async function buildSelection(
  env: Env,
  snapshot: OrderedPackSnapshot,
  neededOids: string[],
  log: Logger,
  warnedFlags: Set<string>,
  options?: RewriteOptions
): Promise<BuildSelectionResult | undefined> {
  const table = allocateSelectionTable(Math.max(neededOids.length, 16));
  const readerStates = new Map<number, PackReadState>();
  /** Maps selectionKey(packSlot, entryIndex) → selection slot for dedup. */
  const dedupMap = new Map<number, number>();

  const resolveStart = Date.now();

  // --- Phase A: resolve all needed OIDs to (packSlot, entryIndex) ----------
  for (const oid of neededOids) {
    if (options?.signal?.aborted) return undefined;

    const oidBytes = hexToBytes(oid);
    const location = resolveOrderedEntryByOid(snapshot, oidBytes);
    if (!location) {
      log.warn("rewrite:missing-needed-object", { oid });
      return undefined;
    }

    addEntry(table, dedupMap, location.packSlot, location.entryIndex, location.pack.idx);
  }

  const initialResolvedCount = table.count;
  const resolveMs = Date.now() - resolveStart;

  // --- Early passthrough: skip header scan + base chase when every object
  //     in a single pack is already selected. The passthrough stream path
  //     reads raw pack bytes and needs none of the header data. ----------
  if (snapshot.packs.length === 1 && table.count === snapshot.packs[0].idx.count) {
    log.info("rewrite:selection", {
      requestedOids: neededOids.length,
      selectedEntries: table.count,
      addedDeltaBases: 0,
      resolveMs,
      headerReadMs: 0,
      baseChaseIterations: 0,
    });
    return { table, readerStates, addedDeltaBases: 0 };
  }

  const headerStart = Date.now();

  // --- Phase B: sort once, read headers in offset order --------------------
  // Sorting by (packSlot, offset) maximizes SequentialReader locality.
  const sortedSels = buildSortedIndex(table, 0, table.count);
  let secondaryQueue: number[] = [];

  for (const sel of sortedSels) {
    if (options?.signal?.aborted) return undefined;
    const ok = await readHeaderAndResolveBase(
      table,
      sel,
      snapshot,
      readerStates,
      dedupMap,
      secondaryQueue,
      env,
      log,
      warnedFlags,
      options
    );
    if (!ok) return undefined;
  }

  // --- Phase C: chase delta bases until no new bases are discovered --------
  let baseChaseIterations = 0;
  while (secondaryQueue.length > 0) {
    baseChaseIterations++;
    if (options?.signal?.aborted) return undefined;

    const batch = secondaryQueue;
    secondaryQueue = [];

    // Sort for sequential reads within each pack
    batch.sort(
      (a, b) => table.packSlots[a] - table.packSlots[b] || table.offsets[a] - table.offsets[b]
    );

    for (const sel of batch) {
      if (options?.signal?.aborted) return undefined;
      const ok = await readHeaderAndResolveBase(
        table,
        sel,
        snapshot,
        readerStates,
        dedupMap,
        secondaryQueue,
        env,
        log,
        warnedFlags,
        options
      );
      if (!ok) return undefined;
    }
  }

  const headerReadMs = Date.now() - headerStart;
  const addedDeltaBases = table.count - initialResolvedCount;

  log.info("rewrite:selection", {
    requestedOids: neededOids.length,
    selectedEntries: table.count,
    addedDeltaBases,
    resolveMs,
    headerReadMs,
    baseChaseIterations,
  });

  return { table, readerStates, addedDeltaBases };
}

// ---------------------------------------------------------------------------
// Passthrough detection
// ---------------------------------------------------------------------------

export function canPassthroughSinglePack(
  snapshot: OrderedPackSnapshot,
  table: SelectionTable
): boolean {
  return snapshot.packs.length === 1 && table.count === snapshot.packs[0]?.idx.count;
}

// ---------------------------------------------------------------------------
// Topology sort: Kahn's algorithm with binary min-heap
// ---------------------------------------------------------------------------

/**
 * Populates `table.outputOrder` with a topologically valid output ordering
 * (bases before deltas). Returns false on cycle or incomplete graph.
 */
export function buildOutputOrder(table: SelectionTable, log: Logger): boolean {
  const n = table.count;

  // Build dependency graph using linked-list arrays (same pattern as
  // src/git/pack/indexer/resolve/dependencies.ts)
  const indegree = new Uint8Array(n);
  const childHead = new Int32Array(n).fill(-1);
  const childNext = new Int32Array(n).fill(-1);

  for (let sel = 0; sel < n; sel++) {
    const base = table.baseSlots[sel];
    if (base < 0) continue;
    indegree[sel]++;
    childNext[sel] = childHead[base];
    childHead[base] = sel;
  }

  // Min-heap comparing (packSlots, offsets) for stable tie-breaking.
  // Reuses the generic BinaryHeap from src/common/heap.ts.
  const cmp = (a: number, b: number) => {
    const packDiff = table.packSlots[a] - table.packSlots[b];
    if (packDiff !== 0) return packDiff;
    return table.offsets[a] - table.offsets[b];
  };
  const roots: number[] = [];
  for (let sel = 0; sel < n; sel++) {
    if (indegree[sel] === 0) roots.push(sel);
  }
  const heap = new BinaryHeap<number>(cmp, roots);

  let cursor = 0;
  while (!heap.isEmpty()) {
    const sel = heap.pop()!;
    table.outputOrder[cursor++] = sel;

    let child = childHead[sel];
    while (child >= 0) {
      indegree[child]--;
      if (indegree[child] === 0) heap.push(child);
      child = childNext[child];
    }
  }

  if (cursor !== n) {
    log.warn("rewrite:topology-incomplete", { selected: n, ordered: cursor });
    return false;
  }
  return true;
}

// ---------------------------------------------------------------------------
// Header convergence: recompute OFS distances until stable
// ---------------------------------------------------------------------------

/**
 * Computes output header lengths and offsets, iterating until OFS_DELTA
 * distance varints stabilize. Returns false on convergence failure.
 */
export function computeHeaderLengths(table: SelectionTable, log: Logger): boolean {
  const n = table.count;

  // Seed initial output header lengths
  for (let i = 0; i < n; i++) {
    const sel = table.outputOrder[i];
    const svLen = table.sizeVarLens[sel];
    const type = table.typeCodes[sel];

    if (type === 6) {
      // Initial estimate using the original source-pack OFS distance
      const base = table.baseSlots[sel];
      table.outputHeaderLens[sel] =
        svLen + ofsDeltaDistanceLength(table.offsets[sel] - table.offsets[base]);
    } else if (type === 7) {
      table.outputHeaderLens[sel] = svLen + 20;
    } else {
      table.outputHeaderLens[sel] = svLen;
    }
  }

  for (let iteration = 0; iteration < HEADER_STABILITY_CAP; iteration++) {
    // Compute cumulative output offsets (12-byte PACK header)
    let cursor = 12;
    for (let i = 0; i < n; i++) {
      const sel = table.outputOrder[i];
      table.outputOffsets[sel] = cursor;
      cursor += table.outputHeaderLens[sel] + table.payloadLens[sel];
    }

    // Recompute OFS_DELTA distances with new output offsets
    let changed = false;
    for (let i = 0; i < n; i++) {
      const sel = table.outputOrder[i];
      if (table.typeCodes[sel] !== 6) continue;

      const base = table.baseSlots[sel];
      if (base < 0) continue;

      const distance = table.outputOffsets[sel] - table.outputOffsets[base];
      const nextLen = table.sizeVarLens[sel] + ofsDeltaDistanceLength(distance);
      if (nextLen !== table.outputHeaderLens[sel]) {
        table.outputHeaderLens[sel] = nextLen;
        changed = true;
      }
    }

    if (!changed) {
      // The 5-byte sizeVarBuf and 32-bit OFS distance encoding assume < 4 GiB.
      if (cursor > 0xffff_ffff) {
        log.warn("rewrite:output-pack-exceeds-32bit", { totalBytes: cursor });
        return false;
      }
      return true;
    }
  }

  log.warn("rewrite:header-lengths-did-not-converge", { selected: n });
  return false;
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/**
 * Add a (packSlot, entryIndex) pair to the selection table if not already
 * present. Returns the selection slot (new or existing).
 */
function addEntry(
  table: SelectionTable,
  dedupMap: Map<number, number>,
  packSlot: number,
  entryIndex: number,
  idx: IdxView
): number {
  const key = selectionKey(packSlot, entryIndex);
  const existing = dedupMap.get(key);
  if (existing !== undefined) return existing;

  if (table.count >= table.capacity) growSelectionTable(table);

  const sel = table.count++;
  table.packSlots[sel] = packSlot;
  table.entryIndices[sel] = entryIndex;
  table.offsets[sel] = idx.offsets[entryIndex];

  const nextOffset = getNextOffsetByIndex(idx, entryIndex);
  if (nextOffset === undefined) {
    throw new Error(`rewrite: missing next offset for pack#${packSlot} entry#${entryIndex}`);
  }
  table.nextOffsets[sel] = nextOffset;
  table.baseSlots[sel] = -1;

  dedupMap.set(key, sel);
  return sel;
}

/**
 * Read the header for a selection slot, store all fields into the table,
 * and immediately resolve any delta base. New bases are pushed to
 * `secondaryQueue` for chase in the next iteration.
 */
async function readHeaderAndResolveBase(
  table: SelectionTable,
  sel: number,
  snapshot: OrderedPackSnapshot,
  readerStates: Map<number, PackReadState>,
  dedupMap: Map<number, number>,
  secondaryQueue: number[],
  env: Env,
  log: Logger,
  warnedFlags: Set<string>,
  options?: RewriteOptions
): Promise<boolean> {
  // Skip if already read (entry added as both needed and as a base)
  table.queuedForHeader[sel] = 0;
  if (table.typeCodes[sel] !== 0) return true;

  const packSlot = table.packSlots[sel];
  const pack = snapshot.packs[packSlot];
  const readState = await ensurePackReadState(
    env,
    pack,
    packSlot,
    readerStates,
    log,
    warnedFlags,
    options
  );

  const offset = table.offsets[sel];
  const header = await readSelectedHeader(readState, offset);
  if (!header) {
    log.warn("rewrite:header-read-failed", { packKey: pack.packKey, offset });
    return false;
  }

  table.typeCodes[sel] = header.type;
  table.headerLens[sel] = header.headerLen;

  const payloadLength = table.nextOffsets[sel] - offset - header.headerLen;
  if (payloadLength < 0) {
    log.warn("rewrite:invalid-payload-length", { packKey: pack.packKey, offset });
    return false;
  }
  table.payloadLens[sel] = payloadLength;

  // Copy sizeVarBytes into concatenated buffer to avoid pinning reader chunks
  const svStart = sel * 5;
  table.sizeVarBuf.set(header.sizeVarBytes, svStart);
  table.sizeVarLens[sel] = header.sizeVarBytes.length;

  // --- OFS_DELTA: base is at (same pack, offset - baseRel) ----------------
  if (header.type === 6 && header.baseRel !== undefined) {
    const baseOffset = offset - header.baseRel;
    const baseIndex = findOffsetIndex(pack.idx, baseOffset);
    if (baseIndex === undefined) {
      log.warn("rewrite:missing-ofs-base", { packKey: pack.packKey, offset, baseOffset });
      return false;
    }

    const baseSel = addEntry(table, dedupMap, packSlot, baseIndex, pack.idx);
    table.baseSlots[sel] = baseSel;
    if (table.typeCodes[baseSel] === 0 && !table.queuedForHeader[baseSel]) {
      table.queuedForHeader[baseSel] = 1;
      secondaryQueue.push(baseSel);
    }
  }

  // --- REF_DELTA: base identified by OID, may be in any pack --------------
  if (header.type === 7 && header.baseOid) {
    // Store raw base OID bytes for streaming (avoids hex round-trip later)
    const rawBytes = hexToBytes(header.baseOid);
    if (!table.baseOidRaw) {
      table.baseOidRaw = new Uint8Array(table.capacity * 20);
    }
    table.baseOidRaw.set(rawBytes, sel * 20);

    // findOidIndex accepts Uint8Array — avoids re-materializing hex string
    const location = resolveOrderedEntryByOid(snapshot, rawBytes);
    if (!location) {
      log.warn("rewrite:missing-ref-base", {
        packKey: pack.packKey,
        offset,
        baseOid: header.baseOid,
      });
      return false;
    }

    const baseSel = addEntry(
      table,
      dedupMap,
      location.packSlot,
      location.entryIndex,
      location.pack.idx
    );
    table.baseSlots[sel] = baseSel;
    if (table.typeCodes[baseSel] === 0 && !table.queuedForHeader[baseSel]) {
      table.queuedForHeader[baseSel] = 1;
      secondaryQueue.push(baseSel);
    }
  }

  return true;
}

/** Build a Uint32Array of selection indices sorted by (packSlot, offset). */
function buildSortedIndex(table: SelectionTable, start: number, end: number): Uint32Array {
  const len = end - start;
  const indices = new Uint32Array(len);
  for (let i = 0; i < len; i++) indices[i] = start + i;
  indices.sort(
    (a, b) => table.packSlots[a] - table.packSlots[b] || table.offsets[a] - table.offsets[b]
  );
  return indices;
}
