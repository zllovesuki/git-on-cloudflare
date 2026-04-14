import type { OrderedPackSnapshot } from "@/git/operations/fetch/types.ts";
import type { IdxView } from "@/git/object-store/types.ts";
import type { Logger } from "@/common/logger.ts";
import type { PackHeaderEx } from "../packMeta.ts";

import { hexToBytes } from "@/common/index.ts";
import { findOffsetIndex } from "@/git/object-store/index.ts";
import {
  claimCanonicalOwner,
  clonePackHeader,
  createSelectedOidLookup,
  type DuplicateHeaderCache,
  type SelectedOidLookup,
  type SelectionStats,
} from "./ownership.ts";
import {
  allocateSelectionTable,
  ensurePackReadState,
  growSelectionTable,
  readSelectedHeader,
  resolveOrderedEntryByOid,
  selectionKey,
  setSelectionEntryIdentity,
  storeSelectionHeader,
  type PackReadState,
  type RewriteOptions,
  type SelectionTable,
} from "./shared.ts";

export type BuildSelectionResult = {
  table: SelectionTable;
  readerStates: Map<number, PackReadState>;
  addedDeltaBases: number;
};

/**
 * Result of reading a single entry header and resolving its delta base.
 *
 * When `redirectTo` is set, `sel` was redirected to another already-selected
 * owner for the same OID. When `supersedeSel` is set, `sel` took ownership
 * back from an older duplicate and that previous owner should be compacted out
 * after all phases complete.
 */
type HeaderResolveResult =
  | { ok: true; ofsBaseCanonicalized: boolean; redirectTo?: number; supersedeSel?: number }
  | { ok: false };

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
  /** Maps selectionKey(packSlot, entryIndex) → selection slot for exact pack-position dedup. */
  const dedupMap = new Map<number, number>();
  const oidOwners = createSelectedOidLookup(table.capacity);
  const duplicateHeaderCache: DuplicateHeaderCache = new Map();
  const stats: SelectionStats = {
    duplicateRedirects: 0,
    duplicateOwnerUpgrades: 0,
    duplicateOfsOwnerTakeovers: 0,
    duplicateHeaderProbes: 0,
  };

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

  // Counter for selected delta entries replaced with a full-object duplicate
  // or redirected to an already-selected full owner for the same OID.
  let duplicateCanonicalizations = 0;

  // Duplicate-OID rows that will be compacted out after selection completes.
  // Most entries redirect current sel → owner sel; OFS-pinned takeovers invert
  // that and retire the previous owner instead.
  const deadSlots = new Map<number, number>();

  /** Collect the result of readHeaderAndResolveBase. Returns false on failure. */
  function collectResult(sel: number, result: HeaderResolveResult): boolean {
    if (!result.ok) return false;
    if (result.ofsBaseCanonicalized) duplicateCanonicalizations++;
    if (result.redirectTo !== undefined) {
      deadSlots.set(sel, result.redirectTo);
      stats.duplicateRedirects++;
    }
    if (result.supersedeSel !== undefined) {
      deadSlots.delete(sel);
      deadSlots.set(result.supersedeSel, sel);
      stats.duplicateRedirects++;
    }
    return true;
  }

  // --- Phase B: sort once, read headers in offset order
  // Sorting by (packSlot, offset) maximizes SequentialReader locality.
  const sortedSels = buildSortedIndex(table, 0, table.count);
  let secondaryQueue: number[] = [];

  for (const sel of sortedSels) {
    if (options?.signal?.aborted) return undefined;
    const result = await readHeaderAndResolveBase(
      table,
      sel,
      snapshot,
      readerStates,
      dedupMap,
      oidOwners,
      duplicateHeaderCache,
      stats,
      secondaryQueue,
      env,
      log,
      warnedFlags,
      options
    );
    if (!collectResult(sel, result)) return undefined;
  }

  // --- Phase C: chase delta bases until no new bases are discovered
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
      const result = await readHeaderAndResolveBase(
        table,
        sel,
        snapshot,
        readerStates,
        dedupMap,
        oidOwners,
        duplicateHeaderCache,
        stats,
        secondaryQueue,
        env,
        log,
        warnedFlags,
        options
      );
      if (!collectResult(sel, result)) return undefined;
    }
  }

  // Dead-slot pruning may keep some redirected duplicate deltas live when the
  // selected owner still depends on their exact row. Those redirected rows
  // returned early before resolving their own base chains, so finish that work
  // now before compaction decides which duplicates remain in the output.
  //
  // Footgun: "redirected" does not mean "safe to ignore". A redirected row is
  // only dead if compaction really removes it. Once pruneUnsafeDeadSlotRedirects
  // decides the row must stay live, streaming will visit it like any other row
  // and therefore requires `baseSlots[sel]` to be fully wired first.
  let retainedRedirectResolutions = 0;
  while (deadSlots.size > 0) {
    const safeDeadSlots = pruneUnsafeDeadSlotRedirects(table, deadSlots);
    const retainedRedirects = collectRetainedRedirectsNeedingBaseResolution(
      table,
      deadSlots,
      safeDeadSlots
    );
    if (retainedRedirects.length === 0) break;

    retainedRedirects.sort(
      (a, b) => table.packSlots[a] - table.packSlots[b] || table.offsets[a] - table.offsets[b]
    );

    for (const sel of retainedRedirects) {
      if (options?.signal?.aborted) return undefined;
      const resolved = await resolveRetainedRedirectBase(
        table,
        sel,
        snapshot,
        readerStates,
        dedupMap,
        duplicateHeaderCache,
        secondaryQueue,
        env,
        log,
        warnedFlags,
        options
      );
      if (!resolved) return undefined;
      retainedRedirectResolutions++;
    }

    while (secondaryQueue.length > 0) {
      baseChaseIterations++;
      if (options?.signal?.aborted) return undefined;

      const batch = secondaryQueue;
      secondaryQueue = [];

      batch.sort(
        (a, b) => table.packSlots[a] - table.packSlots[b] || table.offsets[a] - table.offsets[b]
      );

      for (const sel of batch) {
        if (options?.signal?.aborted) return undefined;
        const result = await readHeaderAndResolveBase(
          table,
          sel,
          snapshot,
          readerStates,
          dedupMap,
          oidOwners,
          duplicateHeaderCache,
          stats,
          secondaryQueue,
          env,
          log,
          warnedFlags,
          options
        );
        if (!collectResult(sel, result)) return undefined;
      }
    }
  }

  // --- Compact dead duplicate-OID slots out of the table. Most redirected
  //     duplicates can collapse onto their surviving owner, but duplicates
  //     that are still needed to keep the base graph acyclic stay live.
  if (deadSlots.size > 0) {
    compactDeadSlots(table, deadSlots, log);
  }

  const headerReadMs = Date.now() - headerStart;
  const addedDeltaBases = table.count - initialResolvedCount;

  log.info("rewrite:selection", {
    requestedOids: neededOids.length,
    selectedEntries: table.count,
    addedDeltaBases,
    duplicateCanonicalizations,
    ownerLookupEntries: oidOwners.count,
    duplicateRedirects: stats.duplicateRedirects,
    duplicateOwnerUpgrades: stats.duplicateOwnerUpgrades,
    duplicateOfsOwnerTakeovers: stats.duplicateOfsOwnerTakeovers,
    duplicateHeaderProbes: stats.duplicateHeaderProbes,
    retainedRedirectResolutions,
    resolveMs,
    headerReadMs,
    baseChaseIterations,
  });

  return { table, readerStates, addedDeltaBases };
}

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
  setSelectionEntryIdentity(table, sel, packSlot, entryIndex, idx);
  table.baseSlots[sel] = -1;

  dedupMap.set(key, sel);
  return sel;
}

/**
 * Read the header for a selection slot, store all fields into the table,
 * and immediately resolve any delta base. New bases are pushed to
 * `secondaryQueue` for chase in the next iteration.
 *
 */
async function readHeaderAndResolveBase(
  table: SelectionTable,
  sel: number,
  snapshot: OrderedPackSnapshot,
  readerStates: Map<number, PackReadState>,
  dedupMap: Map<number, number>,
  oidOwners: SelectedOidLookup,
  duplicateHeaderCache: DuplicateHeaderCache,
  stats: SelectionStats,
  secondaryQueue: number[],
  env: Env,
  log: Logger,
  warnedFlags: Set<string>,
  options?: RewriteOptions
): Promise<HeaderResolveResult> {
  // Skip if already read (entry added as both needed and as a base)
  table.queuedForHeader[sel] = 0;
  if (table.typeCodes[sel] !== 0) return { ok: true, ofsBaseCanonicalized: false };

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
    return { ok: false };
  }
  duplicateHeaderCache.set(
    selectionKey(packSlot, table.entryIndices[sel]),
    clonePackHeader(header)
  );

  if (!storeSelectionHeader(table, sel, offset, table.nextOffsets[sel], header)) {
    log.warn("rewrite:invalid-payload-length", { packKey: pack.packKey, offset });
    return { ok: false };
  }
  const resolvedHeader = header;

  let canonicalized = false;
  const ownership = await claimCanonicalOwner(
    table,
    sel,
    snapshot,
    readerStates,
    dedupMap,
    oidOwners,
    duplicateHeaderCache,
    stats,
    env,
    log,
    warnedFlags,
    options
  );
  if (ownership.kind === "error") {
    return { ok: false };
  }
  if (ownership.kind === "redirect") {
    if (ownership.upgradedOwner) {
      log.debug("rewrite:duplicate-owner-upgraded", {
        fromPackKey: pack.packKey,
        offset,
        targetSel: ownership.targetSel,
      });
    } else {
      log.debug("rewrite:duplicate-owner-redirect", {
        fromPackKey: pack.packKey,
        offset,
        targetSel: ownership.targetSel,
      });
    }
    return {
      ok: true,
      ofsBaseCanonicalized: ownership.canonicalized,
      redirectTo: ownership.targetSel,
    };
  }
  if (ownership.kind === "takeover") {
    log.debug("rewrite:duplicate-owner-taken-over-by-ofs-base", {
      fromPackKey: pack.packKey,
      offset,
      previousOwnerSel: ownership.previousOwnerSel,
    });
    canonicalized = ownership.canonicalized;
    // The current row stays live after reclaiming ownership, so its delta base
    // still needs to be resolved below before the row can be streamed.
    return resolveDeltaBaseAndFinish(ownership.previousOwnerSel);
  }
  if (ownership.kind === "swapped") {
    log.debug("rewrite:delta-canonicalized-to-full", {
      fromPackKey: pack.packKey,
      offset,
    });
    return { ok: true, ofsBaseCanonicalized: true };
  }
  canonicalized = ownership.canonicalized;

  return resolveDeltaBaseAndFinish();

  function resolveDeltaBaseAndFinish(supersedeSel?: number): HeaderResolveResult {
    if (
      !resolveDeltaBaseFromHeader(
        table,
        sel,
        snapshot,
        dedupMap,
        secondaryQueue,
        log,
        resolvedHeader
      )
    ) {
      return { ok: false };
    }

    return {
      ok: true,
      ofsBaseCanonicalized: canonicalized,
      supersedeSel,
    };
  }
}

function resolveDeltaBaseFromHeader(
  table: SelectionTable,
  sel: number,
  snapshot: OrderedPackSnapshot,
  dedupMap: Map<number, number>,
  secondaryQueue: number[],
  log: Logger,
  header: PackHeaderEx
): boolean {
  const packSlot = table.packSlots[sel];
  const pack = snapshot.packs[packSlot];
  const offset = table.offsets[sel];

  // Shared helper for both the first header-read pass and the retained-
  // redirect repair pass above. Keeping this logic in one place avoids a
  // second implementation drifting on subtle rules like OFS pinning or
  // REF_DELTA base OID storage.

  // --- OFS_DELTA: base is at (same pack, offset - baseRel) ------------------
  if (header.type === 6 && header.baseRel !== undefined) {
    const baseOffset = offset - header.baseRel;
    const baseIndex = findOffsetIndex(pack.idx, baseOffset);
    if (baseIndex === undefined) {
      log.warn("rewrite:missing-ofs-base", { packKey: pack.packKey, offset, baseOffset });
      return false;
    }

    // OFS_DELTA bases stay pack-local. The source pack's offset ordering is
    // already acyclic; cross-pack canonicalization here can manufacture cycles
    // when a newer duplicate of the same OID is itself stored as a delta.
    const baseSel = addEntry(table, dedupMap, packSlot, baseIndex, pack.idx);
    table.ofsPinned[baseSel] = 1;

    if (baseSel === sel) {
      // Self-referential OFS_DELTA (e.g. baseRel is zero or points back to
      // own offset). This indicates pack corruption — abort the selection so
      // the caller can retry or investigate.
      log.warn("rewrite:self-referential-delta", {
        packKey: pack.packKey,
        offset,
        deltaType: "ofs",
        baseRel: header.baseRel,
      });
      return false;
    }
    table.baseSlots[sel] = baseSel;
    if (table.typeCodes[baseSel] === 0 && !table.queuedForHeader[baseSel]) {
      table.queuedForHeader[baseSel] = 1;
      secondaryQueue.push(baseSel);
    }
  }

  // --- REF_DELTA: base identified by OID, may be in any pack ----------------
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
    if (baseSel === sel) {
      // Self-referential REF_DELTA with no full-object duplicate available.
      // This pack cannot be rewritten into a topologically valid output.
      log.warn("rewrite:self-referential-delta", {
        packKey: pack.packKey,
        offset,
        deltaType: "ref",
        baseOid: header.baseOid,
      });
      return false;
    }

    table.baseSlots[sel] = baseSel;
    if (table.typeCodes[baseSel] === 0 && !table.queuedForHeader[baseSel]) {
      table.queuedForHeader[baseSel] = 1;
      secondaryQueue.push(baseSel);
    }
  }

  return true;
}

function collectRetainedRedirectsNeedingBaseResolution(
  table: SelectionTable,
  deadSlots: Map<number, number>,
  safeDeadSlots: Map<number, number>
): number[] {
  const retained: number[] = [];
  for (const [deadSel] of deadSlots) {
    // `safeDeadSlots` is the subset we are still allowed to remove. When a row
    // is present in `deadSlots` but absent here, dead-slot pruning decided that
    // the redirect would manufacture a cycle, so this "dead" row is actually
    // staying live in the output pack.
    if (safeDeadSlots.has(deadSel)) continue;
    const typeCode = table.typeCodes[deadSel];
    if ((typeCode === 6 || typeCode === 7) && table.baseSlots[deadSel] < 0) {
      retained.push(deadSel);
    }
  }
  return retained;
}

async function resolveRetainedRedirectBase(
  table: SelectionTable,
  sel: number,
  snapshot: OrderedPackSnapshot,
  readerStates: Map<number, PackReadState>,
  dedupMap: Map<number, number>,
  duplicateHeaderCache: DuplicateHeaderCache,
  secondaryQueue: number[],
  env: Env,
  log: Logger,
  warnedFlags: Set<string>,
  options?: RewriteOptions
): Promise<boolean> {
  if (table.baseSlots[sel] >= 0) return true;

  const packSlot = table.packSlots[sel];
  const pack = snapshot.packs[packSlot];
  const cacheKey = selectionKey(packSlot, table.entryIndices[sel]);
  let header = duplicateHeaderCache.get(cacheKey);

  if (header === undefined) {
    // The common path already memoized this header before issuing the redirect.
    // Falling back to a reread is correct here, but it should stay rare and
    // bounded to the retained-redirect edge case.
    const readState = await ensurePackReadState(
      env,
      pack,
      packSlot,
      readerStates,
      log,
      warnedFlags,
      options
    );
    header = await readSelectedHeader(readState, table.offsets[sel]);
    duplicateHeaderCache.set(cacheKey, header ? clonePackHeader(header) : null);
  }

  if (!header) {
    log.warn("rewrite:header-read-failed", { packKey: pack.packKey, offset: table.offsets[sel] });
    return false;
  }

  log.debug("rewrite:retained-redirect-base-resolve", {
    sel,
    packKey: pack.packKey,
    entryIndex: table.entryIndices[sel],
    typeCode: table.typeCodes[sel],
  });

  // This intentionally resolves only the retained row's own base edge. Any
  // newly discovered base rows still flow through the normal secondary queue so
  // they get the same header-read and duplicate-owner handling as the primary
  // selection pass.
  return resolveDeltaBaseFromHeader(table, sel, snapshot, dedupMap, secondaryQueue, log, header);
}

/**
 * Remove dead selection slots and remap baseSel references.
 *
 * Most dead slots are duplicate-OID selections redirected to another already-
 * selected owner. A small subset may need to stay live when collapsing them
 * would fold the surviving row's base chain back onto itself. This runs once
 * after all phases complete, so the extra graph walk is acceptable.
 */
export function compactDeadSlots(
  table: SelectionTable,
  deadSlots: Map<number, number>,
  log: Logger
): void {
  const safeDeadSlots = pruneUnsafeDeadSlotRedirects(table, deadSlots);

  // 1. Redirect baseSel references from dead slots to their targets.
  //    Handles chains (dead → dead → live) by following until stable.
  function resolve(sel: number): number {
    let cur = sel;
    for (let depth = 0; depth < safeDeadSlots.size + 1; depth++) {
      const next = safeDeadSlots.get(cur);
      if (next === undefined) return cur;
      cur = next;
    }
    return cur; // fallback: should not loop
  }

  for (let i = 0; i < table.count; i++) {
    if (table.baseSlots[i] >= 0) {
      table.baseSlots[i] = resolve(table.baseSlots[i]);
    }
  }

  // 2. Build old → new index remap (skipping dead slots).
  const remap = new Int32Array(table.count).fill(-1);
  let write = 0;
  for (let read = 0; read < table.count; read++) {
    if (safeDeadSlots.has(read)) continue;
    remap[read] = write;
    if (write !== read) {
      table.packSlots[write] = table.packSlots[read];
      table.entryIndices[write] = table.entryIndices[read];
      table.offsets[write] = table.offsets[read];
      table.nextOffsets[write] = table.nextOffsets[read];
      // The live row's raw OID stays authoritative for owner lookups and
      // follow-on duplicate redirects, so compact it with the rest of the row.
      table.oidsRaw.set(table.oidsRaw.subarray(read * 20, read * 20 + 20), write * 20);
      table.typeCodes[write] = table.typeCodes[read];
      table.headerLens[write] = table.headerLens[read];
      table.payloadLens[write] = table.payloadLens[read];
      table.sizeVarLens[write] = table.sizeVarLens[read];
      // baseSlots is row state too: move it with the row now, then remap the
      // preserved old live indices to their new compacted indices below.
      table.baseSlots[write] = table.baseSlots[read];
      table.sizeVarBuf.set(table.sizeVarBuf.subarray(read * 5, read * 5 + 5), write * 5);
      table.queuedForHeader[write] = table.queuedForHeader[read];
      table.ofsPinned[write] = table.ofsPinned[read];
      if (table.baseOidRaw) {
        table.baseOidRaw.set(table.baseOidRaw.subarray(read * 20, read * 20 + 20), write * 20);
      }
    }
    write++;
  }

  // 3. Remap baseSel references to new indices.
  for (let i = 0; i < write; i++) {
    const base = table.baseSlots[i];
    if (base >= 0) {
      table.baseSlots[i] = remap[base];
    }
  }

  log.debug("rewrite:compact-dead-slots", {
    removed: table.count - write,
  });
  table.count = write;
}

/**
 * Some duplicate-owner redirects are not safe to compact away.
 *
 * If the surviving owner already depends on the duplicate being removed,
 * redirecting every reference from `dead -> live` would collapse that live
 * row's base chain back onto itself and manufacture a cycle. Git packs may
 * legitimately carry duplicate OIDs, so retain those duplicates instead of
 * forcing a single canonical row in this case.
 */
function pruneUnsafeDeadSlotRedirects(
  table: SelectionTable,
  deadSlots: Map<number, number>
): Map<number, number> {
  const safeDeadSlots = new Map(deadSlots);

  let changed = true;
  while (changed) {
    changed = false;

    for (const [deadSel] of safeDeadSlots) {
      const targetSel = resolveDeadSlotRedirect(deadSel, safeDeadSlots);
      if (targetSel === deadSel || dependsOnSelectionSlot(table, targetSel, deadSel)) {
        safeDeadSlots.delete(deadSel);
        changed = true;
      }
    }
  }

  return safeDeadSlots;
}

function resolveDeadSlotRedirect(sel: number, deadSlots: Map<number, number>): number {
  let cur = sel;
  for (let depth = 0; depth < deadSlots.size + 1; depth++) {
    const next = deadSlots.get(cur);
    if (next === undefined) return cur;
    cur = next;
  }
  return cur;
}

/**
 * Selection dependencies are a single base chain per row, so checking whether
 * `startSel` depends on `targetSel` is a bounded linear walk.
 */
function dependsOnSelectionSlot(
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
