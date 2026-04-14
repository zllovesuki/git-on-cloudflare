import type { OrderedPackSnapshot } from "@/git/operations/fetch/types.ts";
import type { Logger } from "@/common/logger.ts";

import { hexToBytes } from "@/common/index.ts";
import {
  createSelectedOidLookup,
  type DuplicateHeaderCache,
  type SelectionStats,
} from "./ownership.ts";
import { collapseUnsafeRedirectOwners, compactDeadSlots } from "./selectionCompact.ts";
import {
  collectRetainedRedirectsNeedingBaseResolution,
  resolveRetainedRedirectBase,
} from "./selectionRetained.ts";
import {
  addEntry,
  readHeaderAndResolveBase,
  type HeaderResolveResult,
} from "./selectionResolve.ts";
import {
  allocateSelectionTable,
  resolveOrderedEntryByOid,
  sortSelectionSlots,
  type PackReadState,
  type RewriteOptions,
  type SelectionTable,
} from "./shared.ts";

export type BuildSelectionResult = {
  table: SelectionTable;
  readerStates: Map<number, PackReadState>;
  addedDeltaBases: number;
};

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

  async function processHeaderBatch(batch: Uint32Array | number[]): Promise<boolean> {
    for (const sel of batch) {
      if (options?.signal?.aborted) return false;
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
      if (!collectResult(sel, result)) return false;
    }
    return true;
  }

  async function drainSecondaryQueue(): Promise<boolean> {
    while (secondaryQueue.length > 0) {
      baseChaseIterations++;
      if (options?.signal?.aborted) return false;

      const batch = secondaryQueue;
      secondaryQueue = [];

      // Keep each pass mostly forward-only within a pack so the reader can
      // stay on a small sliding window instead of bouncing around R2.
      sortSelectionSlots(table, batch);
      if (!(await processHeaderBatch(batch))) return false;
    }
    return true;
  }

  // --- Phase B: sort once, read headers in offset order
  // Sorting by (packSlot, offset) maximizes SequentialReader locality.
  const sortedSels = buildSortedIndex(table.count);
  let secondaryQueue: number[] = [];

  if (!(await processHeaderBatch(sortedSels))) return undefined;

  // --- Phase C: chase delta bases until no new bases are discovered
  let baseChaseIterations = 0;
  if (!(await drainSecondaryQueue())) return undefined;

  // Dead-slot pruning may keep some redirected duplicate deltas live when the
  // selected owner still depends on their exact row. Those redirected rows
  // returned early before resolving their own base chains, so finish that work
  // now before compaction decides which duplicates remain in the output.
  //
  // Footgun: "redirected" does not mean "safe to ignore". A redirected row is
  // only dead if compaction really removes it. Once dead-slot pruning decides
  // the row must stay live, streaming will visit it like any other row and
  // therefore requires `baseSlots[sel]` to be fully wired first.
  let retainedRedirectResolutions = 0;
  while (deadSlots.size > 0) {
    const retainedRedirects = collectRetainedRedirectsNeedingBaseResolution(table, deadSlots);
    if (retainedRedirects.length === 0) break;

    sortSelectionSlots(table, retainedRedirects);

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

    if (!(await drainSecondaryQueue())) return undefined;
  }

  // Some redirected duplicates only stayed live because the chosen owner still
  // depended on their exact row. Keeping both rows in the final pack makes the
  // rewrite fetchable again, but Git still rejects the output because the same
  // object OID appears twice. Collapse those cases back to one live row by
  // rewriting the owner slot to stream the retained duplicate's encoding.
  //
  // Footgun: the owner slot index must stay stable here because children may
  // already point at it. Only the row's source pack position and header/base
  // metadata change; the selection slot itself remains the canonical owner.
  let collapsedUnsafeRedirectOwners = 0;
  if (deadSlots.size > 0) {
    collapsedUnsafeRedirectOwners = collapseUnsafeRedirectOwners(table, deadSlots, log);
  }

  // --- Compact dead duplicate-OID slots out of the table. Most redirected
  //     duplicates can collapse onto their surviving owner. The retained-
  //     redirect repair above already rewrote the few topology-sensitive cases
  //     back to one live row before this final compaction pass runs.
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
    collapsedUnsafeRedirectOwners,
    resolveMs,
    headerReadMs,
    baseChaseIterations,
  });

  return { table, readerStates, addedDeltaBases };

  function buildSortedIndex(count: number): Uint32Array {
    const indices = new Uint32Array(count);
    for (let i = 0; i < count; i++) indices[i] = i;
    return sortSelectionSlots(table, indices) as Uint32Array;
  }
}
