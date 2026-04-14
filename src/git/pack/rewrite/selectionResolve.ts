import type { OrderedPackSnapshot } from "@/git/operations/fetch/types.ts";
import type { IdxView } from "@/git/object-store/types.ts";
import type { Logger } from "@/common/logger.ts";
import type { PackHeaderEx } from "../packMeta.ts";

import { hexToBytes } from "@/common/index.ts";
import { findOffsetIndex } from "@/git/object-store/index.ts";
import {
  claimCanonicalOwner,
  clonePackHeader,
  type DuplicateHeaderCache,
  type SelectedOidLookup,
  type SelectionStats,
} from "./ownership.ts";
import {
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

/**
 * Result of reading a single entry header and resolving its delta base.
 *
 * When `redirectTo` is set, `sel` was redirected to another already-selected
 * owner for the same OID. When `supersedeSel` is set, `sel` took ownership
 * back from an older duplicate and that previous owner should be compacted out
 * after all phases complete.
 */
export type HeaderResolveResult =
  | { ok: true; ofsBaseCanonicalized: boolean; redirectTo?: number; supersedeSel?: number }
  | { ok: false };

/**
 * Add a (packSlot, entryIndex) pair to the selection table if not already
 * present. Returns the selection slot (new or existing).
 */
export function addEntry(
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
 */
export async function readHeaderAndResolveBase(
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

export function resolveDeltaBaseFromHeader(
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
  // redirect repair pass. Keeping this logic in one place avoids a second
  // implementation drifting on subtle rules like OFS pinning or REF_DELTA
  // base OID storage.

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
