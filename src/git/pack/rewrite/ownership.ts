import type { OrderedPackSnapshot } from "@/git/operations/fetch/types.ts";
import type { Logger } from "@/common/logger.ts";

import { collectPackedObjectCandidates } from "@/git/object-store/index.ts";
import type { PackHeaderEx } from "../packMeta.ts";
import {
  copySelectionRow,
  ensurePackReadState,
  readSelectedHeader,
  selectionKey,
  setSelectionEntryIdentity,
  storeSelectionHeader,
  type PackReadState,
  type RewriteOptions,
  type SelectionTable,
} from "./shared.ts";

/**
 * Planner-local OID owner table.
 *
 * This keeps canonical ownership keyed by the 20-byte object ID without
 * materializing hex strings or per-entry JS objects. The table stores only
 * live owner rows; duplicate rows are redirected and compacted later.
 */
export type SelectedOidLookup = {
  count: number;
  mask: number;
  used: Uint8Array;
  keys: Uint8Array;
  owners: Int32Array;
};

/**
 * Duplicate-run probes may re-read the same candidate entry header multiple
 * times while different rows converge onto the same OID. Cache a copied header
 * per pack position so those probes stay request-local and bounded.
 */
export type DuplicateHeaderCache = Map<number, PackHeaderEx | null>;

export type SelectionStats = {
  duplicateRedirects: number;
  duplicateOwnerUpgrades: number;
  duplicateOfsOwnerTakeovers: number;
  duplicateHeaderProbes: number;
};

export type ClaimOwnerResult =
  | { kind: "unchanged"; canonicalized: boolean }
  | { kind: "swapped" }
  | { kind: "error" }
  | { kind: "takeover"; canonicalized: boolean; previousOwnerSel: number }
  | { kind: "redirect"; canonicalized: boolean; targetSel: number; upgradedOwner: boolean };

type FullDuplicateCandidate =
  | { kind: "none" }
  | { kind: "redirect"; targetSel: number }
  | {
      kind: "swap";
      packSlot: number;
      entryIndex: number;
      offset: number;
      nextOffset: number;
      header: PackHeaderEx;
    };

/** Smallest power-of-two table size for the open-addressed OID owner map. */
function nextPowerOfTwo(value: number): number {
  let out = 1;
  while (out < value) out <<= 1;
  return out;
}

/** Hash a raw 20-byte object ID stored at `start` in `rawBytes`. */
function hashOidAt(rawBytes: Uint8Array, start: number): number {
  let hash = 2166136261;
  for (let i = 0; i < 20; i++) {
    hash ^= rawBytes[start + i]!;
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function slotKeyEquals(
  keys: Uint8Array,
  slot: number,
  rawBytes: Uint8Array,
  start: number
): boolean {
  const keyStart = slot * 20;
  for (let i = 0; i < 20; i++) {
    if (keys[keyStart + i] !== rawBytes[start + i]) return false;
  }
  return true;
}

/** Create the planner's `OID -> owner selection slot` table. */
export function createSelectedOidLookup(minimumEntries: number): SelectedOidLookup {
  const slotCount = nextPowerOfTwo(Math.max(16, minimumEntries * 2));
  return {
    count: 0,
    mask: slotCount - 1,
    used: new Uint8Array(slotCount),
    keys: new Uint8Array(slotCount * 20),
    owners: new Int32Array(slotCount).fill(-1),
  };
}

/** Rebuild the owner map when it crosses the target load factor. */
function rehashSelectedOidLookup(lookup: SelectedOidLookup, nextSlotCount: number): void {
  const previousUsed = lookup.used;
  const previousKeys = lookup.keys;
  const previousOwners = lookup.owners;

  lookup.mask = nextSlotCount - 1;
  lookup.used = new Uint8Array(nextSlotCount);
  lookup.keys = new Uint8Array(nextSlotCount * 20);
  lookup.owners = new Int32Array(nextSlotCount).fill(-1);
  lookup.count = 0;

  for (let slot = 0; slot < previousUsed.length; slot++) {
    if (!previousUsed[slot]) continue;
    const keyStart = slot * 20;
    setSelectedOidOwner(lookup, previousKeys, keyStart, previousOwners[slot]!);
  }
}

function ensureSelectedOidLookupCapacity(lookup: SelectedOidLookup): void {
  if ((lookup.count + 1) * 10 <= lookup.used.length * 7) return;
  rehashSelectedOidLookup(lookup, lookup.used.length * 2);
}

/** Return the live owner row for an OID, or `-1` if the OID is unseen. */
function findSelectedOidOwner(
  lookup: SelectedOidLookup,
  rawBytes: Uint8Array,
  start: number
): number {
  let slot = hashOidAt(rawBytes, start) & lookup.mask;
  while (lookup.used[slot]) {
    if (slotKeyEquals(lookup.keys, slot, rawBytes, start)) {
      return lookup.owners[slot]!;
    }
    slot = (slot + 1) & lookup.mask;
  }
  return -1;
}

/** Install or replace the live owner row for an OID. */
function setSelectedOidOwner(
  lookup: SelectedOidLookup,
  rawBytes: Uint8Array,
  start: number,
  ownerSel: number
): void {
  ensureSelectedOidLookupCapacity(lookup);

  let slot = hashOidAt(rawBytes, start) & lookup.mask;
  while (lookup.used[slot]) {
    if (slotKeyEquals(lookup.keys, slot, rawBytes, start)) {
      lookup.owners[slot] = ownerSel;
      return;
    }
    slot = (slot + 1) & lookup.mask;
  }

  lookup.used[slot] = 1;
  lookup.keys.set(rawBytes.subarray(start, start + 20), slot * 20);
  lookup.owners[slot] = ownerSel;
  lookup.count++;
}

export function clonePackHeader(header: PackHeaderEx): PackHeaderEx {
  return {
    type: header.type,
    sizeVarBytes: header.sizeVarBytes.slice(),
    headerLen: header.headerLen,
    baseOid: header.baseOid,
    baseRel: header.baseRel,
  };
}

/**
 * Read and memoize a duplicate candidate header.
 *
 * The rewrite pass already reads the live row header once. This memo is only
 * for duplicate-run scans that would otherwise reread the same pack entry
 * header multiple times while deciding which OID owner should survive.
 */
async function readDuplicateCandidateHeader(
  cache: DuplicateHeaderCache,
  stats: SelectionStats,
  key: number,
  readState: PackReadState,
  offset: number
): Promise<PackHeaderEx | null> {
  const cached = cache.get(key);
  if (cached !== undefined) return cached;

  stats.duplicateHeaderProbes++;
  const header = await readSelectedHeader(readState, offset);
  const cloned = header ? clonePackHeader(header) : null;
  cache.set(key, cloned);
  return cloned;
}

export async function claimCanonicalOwner(
  table: SelectionTable,
  sel: number,
  snapshot: OrderedPackSnapshot,
  readerStates: Map<number, PackReadState>,
  dedupMap: Map<number, number>,
  oidOwners: SelectedOidLookup,
  duplicateHeaderCache: DuplicateHeaderCache,
  stats: SelectionStats,
  env: Env,
  log: Logger,
  warnedFlags: Set<string>,
  options?: RewriteOptions
): Promise<ClaimOwnerResult> {
  // The rewrite output still prefers a single live row per OID, but OFS-pinned
  // rows may need to reclaim ownership so pack-local base chains stay valid.
  // Resolve that ownership before wiring any new base edges.
  const oidStart = sel * 20;
  const ownerSel = findSelectedOidOwner(oidOwners, table.oidsRaw, oidStart);
  if (ownerSel >= 0) {
    const currentKey = selectionKey(table.packSlots[sel]!, table.entryIndices[sel]!);
    if (ownerSel === sel) {
      return { kind: "unchanged", canonicalized: false };
    }

    if (table.ofsPinned[sel] && table.typeCodes[sel] >= 6 && !table.ofsPinned[ownerSel]) {
      // OFS_DELTA children depend on the exact pack-local base position, not
      // just the resulting OID. Let that exact row take ownership back so the
      // later topology sort still sees the original acyclic within-pack chain.
      stats.duplicateOfsOwnerTakeovers++;
      setSelectedOidOwner(oidOwners, table.oidsRaw, oidStart, sel);
      return {
        kind: "takeover",
        canonicalized: false,
        previousOwnerSel: ownerSel,
      };
    }

    // Keep future exact pack-position lookups collapsed onto the current live
    // owner. Without this, later base resolution can recreate an extra row for
    // the same pack entry after ownership already converged.
    dedupMap.set(currentKey, ownerSel);

    if (
      !table.ofsPinned[ownerSel] &&
      table.typeCodes[sel]! < 6 &&
      table.typeCodes[ownerSel]! >= 6 &&
      upgradeOwnerSelectionToFull(table, ownerSel, sel)
    ) {
      // Preserve the existing owner slot when a later full-object duplicate
      // arrives. This upgrades the owner in place and keeps any already-wired
      // `baseSlots` stable.
      stats.duplicateOwnerUpgrades++;
      setSelectedOidOwner(oidOwners, table.oidsRaw, oidStart, ownerSel);
      return {
        kind: "redirect",
        canonicalized: true,
        targetSel: ownerSel,
        upgradedOwner: true,
      };
    }

    return {
      kind: "redirect",
      canonicalized: false,
      targetSel: ownerSel,
      upgradedOwner: false,
    };
  }

  if (!table.ofsPinned[sel] && (table.typeCodes[sel] === 6 || table.typeCodes[sel] === 7)) {
    // Delta rows get one extra chance to become the canonical owner by
    // locating a full-object duplicate anywhere in the snapshot.
    const candidate = await tryCanonicalizeDeltaSelectionToFull(
      table,
      sel,
      snapshot,
      readerStates,
      dedupMap,
      duplicateHeaderCache,
      stats,
      env,
      log,
      warnedFlags,
      options
    );
    if (candidate.kind === "redirect") {
      // The best full-object duplicate is already selected elsewhere. Publish
      // that row as the owner immediately so later duplicates short-circuit.
      setSelectedOidOwner(oidOwners, table.oidsRaw, oidStart, candidate.targetSel);
      return {
        kind: "redirect",
        canonicalized: true,
        targetSel: candidate.targetSel,
        upgradedOwner: false,
      };
    }
    if (candidate.kind === "swap") {
      // No selected full owner exists yet, so mutate the current row in place
      // to the chosen full-object duplicate and keep this same `sel` alive.
      const altPack = snapshot.packs[candidate.packSlot]!;
      setSelectionEntryIdentity(table, sel, candidate.packSlot, candidate.entryIndex, altPack.idx);
      if (
        !storeSelectionHeader(table, sel, candidate.offset, candidate.nextOffset, candidate.header)
      ) {
        log.warn("rewrite:invalid-payload-length", {
          packKey: altPack.packKey,
          offset: candidate.offset,
        });
        return { kind: "error" };
      }
      table.baseSlots[sel] = -1;
      setSelectedOidOwner(oidOwners, table.oidsRaw, oidStart, sel);
      return { kind: "swapped" };
    }
  }

  setSelectedOidOwner(oidOwners, table.oidsRaw, oidStart, sel);
  return { kind: "unchanged", canonicalized: false };
}

function upgradeOwnerSelectionToFull(
  table: SelectionTable,
  ownerSel: number,
  fullSel: number
): boolean {
  // Preserve the existing owner slot so previously wired `baseSlots` keep
  // pointing at the same `sel`. Only the owner's pack position and header
  // fields change to the newly observed full-object duplicate.
  if (table.typeCodes[fullSel] >= 6 || table.typeCodes[ownerSel] < 6) return false;

  // Keep the owner slot's pin state intact: children may already require this
  // exact selection slot to stay the canonical OFS-stable base.
  copySelectionRow(table, ownerSel, fullSel, { preserveTargetOfsPinned: true });
  table.baseSlots[ownerSel] = -1;
  return true;
}

async function tryCanonicalizeDeltaSelectionToFull(
  table: SelectionTable,
  sel: number,
  snapshot: OrderedPackSnapshot,
  readerStates: Map<number, PackReadState>,
  dedupMap: Map<number, number>,
  duplicateHeaderCache: DuplicateHeaderCache,
  stats: SelectionStats,
  env: Env,
  log: Logger,
  warnedFlags: Set<string>,
  options?: RewriteOptions
): Promise<FullDuplicateCandidate> {
  const packSlot = table.packSlots[sel]!;
  const entryIndex = table.entryIndices[sel]!;
  const currentKey = selectionKey(packSlot, entryIndex);
  const rawBytes = table.oidsRaw.subarray(sel * 20, sel * 20 + 20);

  // Scan duplicate runs in caller-provided snapshot order so tie-breaking
  // stays deterministic. The first full-object duplicate wins; if it is
  // already selected, redirect to that row instead of creating a second owner.
  for (const candidate of collectPackedObjectCandidates(snapshot.packs, rawBytes)) {
    if (candidate.packSlot === packSlot && candidate.objectIndex === entryIndex) continue;

    const altPack = snapshot.packs[candidate.packSlot]!;
    const altReadState = await ensurePackReadState(
      env,
      altPack,
      candidate.packSlot,
      readerStates,
      log,
      warnedFlags,
      options
    );
    const altKey = selectionKey(candidate.packSlot, candidate.objectIndex);
    const altHeader = await readDuplicateCandidateHeader(
      duplicateHeaderCache,
      stats,
      altKey,
      altReadState,
      candidate.offset
    );
    if (!altHeader || altHeader.type === 6 || altHeader.type === 7) continue;

    const existingOwner = dedupMap.get(altKey);
    if (existingOwner !== undefined && existingOwner !== sel) {
      // Keep the original position key pointing at the existing owner so any
      // OFS bases already resolved against this slot continue to hit the
      // canonical full-object selection after dead-slot compaction.
      dedupMap.set(currentKey, existingOwner);
      return { kind: "redirect", targetSel: existingOwner };
    }

    // Replace the selected delta in-place with the verified full-object
    // duplicate. The original position key intentionally keeps pointing at
    // `sel` so future OFS lookups collapse onto the same live owner slot.
    dedupMap.set(altKey, sel);
    return {
      kind: "swap",
      packSlot: candidate.packSlot,
      entryIndex: candidate.objectIndex,
      offset: candidate.offset,
      nextOffset: candidate.nextOffset,
      header: altHeader,
    };
  }

  return { kind: "none" };
}
