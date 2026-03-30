import type { PackEntryTable, ScanResult } from "../types.ts";
import { getRefBaseOidAt } from "../types.ts";

import { enqueueReadyDeferred, extendDeadlineChain } from "./dependencies.ts";

export interface RefBaseLookup {
  mask: number;
  used: Uint8Array;
  keys: Uint8Array;
  resolvedEntries: Int32Array;
  waitHeadBySlot: Int32Array;
  nextWait: Int32Array;
  entrySlots: Int32Array;
}

function nextPowerOfTwo(value: number): number {
  let out = 1;
  while (out < value) out <<= 1;
  return out;
}

function hashOid(oid: Uint8Array): number {
  let hash = 2166136261;
  for (let i = 0; i < 20; i++) {
    hash ^= oid[i];
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function slotKeyEquals(keys: Uint8Array, slot: number, oid: Uint8Array): boolean {
  const start = slot * 20;
  for (let i = 0; i < 20; i++) {
    if (keys[start + i] !== oid[i]) return false;
  }
  return true;
}

function findOrCreateSlot(lookup: RefBaseLookup, oid: Uint8Array): number {
  let slot = hashOid(oid) & lookup.mask;
  while (lookup.used[slot]) {
    if (slotKeyEquals(lookup.keys, slot, oid)) return slot;
    slot = (slot + 1) & lookup.mask;
  }
  lookup.used[slot] = 1;
  lookup.keys.set(oid, slot * 20);
  return slot;
}

function findExistingSlot(lookup: RefBaseLookup, oid: Uint8Array): number {
  let slot = hashOid(oid) & lookup.mask;
  while (lookup.used[slot]) {
    if (slotKeyEquals(lookup.keys, slot, oid)) return slot;
    slot = (slot + 1) & lookup.mask;
  }
  return -1;
}

export function createRefBaseLookup(scanResult: ScanResult): RefBaseLookup | null {
  if (scanResult.refDeltaCount === 0) return null;

  const slotCount = nextPowerOfTwo(Math.max(4, scanResult.refDeltaCount * 2));
  const lookup: RefBaseLookup = {
    mask: slotCount - 1,
    used: new Uint8Array(slotCount),
    keys: new Uint8Array(slotCount * 20),
    resolvedEntries: new Int32Array(slotCount).fill(-1),
    waitHeadBySlot: new Int32Array(slotCount).fill(-1),
    nextWait: new Int32Array(scanResult.objectCount).fill(-1),
    entrySlots: new Int32Array(scanResult.objectCount).fill(-1),
  };

  for (let i = 0; i < scanResult.objectCount; i++) {
    if (scanResult.table.types[i] !== 7) continue;
    lookup.entrySlots[i] = findOrCreateSlot(lookup, getRefBaseOidAt(scanResult.refBaseOids, i));
  }

  return lookup;
}

export function noteResolvedEntry(
  lookup: RefBaseLookup,
  oidBuffer: Uint8Array,
  entryIndex: number
): void {
  const start = entryIndex * 20;
  const slot = findExistingSlot(lookup, oidBuffer.subarray(start, start + 20));
  if (slot >= 0) lookup.resolvedEntries[slot] = entryIndex;
}

export function getResolvedBaseEntry(lookup: RefBaseLookup, entryIndex: number): number {
  const slot = lookup.entrySlots[entryIndex];
  return slot < 0 ? -1 : lookup.resolvedEntries[slot];
}

export function enqueueWaitingRefDelta(lookup: RefBaseLookup, entryIndex: number): void {
  const slot = lookup.entrySlots[entryIndex];
  if (slot < 0) return;
  lookup.nextWait[entryIndex] = lookup.waitHeadBySlot[slot];
  lookup.waitHeadBySlot[slot] = entryIndex;
}

export function promoteWaitingRefDeltas(
  lookup: RefBaseLookup,
  resolvedIndex: number,
  table: PackEntryTable,
  baseIndexArr: Int32Array,
  isBaseArr: Uint8Array,
  deadlines: Uint32Array,
  readyDeferred?: number[],
  deferredQueued?: Uint8Array
): void {
  const slot = findExistingSlot(
    lookup,
    table.oids.subarray(resolvedIndex * 20, resolvedIndex * 20 + 20)
  );
  if (slot < 0) return;

  lookup.resolvedEntries[slot] = resolvedIndex;
  let waiter = lookup.waitHeadBySlot[slot];
  if (waiter < 0) return;

  lookup.waitHeadBySlot[slot] = -1;
  isBaseArr[resolvedIndex] = 1;
  while (waiter >= 0) {
    const next = lookup.nextWait[waiter];
    lookup.nextWait[waiter] = -1;
    if (baseIndexArr[waiter] < 0) {
      baseIndexArr[waiter] = resolvedIndex;
      extendDeadlineChain(
        deadlines,
        baseIndexArr,
        resolvedIndex,
        Math.max(table.offsets[waiter], deadlines[waiter])
      );
      if (readyDeferred && deferredQueued) {
        enqueueReadyDeferred(readyDeferred, deferredQueued, table, baseIndexArr, waiter);
      }
    }
    waiter = next;
  }
}
