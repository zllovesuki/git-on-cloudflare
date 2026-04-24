import type { IdxView } from "./types.ts";

import { bytesToHex, hexToBytes } from "@/common/hex.ts";
import { findOidIndexFromBytes, getNextOffsetByIndex, getOidHexAt } from "./idxView.ts";

export type IndexedPackSource = {
  packKey: string;
  packBytes: number;
  idx: IdxView;
};

export type PackedObjectCandidate = {
  source: IndexedPackSource;
  packSlot: number;
  objectIndex: number;
  offset: number;
  nextOffset: number;
  oid: string;
};

export type OidRun = {
  /** Inclusive first `.idx` row whose OID matches the lookup OID. */
  startIndex: number;
  /** Inclusive final `.idx` row whose OID matches the lookup OID. */
  endIndex: number;
};

export type CandidateCollectionOptions = {
  excludePackKey?: string;
};

type NormalizedOid = {
  oidHex: string;
  oidBytes: Uint8Array;
};

function normalizeOid(oid: string | Uint8Array): NormalizedOid | undefined {
  if (typeof oid === "string") {
    const oidHex = oid.toLowerCase();
    if (oidHex.length !== 40) return undefined;
    return { oidHex, oidBytes: hexToBytes(oidHex) };
  }

  if (oid.byteLength !== 20) return undefined;
  return { oidHex: bytesToHex(oid), oidBytes: oid };
}

function oidAtIndexMatches(rawNames: Uint8Array, index: number, oidBytes: Uint8Array): boolean {
  const rawStart = index * 20;
  for (let offset = 0; offset < 20; offset++) {
    if (rawNames[rawStart + offset] !== oidBytes[offset]) return false;
  }
  return true;
}

function candidateFromIndex(
  source: IndexedPackSource,
  packSlot: number,
  objectIndex: number,
  oidHex: string
): PackedObjectCandidate | undefined {
  const nextOffset = getNextOffsetByIndex(source.idx, objectIndex);
  if (nextOffset === undefined) return undefined;

  return {
    source,
    packSlot,
    objectIndex,
    offset: source.idx.offsets[objectIndex],
    nextOffset,
    oid: oidHex,
  };
}

/**
 * Locate the full contiguous duplicate-OID run in an idx view.
 *
 * Git idx files are sorted by object ID, so duplicate rows for the same OID
 * are adjacent. The binary-search hit can land anywhere inside that run; this
 * helper expands both directions so callers do not accidentally inspect only
 * one arbitrary duplicate.
 */
export function findOidRunInIdx(idx: IdxView, oid: string | Uint8Array): OidRun | undefined {
  const normalized = normalizeOid(oid);
  if (!normalized) return undefined;

  const hitIndex = findOidIndexFromBytes(idx, normalized.oidBytes);
  if (hitIndex < 0) return undefined;

  let startIndex = hitIndex;
  while (startIndex > 0 && oidAtIndexMatches(idx.rawNames, startIndex - 1, normalized.oidBytes)) {
    startIndex--;
  }

  let endIndex = hitIndex;
  while (
    endIndex + 1 < idx.count &&
    oidAtIndexMatches(idx.rawNames, endIndex + 1, normalized.oidBytes)
  ) {
    endIndex++;
  }

  return { startIndex, endIndex };
}

/**
 * Enumerate every packed-object candidate for an OID in snapshot order.
 *
 * Within a pack this returns the entire duplicate run in idx row order. The
 * caller owns higher-level policy such as whether to accept the first material
 * object, prefer full objects, or skip a target pack during refs-only backfill.
 */
export function collectPackedObjectCandidates(
  sources: readonly IndexedPackSource[],
  oid: string | Uint8Array,
  options: CandidateCollectionOptions = {}
): PackedObjectCandidate[] {
  const normalized = normalizeOid(oid);
  if (!normalized) return [];

  const candidates: PackedObjectCandidate[] = [];
  for (let packSlot = 0; packSlot < sources.length; packSlot++) {
    const source = sources[packSlot]!;
    if (source.packKey === options.excludePackKey) continue;

    const run = findOidRunInIdx(source.idx, normalized.oidBytes);
    if (!run) continue;

    for (let objectIndex = run.startIndex; objectIndex <= run.endIndex; objectIndex++) {
      const candidate = candidateFromIndex(source, packSlot, objectIndex, normalized.oidHex);
      if (candidate) candidates.push(candidate);
    }
  }

  return candidates;
}

/**
 * Find the first snapshot pack that contains an OID, preserving the existing
 * binary-search hit semantics inside that pack.
 */
export function findFirstPackedObjectCandidate(
  sources: readonly IndexedPackSource[],
  oid: string | Uint8Array,
  options: CandidateCollectionOptions = {}
): PackedObjectCandidate | undefined {
  const normalized = normalizeOid(oid);
  if (!normalized) return undefined;

  for (let packSlot = 0; packSlot < sources.length; packSlot++) {
    const source = sources[packSlot]!;
    if (source.packKey === options.excludePackKey) continue;

    const objectIndex = findOidIndexFromBytes(source.idx, normalized.oidBytes);
    if (objectIndex < 0) continue;

    return candidateFromIndex(source, packSlot, objectIndex, getOidHexAt(source.idx, objectIndex));
  }

  return undefined;
}
