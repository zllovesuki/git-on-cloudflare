import type { OrderedPackSnapshot } from "@/git/operations/fetch/types.ts";
import type { IdxView, PackedObjectResult } from "@/git/object-store/types.ts";
import type { Logger } from "@/common/logger.ts";
import type { PackHeaderEx } from "../packMeta.ts";

import { bytesToHex, deflate, hexToBytes } from "@/common/index.ts";
import { encodeObjHeader, objTypeCode } from "@/git/core/objects.ts";
import {
  collectPackedObjectCandidates,
  findOffsetIndex,
  findOidRunInIdx,
} from "@/git/object-store/index.ts";
import { materializePackedObjectCandidate } from "@/git/object-store/materialize.ts";
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
  recordRewriteFailure,
  selectionDependsOn,
  selectionKey,
  setSelectionEntryIdentity,
  storeSelectionHeader,
  type PackReadState,
  type RewriteOptions,
  type SelectionTable,
} from "./shared.ts";

const SYNTHETIC_OBJECT_MAX_BYTES = 8 * 1024 * 1024;
const SYNTHETIC_PAYLOAD_TOTAL_MAX_BYTES = 32 * 1024 * 1024;

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

type RefDeltaBaseChoice = {
  baseSel: number;
  candidateCount: number;
};

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
    return await resolveDeltaBaseAndFinish(ownership.previousOwnerSel);
  }
  if (ownership.kind === "swapped") {
    log.debug("rewrite:delta-canonicalized-to-full", {
      fromPackKey: pack.packKey,
      offset,
    });
    return { ok: true, ofsBaseCanonicalized: true };
  }
  canonicalized = ownership.canonicalized;

  return await resolveDeltaBaseAndFinish();

  async function resolveDeltaBaseAndFinish(supersedeSel?: number): Promise<HeaderResolveResult> {
    if (
      !(await resolveDeltaBaseFromHeader(
        table,
        sel,
        snapshot,
        dedupMap,
        secondaryQueue,
        env,
        log,
        resolvedHeader,
        options
      ))
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

export async function resolveDeltaBaseFromHeader(
  table: SelectionTable,
  sel: number,
  snapshot: OrderedPackSnapshot,
  dedupMap: Map<number, number>,
  secondaryQueue: number[],
  env: Env,
  log: Logger,
  header: PackHeaderEx,
  options?: RewriteOptions
): Promise<boolean> {
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
      recordRewriteFailure(options, {
        reason: "missing-ofs-base",
        retryable: false,
        details: { packKey: pack.packKey, offset, baseOffset },
      });
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
      recordRewriteFailure(options, {
        reason: "self-referential-ofs-delta",
        retryable: false,
        details: { packKey: pack.packKey, offset, baseRel: header.baseRel },
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

    const choice = chooseRefDeltaBase(
      table,
      sel,
      snapshot,
      dedupMap,
      log,
      rawBytes,
      header.baseOid
    );
    if (choice.baseSel < 0) {
      const reason = choice.candidateCount === 0 ? "missing-ref-base" : "ref-base-cycle-unresolved";
      log.warn(`rewrite:${reason}`, {
        sel,
        packKey: pack.packKey,
        offset,
        baseOid: header.baseOid,
        candidateCount: choice.candidateCount,
      });
      return await materializeSelectionAsFullObject({
        table,
        sel,
        snapshot,
        env,
        log,
        options,
        reason,
        baseOid: header.baseOid,
        candidateCount: choice.candidateCount,
      });
    }

    const baseSel = choice.baseSel;
    if (baseSel === sel) {
      // Self-referential REF_DELTA with no full-object duplicate available.
      // This pack cannot be rewritten into a topologically valid output.
      log.warn("rewrite:self-referential-delta", {
        packKey: pack.packKey,
        offset,
        deltaType: "ref",
        baseOid: header.baseOid,
      });
      return await materializeSelectionAsFullObject({
        table,
        sel,
        snapshot,
        env,
        log,
        options,
        reason: "self-referential-ref-delta",
        baseOid: header.baseOid,
        candidateCount: choice.candidateCount,
      });
    }

    table.baseSlots[sel] = baseSel;
    if (table.typeCodes[baseSel] === 0 && !table.queuedForHeader[baseSel]) {
      table.queuedForHeader[baseSel] = 1;
      secondaryQueue.push(baseSel);
    }
  }

  return true;
}

type MaterializeSelectionArgs = {
  table: SelectionTable;
  sel: number;
  snapshot: OrderedPackSnapshot;
  env: Env;
  log: Logger;
  options?: RewriteOptions;
  reason: string;
  baseOid: string;
  candidateCount: number;
};

type MaterializeOidArgs = {
  snapshot: OrderedPackSnapshot;
  env: Env;
  log: Logger;
  options?: RewriteOptions;
  oid: string | Uint8Array;
  visited: Set<string>;
};

function selectedOidHex(table: SelectionTable, sel: number): string {
  return bytesToHex(table.oidsRaw.subarray(sel * 20, sel * 20 + 20));
}

function syntheticPayloadTotal(table: SelectionTable): number {
  let total = 0;
  for (let sel = 0; sel < table.syntheticPayloads.length; sel++) {
    total += table.syntheticPayloads[sel]?.byteLength ?? 0;
  }
  return total;
}

async function materializeOidFromSnapshot(
  args: MaterializeOidArgs
): Promise<PackedObjectResult | undefined> {
  const limiter = args.options?.limiter;
  const countSubrequest = args.options?.countSubrequest;
  if (!limiter || !countSubrequest) return undefined;

  const candidates = collectPackedObjectCandidates(args.snapshot.packs, args.oid);
  for (const candidate of candidates) {
    if (args.options?.signal?.aborted) return undefined;

    const object = await materializePackedObjectCandidate({
      env: args.env,
      candidate,
      limiter,
      countSubrequest,
      log: args.log,
      cyclePolicy: "miss",
      resolveRefBase: async (baseOid, nextVisited) => {
        return await materializeOidFromSnapshot({
          ...args,
          oid: baseOid,
          visited: nextVisited,
        });
      },
      visited: args.visited,
      signal: args.options?.signal,
    });
    if (object) return object;
  }

  return undefined;
}

async function materializeSelectionAsFullObject(args: MaterializeSelectionArgs): Promise<boolean> {
  const oid = selectedOidHex(args.table, args.sel);
  const object = await materializeOidFromSnapshot({
    snapshot: args.snapshot,
    env: args.env,
    log: args.log,
    options: args.options,
    oid,
    visited: new Set<string>(),
  });
  if (!object) {
    args.log.warn("rewrite:cycle-breaker-materialize-miss", {
      sel: args.sel,
      oid,
      reason: args.reason,
      baseOid: args.baseOid,
      candidateCount: args.candidateCount,
    });
    recordRewriteFailure(args.options, {
      reason: "cycle-breaker-materialize-miss",
      retryable: false,
      details: {
        sel: args.sel,
        oid,
        baseOid: args.baseOid,
        candidateCount: args.candidateCount,
      },
    });
    return false;
  }

  if (object.oid !== oid) {
    args.log.warn("rewrite:cycle-breaker-oid-mismatch", {
      sel: args.sel,
      oid,
      materializedOid: object.oid,
    });
    recordRewriteFailure(args.options, {
      reason: "cycle-breaker-oid-mismatch",
      retryable: false,
      details: { sel: args.sel, oid, materializedOid: object.oid },
    });
    return false;
  }

  if (object.payload.byteLength > SYNTHETIC_OBJECT_MAX_BYTES) {
    args.log.warn("rewrite:cycle-breaker-object-too-large", {
      sel: args.sel,
      oid,
      payloadBytes: object.payload.byteLength,
      maxBytes: SYNTHETIC_OBJECT_MAX_BYTES,
    });
    recordRewriteFailure(args.options, {
      reason: "synthetic-object-too-large",
      retryable: false,
      details: {
        sel: args.sel,
        oid,
        payloadBytes: object.payload.byteLength,
        maxBytes: SYNTHETIC_OBJECT_MAX_BYTES,
      },
    });
    return false;
  }

  const compressedPayload = await deflate(object.payload);
  const existingPayloadBytes = args.table.syntheticPayloads[args.sel]?.byteLength ?? 0;
  const nextSyntheticTotal =
    syntheticPayloadTotal(args.table) - existingPayloadBytes + compressedPayload.byteLength;
  if (nextSyntheticTotal > SYNTHETIC_PAYLOAD_TOTAL_MAX_BYTES) {
    args.log.warn("rewrite:cycle-breaker-total-too-large", {
      sel: args.sel,
      oid,
      compressedBytes: compressedPayload.byteLength,
      totalBytes: nextSyntheticTotal,
      maxBytes: SYNTHETIC_PAYLOAD_TOTAL_MAX_BYTES,
    });
    recordRewriteFailure(args.options, {
      reason: "synthetic-payload-budget-exceeded",
      retryable: false,
      details: {
        sel: args.sel,
        oid,
        compressedBytes: compressedPayload.byteLength,
        totalBytes: nextSyntheticTotal,
        maxBytes: SYNTHETIC_PAYLOAD_TOTAL_MAX_BYTES,
      },
    });
    return false;
  }

  const typeCode = objTypeCode(object.type);
  const headerBytes = encodeObjHeader(typeCode, object.payload.byteLength);
  if (headerBytes.byteLength > 5) {
    recordRewriteFailure(args.options, {
      reason: "synthetic-header-too-large",
      retryable: false,
      details: { sel: args.sel, oid, headerBytes: headerBytes.byteLength },
    });
    return false;
  }

  args.table.typeCodes[args.sel] = typeCode;
  args.table.headerLens[args.sel] = headerBytes.byteLength;
  args.table.payloadLens[args.sel] = compressedPayload.byteLength;
  args.table.sizeVarBuf.set(headerBytes, args.sel * 5);
  args.table.sizeVarLens[args.sel] = headerBytes.byteLength;
  args.table.baseSlots[args.sel] = -1;
  args.table.queuedForHeader[args.sel] = 0;
  args.table.syntheticPayloads[args.sel] = compressedPayload;

  args.log.info("rewrite:cycle-breaker-materialized", {
    sel: args.sel,
    oid,
    type: object.type,
    reason: args.reason,
    baseOid: args.baseOid,
    candidateCount: args.candidateCount,
    payloadBytes: object.payload.byteLength,
    compressedBytes: compressedPayload.byteLength,
    syntheticTotalBytes: nextSyntheticTotal,
  });
  return true;
}

/**
 * Choose a REF_DELTA base by scanning duplicate OID runs in snapshot order.
 *
 * The hot path only uses the already-loaded idx views and the partially wired
 * selection table. If a duplicate candidate is already selected, adding
 * `sel -> candidateSel` must not make the selected base chain cycle back into
 * `sel`; otherwise the chooser falls through to the next duplicate candidate.
 */
function chooseRefDeltaBase(
  table: SelectionTable,
  sel: number,
  snapshot: OrderedPackSnapshot,
  dedupMap: Map<number, number>,
  log: Logger,
  rawBytes: Uint8Array,
  baseOid: string
): RefDeltaBaseChoice {
  const currentPackSlot = table.packSlots[sel];
  const currentEntryIndex = table.entryIndices[sel];
  let candidateCount = 0;

  for (let candidatePackSlot = 0; candidatePackSlot < snapshot.packs.length; candidatePackSlot++) {
    const candidatePack = snapshot.packs[candidatePackSlot]!;
    const run = findOidRunInIdx(candidatePack.idx, rawBytes);
    if (!run) continue;

    for (let entryIndex = run.startIndex; entryIndex <= run.endIndex; entryIndex++) {
      candidateCount++;
      if (candidatePackSlot === currentPackSlot && entryIndex === currentEntryIndex) {
        continue;
      }

      const candidateKey = selectionKey(candidatePackSlot, entryIndex);
      const candidateSel = dedupMap.get(candidateKey);
      if (candidateSel !== undefined) {
        if (candidateSel === sel || selectionDependsOn(table, candidateSel, sel)) {
          log.debug("rewrite:ref-base-candidate-cycle-skipped", {
            sel,
            baseOid,
            candidatePackSlot,
            candidateEntryIndex: entryIndex,
            selectedCandidateSel: candidateSel,
          });
          continue;
        }
        return { baseSel: candidateSel, candidateCount };
      }

      const baseSel = addEntry(table, dedupMap, candidatePackSlot, entryIndex, candidatePack.idx);
      return { baseSel, candidateCount };
    }
  }

  return { baseSel: -1, candidateCount };
}
