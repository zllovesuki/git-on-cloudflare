import type { PackRefSnapshotEntry } from "@/git/pack/refIndex.ts";

import { bytesToHex, createLogger, hexToBytes, isValidOid } from "@/common/index.ts";
import { findOidIndexFromBytes } from "@/git/object-store/index.ts";
import {
  getPackRefRawRefAt,
  getPackRefTypeCode,
  visitPackRefRawRefsAt,
} from "@/git/pack/refIndex.ts";

const HAVE_CAP = 128;
const MAINLINE_ENRICHMENT_BUDGET = 20;
const CLOSURE_TIMEOUT_MS = 49_000;
const MISSING_REF_CAP = 1024;
const OID_BYTES = 20;

export type RefClosureStats = {
  indexedObjects: number;
  queued: number;
  seen: number;
  needed: number;
  missing: number;
  edgeVisits: number;
  duplicateQueueSkips: number;
};

export type RefClosureResult =
  | {
      type: "Ready";
      neededOids: string[];
      ackOids: string[];
      stats: RefClosureStats;
    }
  | {
      type: "BudgetExceeded";
      neededOids: string[];
      ackOids: string[];
      reason: "timeout" | "missing-ref-budget";
      stats: RefClosureStats;
    };

type ClosureIndex = {
  packBaseOrdinals: Uint32Array;
  objectCount: number;
};

type LocatedObject = {
  packSlot: number;
  oidIndex: number;
  ordinal: number;
};

type CommonHave = {
  oid: string;
  located: LocatedObject;
};

type LocatedObjectQueue = {
  packSlots: Uint32Array;
  oidIndices: Uint32Array;
  cursor: number;
  count: number;
};

// Mainline enrichment is intentionally tiny, so raw OIDs keep that side walk
// simple without affecting the bounded final closure queue below.
type RawOidQueue = {
  rawOids: Uint8Array;
  cursor: number;
  count: number;
};

function buildClosureIndex(packs: PackRefSnapshotEntry[]): ClosureIndex {
  const packBaseOrdinals = new Uint32Array(packs.length + 1);
  let objectCount = 0;

  for (let packSlot = 0; packSlot < packs.length; packSlot++) {
    packBaseOrdinals[packSlot] = objectCount;
    objectCount += packs[packSlot]!.idx.count;
  }
  packBaseOrdinals[packs.length] = objectCount;

  return { packBaseOrdinals, objectCount };
}

function locateObject(
  packs: PackRefSnapshotEntry[],
  closureIndex: ClosureIndex,
  rawOid: Uint8Array,
  rawOidStart: number
): LocatedObject | undefined {
  for (let packSlot = 0; packSlot < packs.length; packSlot++) {
    const oidIndex = findOidIndexFromBytes(packs[packSlot]!.idx, rawOid, rawOidStart);
    if (oidIndex < 0) continue;
    return {
      packSlot,
      oidIndex,
      ordinal: closureIndex.packBaseOrdinals[packSlot]! + oidIndex,
    };
  }
  return undefined;
}

function createLocatedObjectQueue(initialEntries: number): LocatedObjectQueue {
  const initialCapacity = Math.max(initialEntries, 16);
  return {
    packSlots: new Uint32Array(initialCapacity),
    oidIndices: new Uint32Array(initialCapacity),
    cursor: 0,
    count: 0,
  };
}

function ensureLocatedObjectQueueCapacity(queue: LocatedObjectQueue, nextCount: number): void {
  if (nextCount <= queue.packSlots.length) return;

  let nextCapacity = queue.packSlots.length;
  while (nextCapacity < nextCount) nextCapacity *= 2;

  const nextPackSlots = new Uint32Array(nextCapacity);
  const nextOidIndices = new Uint32Array(nextCapacity);
  nextPackSlots.set(queue.packSlots);
  nextOidIndices.set(queue.oidIndices);
  queue.packSlots = nextPackSlots;
  queue.oidIndices = nextOidIndices;
}

function pushLocatedObject(queue: LocatedObjectQueue, located: LocatedObject): void {
  ensureLocatedObjectQueueCapacity(queue, queue.count + 1);
  queue.packSlots[queue.count] = located.packSlot;
  queue.oidIndices[queue.count] = located.oidIndex;
  queue.count++;
}

function createRawOidQueue(initialEntries: number): RawOidQueue {
  return {
    rawOids: new Uint8Array(Math.max(initialEntries, 16) * OID_BYTES),
    cursor: 0,
    count: 0,
  };
}

function ensureRawOidQueueCapacity(queue: RawOidQueue, nextCount: number): void {
  if (nextCount * OID_BYTES <= queue.rawOids.byteLength) return;

  let nextCapacity = queue.rawOids.byteLength / OID_BYTES;
  while (nextCapacity < nextCount) nextCapacity *= 2;

  const nextRawOids = new Uint8Array(nextCapacity * OID_BYTES);
  nextRawOids.set(queue.rawOids);
  queue.rawOids = nextRawOids;
}

function pushRawOid(queue: RawOidQueue, rawOid: Uint8Array, rawOidStart: number): void {
  ensureRawOidQueueCapacity(queue, queue.count + 1);
  queue.rawOids.set(rawOid.subarray(rawOidStart, rawOidStart + OID_BYTES), queue.count * OID_BYTES);
  queue.count++;
}

function findCommonHavesInSnapshot(
  packs: PackRefSnapshotEntry[],
  closureIndex: ClosureIndex,
  haves: string[]
): CommonHave[] {
  const cappedHaves = haves.slice(0, HAVE_CAP);
  const found: CommonHave[] = [];
  const seenFlags = new Uint8Array(closureIndex.objectCount);

  for (const have of cappedHaves) {
    const oid = have.toLowerCase();
    if (!isValidOid(oid)) continue;

    const rawOid = hexToBytes(oid);
    const located = locateObject(packs, closureIndex, rawOid, 0);
    if (!located) continue;
    if (seenFlags[located.ordinal]) continue;

    seenFlags[located.ordinal] = 1;
    found.push({ oid, located });
  }

  return found;
}

function enqueueLocatedObject(
  queue: LocatedObjectQueue,
  queuedFlags: Uint8Array,
  located: LocatedObject,
  duplicateQueueSkips: { value: number }
): void {
  if (queuedFlags[located.ordinal]) {
    duplicateQueueSkips.value++;
    return;
  }

  queuedFlags[located.ordinal] = 1;
  pushLocatedObject(queue, located);
}

function recordMissingOid(args: {
  oid: string;
  missingSeen: Set<string>;
  missingNeeded: Set<string>;
  includeNeeded: boolean;
}): "recorded" | "duplicate" | "budget-exceeded" {
  if (args.missingSeen.has(args.oid)) return "duplicate";
  if (args.missingSeen.size >= MISSING_REF_CAP) return "budget-exceeded";

  args.missingSeen.add(args.oid);
  if (args.includeNeeded) {
    args.missingNeeded.add(args.oid);
  }
  return "recorded";
}

function addNeededObject(
  located: LocatedObject,
  neededFlags: Uint8Array,
  neededPackSlots: { values: Uint32Array },
  neededOidIndices: { values: Uint32Array },
  neededCountRef: { value: number }
): void {
  if (neededFlags[located.ordinal]) return;
  neededFlags[located.ordinal] = 1;

  if (neededCountRef.value >= neededPackSlots.values.length) {
    const nextLength = Math.max(neededPackSlots.values.length * 2, 16);
    const nextPackSlots = new Uint32Array(nextLength);
    const nextOidIndices = new Uint32Array(nextLength);
    nextPackSlots.set(neededPackSlots.values);
    nextOidIndices.set(neededOidIndices.values);
    neededPackSlots.values = nextPackSlots;
    neededOidIndices.values = nextOidIndices;
  }

  neededPackSlots.values[neededCountRef.value] = located.packSlot;
  neededOidIndices.values[neededCountRef.value] = located.oidIndex;
  neededCountRef.value++;
}

function buildNeededOids(args: {
  packs: PackRefSnapshotEntry[];
  neededPackSlots: Uint32Array;
  neededOidIndices: Uint32Array;
  neededCount: number;
  missingNeeded: Set<string>;
}): string[] {
  const neededOids: string[] = [];
  for (let index = 0; index < args.neededCount; index++) {
    const packSlot = args.neededPackSlots[index]!;
    const oidIndex = args.neededOidIndices[index]!;
    const rawNames = args.packs[packSlot]!.idx.rawNames;
    const oidStart = oidIndex * OID_BYTES;
    neededOids.push(bytesToHex(rawNames.subarray(oidStart, oidStart + OID_BYTES)));
  }

  for (const oid of args.missingNeeded) {
    neededOids.push(oid);
  }
  return neededOids;
}

function buildRefClosureStats(args: {
  closureIndex: ClosureIndex;
  queue: LocatedObjectQueue;
  seenCount: number;
  neededCount: number;
  missingSeen: Set<string>;
  missingNeeded: Set<string>;
  edgeVisits: number;
  duplicateQueueSkips: number;
}): RefClosureStats {
  return {
    indexedObjects: args.closureIndex.objectCount,
    queued: args.queue.count,
    seen: args.seenCount,
    needed: args.neededCount + args.missingNeeded.size,
    missing: args.missingSeen.size,
    edgeVisits: args.edgeVisits,
    duplicateQueueSkips: args.duplicateQueueSkips,
  };
}

export async function computeNeededFromPackRefs(args: {
  logLevel?: string;
  repoId: string;
  packs: PackRefSnapshotEntry[];
  wants: string[];
  haves: string[];
  onProgress?: (message: string) => void;
}): Promise<RefClosureResult> {
  const log = createLogger(args.logLevel, { service: "RefClosure", repoId: args.repoId });
  const startTime = Date.now();
  const closureIndex = buildClosureIndex(args.packs);
  const stopFlags = new Uint8Array(closureIndex.objectCount);
  const missingStop = new Set<string>();
  let stopCount = 0;

  args.onProgress?.("Finding common commits...\n");
  const commonHaves = findCommonHavesInSnapshot(args.packs, closureIndex, args.haves);
  const ackOids = commonHaves.map((have) => have.oid);
  for (const have of commonHaves) {
    if (stopFlags[have.located.ordinal]) continue;
    stopFlags[have.located.ordinal] = 1;
    stopCount++;
  }

  if (commonHaves.length > 0 && commonHaves.length < 10) {
    const mainlineQueue = createRawOidQueue(commonHaves.length + MAINLINE_ENRICHMENT_BUDGET);
    for (const have of commonHaves) {
      const rawNames = args.packs[have.located.packSlot]!.idx.rawNames;
      pushRawOid(mainlineQueue, rawNames, have.located.oidIndex * OID_BYTES);
    }

    let walked = 0;
    while (mainlineQueue.cursor < mainlineQueue.count && walked < MAINLINE_ENRICHMENT_BUDGET) {
      if (Date.now() - startTime > 2_000) break;

      const oidStart = mainlineQueue.cursor * OID_BYTES;
      mainlineQueue.cursor++;
      const located = locateObject(args.packs, closureIndex, mainlineQueue.rawOids, oidStart);
      if (!located) continue;

      // Commit sidecars store refs as [tree, first-parent, ...remaining-parents].
      if (getPackRefTypeCode(args.packs[located.packSlot]!.refs, located.oidIndex) !== 1) {
        continue;
      }

      const firstParent = getPackRefRawRefAt(
        args.packs[located.packSlot]!.refs,
        located.oidIndex,
        1
      );
      if (!firstParent) continue;

      const parentLocated = locateObject(args.packs, closureIndex, firstParent, 0);
      if (parentLocated) {
        if (stopFlags[parentLocated.ordinal]) continue;
        stopFlags[parentLocated.ordinal] = 1;
        stopCount++;
        pushRawOid(mainlineQueue, firstParent, 0);
        walked++;
        continue;
      }

      const parentOid = bytesToHex(firstParent);
      if (missingStop.has(parentOid)) continue;
      missingStop.add(parentOid);
      stopCount++;
      walked++;
    }

    log.debug("stream:plan:mainline-enriched", { stopSize: stopCount, walked });
  }

  args.onProgress?.("Selecting objects to send...\n");

  const seenFlags = new Uint8Array(closureIndex.objectCount);
  const queuedFlags = new Uint8Array(closureIndex.objectCount);
  const neededFlags = new Uint8Array(closureIndex.objectCount);
  const missingSeen = new Set<string>();
  const missingNeeded = new Set<string>();
  const queue = createLocatedObjectQueue(args.wants.length);
  const neededPackSlots = { values: new Uint32Array(Math.max(args.wants.length, 16)) };
  const neededOidIndices = { values: new Uint32Array(Math.max(args.wants.length, 16)) };
  const neededCount = { value: 0 };
  let seenCount = 0;
  let edgeVisits = 0;
  const duplicateQueueSkips = { value: 0 };

  const buildStats = () =>
    buildRefClosureStats({
      closureIndex,
      queue,
      seenCount,
      neededCount: neededCount.value,
      missingSeen,
      missingNeeded,
      edgeVisits,
      duplicateQueueSkips: duplicateQueueSkips.value,
    });

  const buildBudgetExceededResult = (
    reason: "timeout" | "missing-ref-budget"
  ): RefClosureResult => ({
    type: "BudgetExceeded",
    reason,
    neededOids: buildNeededOids({
      packs: args.packs,
      neededPackSlots: neededPackSlots.values,
      neededOidIndices: neededOidIndices.values,
      neededCount: neededCount.value,
      missingNeeded,
    }),
    ackOids,
    stats: buildStats(),
  });

  for (const want of args.wants) {
    const normalized = want.toLowerCase();
    if (!isValidOid(normalized)) {
      const result = recordMissingOid({
        oid: normalized,
        missingSeen,
        missingNeeded,
        includeNeeded: true,
      });
      if (result === "budget-exceeded") return buildBudgetExceededResult("missing-ref-budget");
      continue;
    }

    const rawOid = hexToBytes(normalized);
    const located = locateObject(args.packs, closureIndex, rawOid, 0);
    if (!located) {
      const result = recordMissingOid({
        oid: normalized,
        missingSeen,
        missingNeeded,
        includeNeeded: true,
      });
      if (result === "budget-exceeded") return buildBudgetExceededResult("missing-ref-budget");
      continue;
    }

    enqueueLocatedObject(queue, queuedFlags, located, duplicateQueueSkips);
  }

  log.info("stream:plan:closure-start", {
    wants: args.wants.length,
    haves: args.haves.length,
    ackOids: ackOids.length,
    indexedObjects: closureIndex.objectCount,
    stopSet: stopCount,
    queued: queue.count,
  });

  while (queue.cursor < queue.count) {
    if (Date.now() - startTime > CLOSURE_TIMEOUT_MS) {
      return buildBudgetExceededResult("timeout");
    }

    const packSlot = queue.packSlots[queue.cursor]!;
    const oidIndex = queue.oidIndices[queue.cursor]!;
    queue.cursor++;
    const ordinal = closureIndex.packBaseOrdinals[packSlot]! + oidIndex;
    const located: LocatedObject = { packSlot, oidIndex, ordinal };

    if (seenFlags[located.ordinal]) continue;
    seenFlags[located.ordinal] = 1;
    seenCount++;

    if (stopFlags[located.ordinal]) {
      log.debug("stream:plan:hit-stop", {
        packKey: args.packs[located.packSlot]!.packKey,
        oidIndex: located.oidIndex,
      });
      continue;
    }

    addNeededObject(located, neededFlags, neededPackSlots, neededOidIndices, neededCount);
    let budgetExceeded = false;
    visitPackRefRawRefsAt(
      args.packs[located.packSlot]!.refs,
      located.oidIndex,
      (rawRefs, start) => {
        edgeVisits++;
        if (budgetExceeded) return;

        const refLocated = locateObject(args.packs, closureIndex, rawRefs, start);
        if (refLocated) {
          enqueueLocatedObject(queue, queuedFlags, refLocated, duplicateQueueSkips);
          return;
        }

        const oid = bytesToHex(rawRefs.subarray(start, start + OID_BYTES));
        const includeNeeded = !missingStop.has(oid);
        const result = recordMissingOid({
          oid,
          missingSeen,
          missingNeeded,
          includeNeeded,
        });
        if (result === "budget-exceeded") {
          budgetExceeded = true;
          return;
        }

        if (result === "recorded" && !includeNeeded) {
          log.debug("stream:plan:hit-stop", { oid });
        }
      }
    );
    if (budgetExceeded) {
      return buildBudgetExceededResult("missing-ref-budget");
    }
  }

  const stats = buildStats();
  log.info("stream:plan:closure-complete", {
    needed: stats.needed,
    seen: stats.seen,
    queued: stats.queued,
    missing: stats.missing,
    edgeVisits: stats.edgeVisits,
    duplicateQueueSkips: stats.duplicateQueueSkips,
    stopSet: stopCount,
    ackOids: ackOids.length,
    timeMs: Date.now() - startTime,
  });

  return {
    type: "Ready",
    neededOids: buildNeededOids({
      packs: args.packs,
      neededPackSlots: neededPackSlots.values,
      neededOidIndices: neededOidIndices.values,
      neededCount: neededCount.value,
      missingNeeded,
    }),
    ackOids,
    stats,
  };
}
