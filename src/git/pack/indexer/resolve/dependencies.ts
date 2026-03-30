import type { PackEntryTable } from "../types.ts";

export interface InPackDependencyQueue {
  waitHeadByBase: Int32Array;
  nextWaitByEntry: Int32Array;
  registeredBaseByEntry: Int32Array;
}

export function createInPackDependencyQueue(objectCount: number): InPackDependencyQueue {
  return {
    waitHeadByBase: new Int32Array(objectCount).fill(-1),
    nextWaitByEntry: new Int32Array(objectCount).fill(-1),
    registeredBaseByEntry: new Int32Array(objectCount).fill(-1),
  };
}

export function extendDeadlineChain(
  deadlines: Uint32Array,
  baseIndexArr: Int32Array,
  startIndex: number,
  neededUntil: number
): void {
  let cur = startIndex;
  for (let guard = 0; cur >= 0 && guard < baseIndexArr.length; guard++) {
    if (deadlines[cur] < neededUntil) {
      deadlines[cur] = neededUntil;
    }
    cur = baseIndexArr[cur];
  }
}

export function registerInPackDependency(
  queue: InPackDependencyQueue,
  baseIndexArr: Int32Array,
  entryIndex: number
): void {
  const baseIndex = baseIndexArr[entryIndex];
  if (baseIndex < 0) return;
  if (queue.registeredBaseByEntry[entryIndex] === baseIndex) return;

  // Every delta has exactly one in-pack base edge once `baseIndexArr` is set.
  // Recording that edge lets the resolver wake dependents in O(children) when
  // a late base finally resolves instead of rescanning the whole deferred set.
  queue.nextWaitByEntry[entryIndex] = queue.waitHeadByBase[baseIndex];
  queue.waitHeadByBase[baseIndex] = entryIndex;
  queue.registeredBaseByEntry[entryIndex] = baseIndex;
}

export function enqueueReadyDeferred(
  readyDeferred: number[],
  deferredQueued: Uint8Array,
  table: PackEntryTable,
  baseIndexArr: Int32Array,
  index: number
): void {
  if (deferredQueued[index] || table.resolved[index]) return;
  const bi = baseIndexArr[index];
  if (bi < 0 || !table.resolved[bi]) return;
  deferredQueued[index] = 1;
  readyDeferred.push(index);
}

export function promoteReadyInPackDependents(
  queue: InPackDependencyQueue,
  resolvedIndex: number,
  readyDeferred: number[],
  deferredQueued: Uint8Array,
  table: PackEntryTable,
  baseIndexArr: Int32Array
): void {
  let waitingEntry = queue.waitHeadByBase[resolvedIndex];
  if (waitingEntry < 0) return;

  queue.waitHeadByBase[resolvedIndex] = -1;
  while (waitingEntry >= 0) {
    const nextWaitingEntry = queue.nextWaitByEntry[waitingEntry];
    queue.nextWaitByEntry[waitingEntry] = -1;
    queue.registeredBaseByEntry[waitingEntry] = -1;
    enqueueReadyDeferred(readyDeferred, deferredQueued, table, baseIndexArr, waitingEntry);
    waitingEntry = nextWaitingEntry;
  }
}
