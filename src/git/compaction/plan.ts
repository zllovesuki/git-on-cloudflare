import type { OrderedPackSnapshotEntry } from "@/git/operations/fetch/types.ts";

import { BinaryHeap, bytesToHex } from "@/common/index.ts";

type CompactionHeapEntry = {
  packSlot: number;
  objectIndex: number;
};

function compareRawOid(
  leftView: OrderedPackSnapshotEntry["idx"],
  leftIndex: number,
  rightView: OrderedPackSnapshotEntry["idx"],
  rightIndex: number
): number {
  const leftStart = leftIndex * 20;
  const rightStart = rightIndex * 20;
  for (let byteIndex = 0; byteIndex < 20; byteIndex++) {
    const diff =
      leftView.rawNames[leftStart + byteIndex] - rightView.rawNames[rightStart + byteIndex];
    if (diff !== 0) return diff;
  }
  return 0;
}

function compareRawOidToBuffer(
  view: OrderedPackSnapshotEntry["idx"],
  objectIndex: number,
  other: Uint8Array
): number {
  const start = objectIndex * 20;
  for (let byteIndex = 0; byteIndex < 20; byteIndex++) {
    const diff = view.rawNames[start + byteIndex] - other[byteIndex];
    if (diff !== 0) return diff;
  }
  return 0;
}

function copyRawOid(view: OrderedPackSnapshotEntry["idx"], objectIndex: number): Uint8Array {
  const start = objectIndex * 20;
  return view.rawNames.slice(start, start + 20);
}

export function buildCompactionNeededOids(sourcePacks: OrderedPackSnapshotEntry[]): string[] {
  const heap = new BinaryHeap<CompactionHeapEntry>((left, right) => {
    const leftView = sourcePacks[left.packSlot]!.idx;
    const rightView = sourcePacks[right.packSlot]!.idx;
    const cmp = compareRawOid(leftView, left.objectIndex, rightView, right.objectIndex);
    if (cmp !== 0) return cmp;
    if (left.packSlot !== right.packSlot) return left.packSlot - right.packSlot;
    return left.objectIndex - right.objectIndex;
  });

  for (let packSlot = 0; packSlot < sourcePacks.length; packSlot++) {
    if (sourcePacks[packSlot]!.idx.count > 0) {
      heap.push({ packSlot, objectIndex: 0 });
    }
  }

  const neededOids: string[] = [];
  let previousOid: Uint8Array | undefined;

  while (!heap.isEmpty()) {
    const current = heap.pop()!;
    const currentPack = sourcePacks[current.packSlot]!;
    if (
      !previousOid ||
      compareRawOidToBuffer(currentPack.idx, current.objectIndex, previousOid) !== 0
    ) {
      const oidBytes = copyRawOid(currentPack.idx, current.objectIndex);
      neededOids.push(bytesToHex(oidBytes));
      previousOid = oidBytes;
    }

    const nextObjectIndex = current.objectIndex + 1;
    if (nextObjectIndex < currentPack.idx.count) {
      heap.push({
        packSlot: current.packSlot,
        objectIndex: nextObjectIndex,
      });
    }
  }

  return neededOids;
}
