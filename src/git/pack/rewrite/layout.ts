import type { OrderedPackSnapshot } from "@/git/operations/fetch/types.ts";
import type { Logger } from "@/common/logger.ts";

import { BinaryHeap } from "@/common/index.ts";
import { ofsDeltaDistanceLength } from "../packMeta.ts";
import { HEADER_STABILITY_CAP, type SelectionTable } from "./shared.ts";

export function canPassthroughSinglePack(
  snapshot: OrderedPackSnapshot,
  table: SelectionTable
): boolean {
  if (snapshot.packs.length !== 1 || table.count !== snapshot.packs[0]?.idx.count) {
    return false;
  }

  for (let sel = 0; sel < table.count; sel++) {
    if (table.syntheticPayloads[sel]) return false;
  }
  return true;
}

/**
 * Populates `table.outputOrder` with a topologically valid output ordering
 * (bases before deltas). Returns false on cycle or incomplete graph.
 */
export function buildOutputOrder(table: SelectionTable, log: Logger): boolean {
  const n = table.count;

  // Build dependency graph using linked-list arrays (same pattern as
  // src/git/pack/indexer/resolve/dependencies.ts)
  const indegree = new Uint8Array(n);
  const childHead = new Int32Array(n).fill(-1);
  const childNext = new Int32Array(n).fill(-1);

  for (let sel = 0; sel < n; sel++) {
    const base = table.baseSlots[sel];
    if (base < 0) continue;
    indegree[sel]++;
    childNext[sel] = childHead[base];
    childHead[base] = sel;
  }

  // Min-heap comparing (packSlots, offsets) for stable tie-breaking.
  // Reuses the generic BinaryHeap from src/common/heap.ts.
  const cmp = (a: number, b: number) => {
    const packDiff = table.packSlots[a] - table.packSlots[b];
    if (packDiff !== 0) return packDiff;
    return table.offsets[a] - table.offsets[b];
  };
  const roots: number[] = [];
  for (let sel = 0; sel < n; sel++) {
    if (indegree[sel] === 0) roots.push(sel);
  }
  const heap = new BinaryHeap<number>(cmp, roots);

  let cursor = 0;
  while (!heap.isEmpty()) {
    const sel = heap.pop()!;
    table.outputOrder[cursor++] = sel;

    let child = childHead[sel];
    while (child >= 0) {
      indegree[child]--;
      if (indegree[child] === 0) heap.push(child);
      child = childNext[child];
    }
  }

  if (cursor !== n) {
    log.warn("rewrite:topology-incomplete", { selected: n, ordered: cursor });
    return false;
  }
  return true;
}

/**
 * Computes output header lengths and offsets, iterating until OFS_DELTA
 * distance varints stabilize. Returns false on convergence failure.
 */
export function computeHeaderLengths(table: SelectionTable, log: Logger): boolean {
  const n = table.count;

  // Seed initial output header lengths
  for (let i = 0; i < n; i++) {
    const sel = table.outputOrder[i];
    const svLen = table.sizeVarLens[sel];
    const type = table.typeCodes[sel];

    if (type === 6) {
      // Initial estimate using the original source-pack OFS distance
      const base = table.baseSlots[sel];
      table.outputHeaderLens[sel] =
        svLen + ofsDeltaDistanceLength(table.offsets[sel] - table.offsets[base]);
    } else if (type === 7) {
      table.outputHeaderLens[sel] = svLen + 20;
    } else {
      table.outputHeaderLens[sel] = svLen;
    }
  }

  for (let iteration = 0; iteration < HEADER_STABILITY_CAP; iteration++) {
    // Compute cumulative output offsets (12-byte PACK header)
    let cursor = 12;
    for (let i = 0; i < n; i++) {
      const sel = table.outputOrder[i];
      table.outputOffsets[sel] = cursor;
      cursor += table.outputHeaderLens[sel] + table.payloadLens[sel];
    }

    // Recompute OFS_DELTA distances with new output offsets
    let changed = false;
    for (let i = 0; i < n; i++) {
      const sel = table.outputOrder[i];
      if (table.typeCodes[sel] !== 6) continue;

      const base = table.baseSlots[sel];
      if (base < 0) continue;

      const distance = table.outputOffsets[sel] - table.outputOffsets[base];
      const nextLen = table.sizeVarLens[sel] + ofsDeltaDistanceLength(distance);
      if (nextLen !== table.outputHeaderLens[sel]) {
        table.outputHeaderLens[sel] = nextLen;
        changed = true;
      }
    }

    if (!changed) {
      // The 5-byte sizeVarBuf and 32-bit OFS distance encoding assume < 4 GiB.
      if (cursor > 0xffff_ffff) {
        log.warn("rewrite:output-pack-exceeds-32bit", { totalBytes: cursor });
        return false;
      }
      return true;
    }
  }

  log.warn("rewrite:header-lengths-did-not-converge", { selected: n });
  return false;
}
