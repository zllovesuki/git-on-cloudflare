import type { Logger } from "@/common/logger.ts";

import {
  compareSelectionSlots,
  copySelectionRow,
  selectionDependsOn,
  type SelectionTable,
} from "./shared.ts";

export function collapseUnsafeRedirectOwners(
  table: SelectionTable,
  deadSlots: Map<number, number>,
  log: Logger
): number {
  let collapsed = 0;

  while (true) {
    const rewrites = collectUnsafeRedirectOwnerRewrites(table, deadSlots);
    if (rewrites.length === 0) return collapsed;

    rewrites.sort((a, b) => compareSelectionSlots(table, a.targetSel, b.targetSel));

    for (const rewrite of rewrites) {
      rewriteSelectionRowFromSource(table, rewrite.targetSel, rewrite.sourceSel);
      collapsed++;

      log.debug("rewrite:collapse-unsafe-redirect-owner", {
        targetSel: rewrite.targetSel,
        sourceSel: rewrite.sourceSel,
        packSlot: table.packSlots[rewrite.targetSel],
        entryIndex: table.entryIndices[rewrite.targetSel],
        typeCode: table.typeCodes[rewrite.targetSel],
      });
    }
  }
}

/**
 * Remove dead selection slots and remap baseSel references.
 *
 * Most dead slots are duplicate-OID selections redirected to another already-
 * selected owner. The retained-redirect repair pass should already have
 * rewritten any topology-sensitive owners before compaction runs, but this
 * final graph walk remains as a guardrail around the remap itself.
 */
export function compactDeadSlots(
  table: SelectionTable,
  deadSlots: Map<number, number>,
  log: Logger
): void {
  const safeDeadSlots = pruneUnsafeDeadSlotRedirects(table, deadSlots);

  // 1. Redirect baseSel references from dead slots to their targets.
  //    Handles chains (dead → dead → live) by following until stable.
  function resolve(sel: number): number {
    let cur = sel;
    for (let depth = 0; depth < safeDeadSlots.size + 1; depth++) {
      const next = safeDeadSlots.get(cur);
      if (next === undefined) return cur;
      cur = next;
    }
    return cur; // fallback: should not loop
  }

  for (let i = 0; i < table.count; i++) {
    if (table.baseSlots[i] >= 0) {
      table.baseSlots[i] = resolve(table.baseSlots[i]);
    }
  }

  // 2. Build old → new index remap (skipping dead slots).
  const remap = new Int32Array(table.count).fill(-1);
  let write = 0;
  for (let read = 0; read < table.count; read++) {
    if (safeDeadSlots.has(read)) continue;
    remap[read] = write;
    if (write !== read) {
      // The live row's raw OID stays authoritative for owner lookups and
      // follow-on duplicate redirects, so compact the full planner row with it.
      // `baseSlots` is row state too: move it now, then remap the preserved
      // old live indices to their new compacted indices below.
      copySelectionRow(table, write, read);
    }
    write++;
  }

  // 3. Remap baseSel references to new indices.
  for (let i = 0; i < write; i++) {
    const base = table.baseSlots[i];
    if (base >= 0) {
      table.baseSlots[i] = remap[base];
    }
  }

  log.debug("rewrite:compact-dead-slots", {
    removed: table.count - write,
  });
  table.count = write;
}

/**
 * Some duplicate-owner redirects are not safe to compact away.
 *
 * If the surviving owner already depends on the duplicate being removed,
 * redirecting every reference from `dead -> live` would collapse that live
 * row's base chain back onto itself and manufacture a cycle. Callers use this
 * to find the remaining rows that still need owner-slot rewrites before the
 * duplicate can be compacted away safely.
 */
export function pruneUnsafeDeadSlotRedirects(
  table: SelectionTable,
  deadSlots: Map<number, number>
): Map<number, number> {
  const safeDeadSlots = new Map(deadSlots);

  let changed = true;
  while (changed) {
    changed = false;

    for (const [deadSel] of safeDeadSlots) {
      const targetSel = resolveDeadSlotRedirect(deadSel, safeDeadSlots);
      if (targetSel === deadSel || selectionDependsOn(table, targetSel, deadSel)) {
        safeDeadSlots.delete(deadSel);
        changed = true;
      }
    }
  }

  return safeDeadSlots;
}

function collectUnsafeRedirectOwnerRewrites(
  table: SelectionTable,
  deadSlots: Map<number, number>
): Array<{ targetSel: number; sourceSel: number }> {
  const rewrites: Array<{ targetSel: number; sourceSel: number }> = [];
  const plannedTargets = new Set<number>();

  for (const [deadSel] of deadSlots) {
    const targetSel = resolveDeadSlotRedirect(deadSel, deadSlots);
    if (targetSel === deadSel || !selectionDependsOn(table, targetSel, deadSel)) {
      continue;
    }
    if (plannedTargets.has(targetSel)) continue;

    const sourceSel = findNearestRedirectDependency(table, targetSel, deadSlots);
    if (sourceSel === undefined) continue;

    rewrites.push({ targetSel, sourceSel });
    plannedTargets.add(targetSel);
  }

  return rewrites;
}

function findNearestRedirectDependency(
  table: SelectionTable,
  targetSel: number,
  deadSlots: Map<number, number>
): number | undefined {
  let cur = targetSel;
  for (let depth = 0; depth < table.count; depth++) {
    const baseSel = table.baseSlots[cur];
    if (baseSel < 0) return undefined;
    if (resolveDeadSlotRedirect(baseSel, deadSlots) === targetSel) {
      return baseSel;
    }
    cur = baseSel;
  }
  return undefined;
}

function rewriteSelectionRowFromSource(
  table: SelectionTable,
  targetSel: number,
  sourceSel: number
): void {
  copySelectionRow(table, targetSel, sourceSel, { preserveTargetOfsPinned: true });
  // The rewritten owner row already has a resolved header; make sure it never
  // re-enters the header-read queue just because the source row had stale state.
  table.queuedForHeader[targetSel] = 0;
  // Unsafe-redirect collapse can move a pinned dependency into an already
  // pinned owner slot, so the merged row keeps either pin requirement.
  table.ofsPinned[targetSel] = table.ofsPinned[targetSel] || table.ofsPinned[sourceSel] ? 1 : 0;
}

function resolveDeadSlotRedirect(sel: number, deadSlots: Map<number, number>): number {
  let cur = sel;
  for (let depth = 0; depth < deadSlots.size + 1; depth++) {
    const next = deadSlots.get(cur);
    if (next === undefined) return cur;
    cur = next;
  }
  return cur;
}
