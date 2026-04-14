import type { OrderedPackSnapshot } from "@/git/operations/fetch/types.ts";
import type { Logger } from "@/common/logger.ts";

import { clonePackHeader, type DuplicateHeaderCache } from "./ownership.ts";
import { pruneUnsafeDeadSlotRedirects } from "./selectionCompact.ts";
import { resolveDeltaBaseFromHeader } from "./selectionResolve.ts";
import {
  ensurePackReadState,
  readSelectedHeader,
  selectionKey,
  type PackReadState,
  type RewriteOptions,
  type SelectionTable,
} from "./shared.ts";

export function collectRetainedRedirectsNeedingBaseResolution(
  table: SelectionTable,
  deadSlots: Map<number, number>
): number[] {
  const safeDeadSlots = pruneUnsafeDeadSlotRedirects(table, deadSlots);
  const retained: number[] = [];

  for (const [deadSel] of deadSlots) {
    // `safeDeadSlots` is the subset we are still allowed to remove. When a row
    // is present in `deadSlots` but absent here, dead-slot pruning decided that
    // the redirect would manufacture a cycle, so this "dead" row is actually
    // staying live in the output pack.
    if (safeDeadSlots.has(deadSel)) continue;
    const typeCode = table.typeCodes[deadSel];
    if ((typeCode === 6 || typeCode === 7) && table.baseSlots[deadSel] < 0) {
      retained.push(deadSel);
    }
  }

  return retained;
}

export async function resolveRetainedRedirectBase(
  table: SelectionTable,
  sel: number,
  snapshot: OrderedPackSnapshot,
  readerStates: Map<number, PackReadState>,
  dedupMap: Map<number, number>,
  duplicateHeaderCache: DuplicateHeaderCache,
  secondaryQueue: number[],
  env: Env,
  log: Logger,
  warnedFlags: Set<string>,
  options?: RewriteOptions
): Promise<boolean> {
  if (table.baseSlots[sel] >= 0) return true;

  const packSlot = table.packSlots[sel];
  const pack = snapshot.packs[packSlot];
  const cacheKey = selectionKey(packSlot, table.entryIndices[sel]);
  let header = duplicateHeaderCache.get(cacheKey);

  if (header === undefined) {
    // The common path already memoized this header before issuing the redirect.
    // Falling back to a reread is correct here, but it should stay rare and
    // bounded to the retained-redirect edge case.
    const readState = await ensurePackReadState(
      env,
      pack,
      packSlot,
      readerStates,
      log,
      warnedFlags,
      options
    );
    header = await readSelectedHeader(readState, table.offsets[sel]);
    duplicateHeaderCache.set(cacheKey, header ? clonePackHeader(header) : null);
  }

  if (!header) {
    log.warn("rewrite:header-read-failed", { packKey: pack.packKey, offset: table.offsets[sel] });
    return false;
  }

  log.debug("rewrite:retained-redirect-base-resolve", {
    sel,
    packKey: pack.packKey,
    entryIndex: table.entryIndices[sel],
    typeCode: table.typeCodes[sel],
  });

  // This intentionally resolves only the retained row's own base edge. Any
  // newly discovered base rows still flow through the normal secondary queue so
  // they get the same header-read and duplicate-owner handling as the primary
  // selection pass.
  return resolveDeltaBaseFromHeader(table, sel, snapshot, dedupMap, secondaryQueue, log, header);
}
