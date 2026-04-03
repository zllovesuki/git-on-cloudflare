import type { HydrationCtx, StageHandlerResult } from "../types.ts";
import type { HydrationWork } from "../../repoState.ts";
import type { PackHeaderEx } from "@/git/pack/packMeta.ts";

import {
  setStage,
  updateProgress,
  clearError,
  type HydrationPhysicalIndex,
  HYDR_SOFT_SUBREQ_LIMIT,
  PACK_TYPE_OFS_DELTA,
  PACK_TYPE_REF_DELTA,
  buildPhysicalIndex,
  makeHydrationLogger,
  nowMs,
} from "../helpers.ts";
import { handleTransientError } from "../cleanup.ts";
import {
  getDb,
  getHydrPendingCounts,
  insertHydrPendingOids,
  filterUncoveredAgainstHydrCover,
  getPackCatalogRow,
} from "../../db/index.ts";
import { loadIdxView } from "@/git/object-store/index.ts";
import { readPackHeaderEx } from "@/git/pack/index.ts";

export async function handleStageScanDeltas(
  ctx: HydrationCtx,
  work: HydrationWork
): Promise<StageHandlerResult> {
  const { state, log, cfg } = ctx;
  const db = getDb(state.storage);
  log.debug("hydration:scan-deltas:tick", {
    packIndex: work.progress?.packIndex || 0,
    objCursor: work.progress?.objCursor || 0,
    window: work.snapshot?.window?.length || 0,
  });
  const res = await scanDeltasSlice(ctx, work);
  if (res === "next") {
    setStage(work, "scan-loose", log);
    const counts = await getHydrPendingCounts(db, work.workId);
    log.info("hydration:scan-deltas:done", { needBases: counts.bases });
    clearError(work);
  } else if (res === "error") {
    await handleTransientError(work, log, cfg);
  } else {
    clearError(work);
  }
  return { continue: true };
}

async function scanDeltasSlice(
  ctx: HydrationCtx,
  work: HydrationWork
): Promise<"more" | "next" | "error"> {
  const { state, env, cfg } = ctx;
  const start = nowMs();
  const log = makeHydrationLogger(env, work.snapshot?.lastPackKey || "");
  const db = getDb(state.storage);

  const window = work.snapshot?.window || [];
  if (!window || window.length === 0) return "next";

  const inPackCoverageCandidates = new Set<string>();
  const needBasesSet = new Set<string>();

  let pIndex = work.progress?.packIndex || 0;
  let objCur = work.progress?.objCursor || 0;
  let subreq = 0;

  while (pIndex < window.length && nowMs() - start < cfg.unpackMaxMs) {
    const key = window[pIndex];
    let idx;
    try {
      const packRow = await getPackCatalogRow(db, key);
      if (!packRow) {
        pIndex++;
        objCur = 0;
        log.warn("scan-deltas:missing-pack-catalog-row", { key });
        continue;
      }
      idx = await loadIdxView(env, key, undefined, packRow.packBytes);
      subreq++;
    } catch (e) {
      log.warn("scan-deltas:idx-load-error", { key, error: String(e) });
      work.error = { message: `Failed to load pack index: ${String(e)}` };
      updateProgress(work, { packIndex: pIndex, objCursor: objCur });
      await insertHydrPendingOids(db, work.workId, "base", Array.from(needBasesSet));
      return "error";
    }
    if (!idx) {
      pIndex++;
      objCur = 0;
      log.warn("scan-deltas:missing-idx", { key });
      continue;
    }
    const phys = buildPhysicalIndex(idx);

    const end = Math.min(phys.sortedOffsets.length, objCur + cfg.chunk);
    for (let j = objCur; j < end; j++) {
      const off = phys.sortedOffsets[j];
      let header;
      try {
        header = await readPackHeaderEx(env, key, off);
        subreq++;
      } catch (e) {
        log.warn("scan-deltas:header-read-error", { key, off, error: String(e) });
        work.error = { message: `Failed to read pack header: ${String(e)}` };
        objCur = j;
        updateProgress(work, { packIndex: pIndex, objCursor: objCur });
        await insertHydrPendingOids(db, work.workId, "base", Array.from(needBasesSet));
        return "error";
      }
      if (!header) continue;

      const chain = await analyzeDeltaChain(ctx, key, header, off, phys, (q: string) => {
        if (phys.hasOid(q)) inPackCoverageCandidates.add(q);
        return false;
      });
      for (const oid of chain) needBasesSet.add(oid);

      if (nowMs() - start >= cfg.unpackMaxMs || subreq >= HYDR_SOFT_SUBREQ_LIMIT) {
        objCur = j + 1;
        try {
          const uncovered = await filterUncoveredAgainstHydrCover(
            db,
            work.workId,
            Array.from(inPackCoverageCandidates)
          );
          const uncoveredSet = new Set(uncovered);
          for (const q of inPackCoverageCandidates) {
            if (!uncoveredSet.has(q)) needBasesSet.delete(q);
          }
        } catch {}
        updateProgress(work, { packIndex: pIndex, objCursor: objCur });
        await insertHydrPendingOids(db, work.workId, "base", Array.from(needBasesSet));
        log.debug("scan-deltas:slice", {
          packIndex: pIndex,
          advanced: j - (work.progress?.objCursor || 0),
          needBases: needBasesSet.size,
        });
        return "more";
      }
    }
    objCur = end;
    if (objCur >= phys.sortedOffsets.length) {
      pIndex++;
      objCur = 0;
    } else {
      updateProgress(work, { packIndex: pIndex, objCursor: objCur });
      await insertHydrPendingOids(db, work.workId, "base", Array.from(needBasesSet));
      log.debug("scan-deltas:continue", {
        packIndex: pIndex,
        objCursor: objCur,
        needBases: needBasesSet.size,
      });
      return "more";
    }
  }

  try {
    const uncovered = await filterUncoveredAgainstHydrCover(
      db,
      work.workId,
      Array.from(inPackCoverageCandidates)
    );
    const uncoveredSet = new Set(uncovered);
    for (const q of inPackCoverageCandidates) {
      if (!uncoveredSet.has(q)) needBasesSet.delete(q);
    }
  } catch {}
  updateProgress(work, { packIndex: pIndex, objCursor: objCur });
  await insertHydrPendingOids(db, work.workId, "base", Array.from(needBasesSet));
  log.info("scan-deltas:complete", { needBases: needBasesSet.size });
  return pIndex < window.length ? "more" : "next";
}

async function analyzeDeltaChain(
  ctx: HydrationCtx,
  packKey: string,
  header: PackHeaderEx,
  off: number,
  idx: HydrationPhysicalIndex,
  coveredHas: (q: string) => boolean
): Promise<string[]> {
  const chain: string[] = [];
  const seen = new Set<string>();
  let baseOid: string | undefined;
  let currentOff = off;
  let currentHeader = header;

  while (true) {
    baseOid = undefined;

    if (currentHeader.type === PACK_TYPE_OFS_DELTA) {
      const baseOff = currentOff - (currentHeader.baseRel || 0);
      const baseIdx = idx.findIndexByOffset(baseOff);
      if (baseIdx !== undefined) baseOid = idx.getOidAtIndex(baseIdx);
      currentOff = baseOff;
    } else if (currentHeader.type === PACK_TYPE_REF_DELTA) {
      baseOid = currentHeader.baseOid;
      if (baseOid) {
        const searchOid = baseOid.toLowerCase();
        const baseIdx = idx.findIndexByOid(searchOid);
        if (baseIdx !== undefined) {
          currentOff = idx.idxView.offsets[baseIdx];
        } else {
          if (!coveredHas(searchOid) && !seen.has(searchOid)) {
            chain.push(searchOid);
          }
          break;
        }
      }
    }

    if (!baseOid) break;

    const q = baseOid.toLowerCase();
    if (seen.has(q)) break;
    seen.add(q);

    if (!idx.hasOid(q) || !coveredHas(q)) {
      chain.push(q);
      if (!idx.hasOid(q)) break;
    }

    try {
      const nextHeader = await readPackHeaderEx(ctx.env, packKey, currentOff);
      if (!nextHeader) break;
      if (nextHeader.type !== PACK_TYPE_OFS_DELTA && nextHeader.type !== PACK_TYPE_REF_DELTA) {
        break;
      }
      currentHeader = nextHeader;
    } catch {
      break;
    }
  }

  return chain;
}
