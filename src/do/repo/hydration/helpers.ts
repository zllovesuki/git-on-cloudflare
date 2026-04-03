import type { RepoStateSchema, HydrationWork, HydrationStage, TypedStorage } from "../repoState.ts";
import type { Logger } from "@/common/logger.ts";
import type { IdxView } from "@/git/object-store/types.ts";

import { createLogger } from "@/common/index.ts";
import { findOffsetIndex, findOidIndex, getOidHexAt } from "@/git/object-store/index.ts";
import { getDoIdFromPath } from "@/keys.ts";
import { getConfig } from "../repoConfig.ts";
import {
  getDb,
  hasHydrCoverForWork,
  insertHydrCoverOids,
  getPackOids,
  normalizePackKey,
} from "../db/index.ts";
import { parseEpochFromHydrPackKey, calculateStableEpochs } from "../packs.ts";

export const HYDR_SAMPLE_PER_PACK = 128;
export const HYDR_SOFT_SUBREQ_LIMIT = 800;
export const HYDR_LOOSE_LIST_PAGE = 250;
export const HYDR_SEG_MAX_BYTES = 8 * 1024 * 1024;
export const HYDR_MAX_OBJS_PER_SEGMENT = 2000;
export const HYDR_EST_COMPRESSION_RATIO = 0.6;
export const PACK_TYPE_OFS_DELTA = 6 as const;
export const PACK_TYPE_REF_DELTA = 7 as const;

export type HydrationConfig = {
  unpackMaxMs: number;
  unpackDelayMs: number;
  unpackBackoffMs: number;
  chunk: number;
  keepPacks: number;
  windowMax: number;
};
export type HydrationStore = TypedStorage<RepoStateSchema>;
export type HydrationPhysicalIndex = {
  idxView: IdxView;
  sortedOffsets: Float64Array;
  getOidAtIndex(index: number): string;
  findIndexByOffset(offset: number): number | undefined;
  findIndexByOid(oid: string): number | undefined;
  hasOid(oid: string): boolean;
};

export function nowMs() {
  return Date.now();
}

export function getHydrConfig(env: Env) {
  const base = getConfig(env);
  const windowMax = base.packListMax;
  return {
    unpackMaxMs: base.unpackMaxMs,
    unpackDelayMs: base.unpackDelayMs,
    unpackBackoffMs: base.unpackBackoffMs,
    chunk: base.unpackChunkSize,
    keepPacks: base.keepPacks,
    windowMax,
  };
}

export function makeHydrationLogger(env: Env, lastPackKey?: string | null): Logger {
  const doId = getDoIdFromPath(lastPackKey || "") || undefined;
  return createLogger(env.LOG_LEVEL, { service: "Hydration", doId });
}

export function buildRecentWindowKeys(
  lastPackKey: string | null,
  packList: string[],
  windowMax: number
): string[] {
  const windowKeys: string[] = [];
  if (lastPackKey) windowKeys.push(lastPackKey);
  for (const k of packList) if (!windowKeys.includes(k)) windowKeys.push(k);
  return windowKeys.slice(0, windowMax);
}

export async function computeStableHydrationWindow(
  store: HydrationStore,
  cfg: HydrationConfig
): Promise<{ window: string[]; lastPackKey: string | null }> {
  const lastPackKey = (await store.get("lastPackKey")) || null;
  const packListRaw = (await store.get("packList")) || [];
  const packList = Array.isArray(packListRaw) ? packListRaw : [];

  const { stableEpochs } = calculateStableEpochs(packList, cfg.keepPacks, lastPackKey || undefined);
  const stableSet = new Set(stableEpochs);

  const hydra: string[] = [];
  for (const k of packList) {
    const e = parseEpochFromHydrPackKey(k);
    if (e && stableSet.has(e)) hydra.push(k);
  }

  const lbase = normalizePackKey(lastPackKey || "");
  const lastEpoch = lastPackKey ? parseEpochFromHydrPackKey(lastPackKey) : null;
  if (lastPackKey && lbase.startsWith("pack-hydr-") && lastEpoch && stableSet.has(lastEpoch)) {
    hydra.unshift(lastPackKey);
  }

  const window = hydra.slice(0, cfg.windowMax);
  return { window, lastPackKey };
}

export async function ensureHydrCoverForWork(
  state: DurableObjectState,
  store: HydrationStore,
  cfg: HydrationConfig,
  workId: string
): Promise<void> {
  if (!workId) return;
  const db = getDb(state.storage);
  try {
    const exists = await hasHydrCoverForWork(db, workId);
    if (exists) return;
  } catch {}

  const { window, lastPackKey } = await computeStableHydrationWindow(store, cfg);

  for (const pk of window) {
    try {
      const oids = (await getPackOids(db, pk)).map((o) => o.toLowerCase());
      await insertHydrCoverOids(db, workId, oids);
    } catch {}
  }

  const includeLastOids =
    !!lastPackKey &&
    normalizePackKey(lastPackKey).startsWith("pack-hydr-") &&
    window.includes(lastPackKey);
  if (includeLastOids) {
    try {
      const last = ((await store.get("lastPackOids")) || []).slice(0, 10000);
      const oids = (Array.isArray(last) ? last : []).map((oid: string) => oid.toLowerCase());
      await insertHydrCoverOids(db, workId, oids);
    } catch {}
  }
}

export async function buildHydrationCoverageSet(
  state: DurableObjectState,
  store: HydrationStore,
  cfg: HydrationConfig
): Promise<Set<string>> {
  const covered = new Set<string>();
  try {
    const db = getDb(state.storage);
    const { window, lastPackKey } = await computeStableHydrationWindow(store, cfg);
    try {
      if (window.length > 0) {
        for (const pk of window) {
          const rows = await getPackOids(db, pk);
          for (const oid of rows) covered.add(String(oid).toLowerCase());
        }
      }
    } catch {}
    const includeLastOids =
      !!lastPackKey &&
      normalizePackKey(lastPackKey).startsWith("pack-hydr-") &&
      window.includes(lastPackKey);
    if (includeLastOids) {
      const last = (await store.get("lastPackOids")) || [];
      for (const x of last.slice(0, 10000)) covered.add(x.toLowerCase());
    }
  } catch {}
  return covered;
}

export function setStage(work: HydrationWork, stage: HydrationStage, log: Logger) {
  if (work.stage !== stage) {
    log.debug("hydration:transition", { from: work.stage, to: stage });
    work.stage = stage;
  }
}

export type HydrationProgress = NonNullable<HydrationWork["progress"]>;

export function updateProgress(work: HydrationWork, patch: Partial<HydrationProgress>) {
  const base: HydrationProgress = { ...(work.progress ?? {}) };
  Object.assign(base, patch);
  work.progress = base;
}

export function clearError(work: HydrationWork) {
  if (work.error) work.error = undefined;
}

export function buildPhysicalIndex(idxView: IdxView): HydrationPhysicalIndex {
  return {
    idxView,
    sortedOffsets: idxView.sortedOffsets,
    getOidAtIndex(index: number) {
      return getOidHexAt(idxView, index);
    },
    findIndexByOffset(offset: number) {
      return findOffsetIndex(idxView, offset);
    },
    findIndexByOid(oid: string) {
      const index = findOidIndex(idxView, oid);
      return index >= 0 ? index : undefined;
    },
    hasOid(oid: string) {
      return findOidIndex(idxView, oid) >= 0;
    },
  };
}
