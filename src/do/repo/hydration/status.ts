import type { HydrationPlan } from "./types.ts";
import type { RepoStateSchema } from "../repoState.ts";
import {
  HYDR_SAMPLE_PER_PACK,
  HYDR_MAX_OBJS_PER_SEGMENT,
  HYDR_SEG_MAX_BYTES,
  HYDR_SOFT_SUBREQ_LIMIT,
  PACK_TYPE_OFS_DELTA,
  PACK_TYPE_REF_DELTA,
  getHydrConfig,
  makeHydrationLogger,
  buildRecentWindowKeys,
  buildHydrationCoverageSet,
  buildPhysicalIndex,
} from "./helpers.ts";
import { asTypedStorage } from "../repoState.ts";
import { getDb, deletePackObjects, getPackCatalogRow } from "../db/index.ts";
import { loadIdxView } from "@/git/object-store/index.ts";
import { readPackHeaderEx } from "@/git/pack/index.ts";
import { createLogger } from "@/common/index.ts";
import { packIndexKey } from "@/keys.ts";
import { normalizePackKey } from "../db/index.ts";

export async function summarizeHydrationPlan(
  state: DurableObjectState,
  env: Env,
  prefix: string
): Promise<HydrationPlan> {
  const log = makeHydrationLogger(env, prefix);
  const store = asTypedStorage<RepoStateSchema>(state.storage);
  const cfg = getHydrConfig(env);
  const db = getDb(state.storage);

  const lastPackKey = (await store.get("lastPackKey")) || null;
  const packListRaw = (await store.get("packList")) || [];
  const packList = Array.isArray(packListRaw) ? packListRaw : [];

  const window = buildRecentWindowKeys(lastPackKey, packList, cfg.windowMax);

  const covered = await buildHydrationCoverageSet(state, store, cfg);

  let examinedObjects = 0;
  const baseCandidates = new Set<string>();
  try {
    const SAMPLE_PER_PACK = HYDR_SAMPLE_PER_PACK;
    for (const key of window) {
      const packRow = await getPackCatalogRow(db, key);
      if (!packRow) continue;

      const idx = await loadIdxView(env, key, undefined, packRow.packBytes);
      if (!idx) continue;

      const phys = buildPhysicalIndex(idx);
      const stride = Math.max(1, Math.floor(phys.sortedOffsets.length / SAMPLE_PER_PACK));
      let count = 0;
      for (let i = 0; i < phys.sortedOffsets.length && count < SAMPLE_PER_PACK; i += stride) {
        const off = phys.sortedOffsets[i];
        const header = await readPackHeaderEx(env, key, off);
        if (!header) continue;
        examinedObjects++;
        let baseOid: string | undefined;
        if (header.type === PACK_TYPE_OFS_DELTA) {
          const baseOff = off - (header.baseRel || 0);
          const baseIdx = phys.findIndexByOffset(baseOff);
          if (baseIdx !== undefined) baseOid = phys.getOidAtIndex(baseIdx);
        } else if (header.type === PACK_TYPE_REF_DELTA) {
          baseOid = header.baseOid;
        }
        if (baseOid) {
          const q = baseOid.toLowerCase();
          if (!phys.hasOid(q) || !covered.has(q)) {
            baseCandidates.add(q);
          }
        }
        count++;
      }
    }
  } catch {}

  let examinedLoose = 0;
  let looseOnly = 0;
  try {
    const it = await state.storage.list({ prefix: "obj:", limit: 500 });
    for (const k of it.keys()) {
      const oid = String(k).slice(4).toLowerCase();
      examinedLoose++;
      if (!covered.has(oid)) looseOnly++;
    }
  } catch {}

  const estimatedDeltaBases = baseCandidates.size;
  const counts = {
    deltaBases: estimatedDeltaBases,
    looseOnly,
    totalCandidates: looseOnly + estimatedDeltaBases,
    alreadyCovered: 0,
    toPack: looseOnly + estimatedDeltaBases,
  };

  const segments = {
    estimated: Math.max(0, Math.ceil(counts.toPack / HYDR_MAX_OBJS_PER_SEGMENT)),
    maxObjectsPerSegment: HYDR_MAX_OBJS_PER_SEGMENT,
    maxBytesPerSegment: HYDR_SEG_MAX_BYTES,
  };

  const out: HydrationPlan = {
    snapshot: { lastPackKey, packListCount: packListRaw.length || 0 },
    window: { packKeys: window },
    counts,
    segments,
    budgets: { timePerSliceMs: cfg.unpackMaxMs, softSubrequestLimit: HYDR_SOFT_SUBREQ_LIMIT },
    stats: { examinedPacks: window.length, examinedObjects, examinedLoose },
    warnings: ["summary-partial-simple", "summary-sampled-deltas"],
    partial: true,
  };
  log.debug("dryRun:summary", out);
  return out;
}

export async function clearHydrationState(
  state: DurableObjectState,
  env: Env
): Promise<{ clearedWork: boolean; clearedQueue: number; removedPacks: number }> {
  const store = asTypedStorage<RepoStateSchema>(state.storage);
  const log = createLogger(env.LOG_LEVEL, { service: "Hydration", doId: state.id.toString() });
  const db = getDb(state.storage);
  let clearedWork = false;
  let clearedQueue = 0;
  let removedPacks = 0;

  const work = await store.get("hydrationWork");
  if (work) {
    await store.delete("hydrationWork");
    clearedWork = true;
  }
  const queue = (await store.get("hydrationQueue")) || [];
  clearedQueue = Array.isArray(queue) ? queue.length : 0;
  await store.put("hydrationQueue", []);

  const list = (await store.get("packList")) || [];
  const toRemove: string[] = [];
  for (const key of list) {
    const base = normalizePackKey(key);
    if (base.startsWith("pack-hydr-")) toRemove.push(key);
  }

  for (const key of toRemove) {
    try {
      await env.REPO_BUCKET.delete(key);
    } catch (e) {
      log.warn("clear:delete-pack-failed", { key, error: String(e) });
    }
    try {
      await env.REPO_BUCKET.delete(packIndexKey(key));
    } catch (e) {
      log.warn("clear:delete-pack-index-failed", { key, error: String(e) });
    }
    try {
      await deletePackObjects(db, key);
    } catch (e) {
      log.warn("clear:delete-packObjects-failed", { key, error: String(e) });
    }
    removedPacks++;
  }

  if (toRemove.length > 0) {
    const keep = list.filter((k) => !toRemove.includes(k));
    try {
      await store.put("packList", keep);
    } catch (e) {
      log.warn("clear:put-packlist-failed", { error: String(e) });
    }
    try {
      const last = await store.get("lastPackKey");
      if (last && toRemove.includes(String(last))) {
        await store.delete("lastPackKey");
        await store.delete("lastPackOids");
      }
    } catch (e) {
      log.warn("clear:put-lastpack-failed", { error: String(e) });
    }
  }

  return { clearedWork, clearedQueue, removedPacks };
}
