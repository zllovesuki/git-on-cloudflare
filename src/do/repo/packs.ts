/**
 * Pack management operations for Git repository
 *
 * This module handles pack metadata and membership tracking,
 * including pack lists, OID memberships, and batch operations.
 */

import type { RepoStateSchema } from "./repoState.ts";

import { asTypedStorage } from "./repoState.ts";
import { getConfig } from "./repoConfig.ts";
import {
  getDb,
  getPackOids as getPackOidsHelper,
  deletePackObjects,
  normalizePackKey,
} from "./db/index.ts";

/**
 * Get list of pack keys (newest first)
 * @param ctx - Durable Object state context
 * @param env - Worker environment for configuration
 * @returns Array of pack keys, limited to configured packListMax
 */
export async function getPacks(ctx: DurableObjectState, env: Env): Promise<string[]> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const cfg = getConfig(env);
  const list = ((await store.get("packList")) || []).slice(0, cfg.packListMax);
  return list;
}

/**
 * Remove pack from list and clean up its metadata
 * @param ctx - Durable Object state context
 * @param packKey - Pack key to remove
 */
export async function removePackFromList(ctx: DurableObjectState, packKey: string): Promise<void> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const db = getDb(ctx.storage);

  // Remove from pack list
  const packList = (await store.get("packList")) || [];
  const newList = packList.filter((k) => k !== packKey);
  await store.put("packList", newList);

  // Clean up pack OIDs
  await deletePackObjects(db, packKey);

  // Update lastPackKey if necessary
  const lastPackKey = await store.get("lastPackKey");
  if (lastPackKey === packKey) {
    if (newList.length > 0) {
      const newest = newList[0];
      await store.put("lastPackKey", newest);
      // Load OIDs from SQLite for the newest pack
      const oids = await getPackOidsHelper(db, newest);
      await store.put("lastPackOids", oids.slice(0, 10000));
    } else {
      await store.delete("lastPackKey");
      await store.delete("lastPackOids");
    }
  }
}

/**
 * Parse epoch identifier from a hydration pack key.
 * Returns e<...> when key matches pack-hydr-e<epoch>-<seq>.pack; otherwise null.
 */
export function parseEpochFromHydrPackKey(key: string): string | null {
  try {
    const base = normalizePackKey(key);
    const m = base.match(/pack-hydr-(e[0-9A-Za-z]+)-\d+\.pack$/);
    return m ? m[1] : null;
  } catch {
    return null;
  }
}

/**
 * Derive epoch id from hydration workId. Example: hydr-1727082945000 -> e1727082945000
 */
export function getEpochFromWorkId(workId: string): string {
  if (workId && workId.startsWith("hydr-")) {
    return `e${workId.slice(5)}`;
  }
  return `e${workId || Date.now()}`;
}

/**
 * Calculate stable epochs and an epoch-aware keep set for maintenance.
 * - Units: [last?], then hydration epochs (atomic groups), then normal packs
 * - Do not split epochs. If next epoch would cross keepPacks, include entire epoch and stop.
 * - Legacy hydration packs (no epoch) are not considered part of stable epochs or keep set
 *   when tight on space (they will naturally fall out when keep horizon is small).
 */
export function calculateStableEpochs(
  packList: string[],
  keepPacks: number,
  lastPackKey?: string
): { stableEpochs: string[]; keepSet: Set<string> } {
  const seen = new Set<string>();
  const hydrationByEpoch = new Map<string, string[]>();
  const normals: string[] = [];

  for (const k of packList) {
    if (seen.has(k)) continue;
    seen.add(k);
    const epoch = parseEpochFromHydrPackKey(k);
    if (epoch) {
      const arr = hydrationByEpoch.get(epoch) || [];
      arr.push(k);
      hydrationByEpoch.set(epoch, arr);
    } else if (k !== lastPackKey) {
      // Exclude legacy hydration (pack-hydr-*) from normals to avoid prioritizing them
      const base = normalizePackKey(k);
      if (!base.startsWith("pack-hydr-")) normals.push(k);
    }
  }

  type Unit = { kind: "last" | "epoch" | "normal"; id?: string; keys: string[] };
  const units: Unit[] = [];
  if (lastPackKey) units.push({ kind: "last", keys: [lastPackKey] });
  // Preserve list order for epochs as they appear in packList
  for (const k of packList) {
    const e = parseEpochFromHydrPackKey(k);
    if (e && hydrationByEpoch.has(e)) {
      const keys = hydrationByEpoch.get(e)!;
      units.push({ kind: "epoch", id: e, keys });
      hydrationByEpoch.delete(e);
    }
  }
  for (const n of normals) units.push({ kind: "normal", keys: [n] });

  let kept = 0;
  const keepSet = new Set<string>();
  const stableEpochs: string[] = [];

  for (const u of units) {
    const weight = u.keys.length;
    if (u.kind === "epoch") {
      if (kept + weight <= keepPacks) {
        for (const k of u.keys) keepSet.add(k);
        stableEpochs.push(u.id!);
        kept += weight;
      } else if (kept < keepPacks) {
        for (const k of u.keys) keepSet.add(k);
        stableEpochs.push(u.id!);
        kept += weight;
        break;
      } else {
        break;
      }
    } else {
      if (kept + weight <= keepPacks) {
        for (const k of u.keys) keepSet.add(k);
        kept += weight;
      } else {
        break;
      }
    }
  }

  return { stableEpochs, keepSet };
}
