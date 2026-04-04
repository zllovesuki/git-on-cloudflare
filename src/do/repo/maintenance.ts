/**
 * Repository maintenance and cleanup operations
 *
 * This module handles idle cleanup, R2 mirror management,
 * and periodic pack pruning to maintain repository health.
 */

import type { RepoStateSchema } from "./repoState.ts";
import type { Logger } from "@/common/logger.ts";

import { asTypedStorage } from "./repoState.ts";
import { getDb } from "./db/client.ts";
import {
  deletePackObjects,
  getPackOids as getPackOidsHelper,
  normalizePackKey,
} from "./db/index.ts";
import { r2PackDirPrefix, isPackKey, packIndexKey, doPrefix } from "@/keys.ts";
import { ensureScheduled } from "./scheduler.ts";
import { getConfig } from "./repoConfig.ts";
import { enqueueHydrationTask } from "./hydration/index.ts";
import { calculateStableEpochs } from "./packs.ts";

/**
 * Handles idle cleanup and periodic maintenance tasks
 * Checks if the repository should be cleaned up due to idleness,
 * and performs periodic maintenance (pack pruning) if due
 * @param ctx - Durable Object state context
 * @param env - Worker environment
 * @param logger - Logger instance
 */
export async function handleIdleAndMaintenance(
  ctx: DurableObjectState,
  env: Env,
  logger?: Logger
): Promise<void> {
  try {
    const cfg = getConfig(env);
    const now = Date.now();
    const store = asTypedStorage<RepoStateSchema>(ctx.storage);
    const lastAccess = await store.get("lastAccessMs");
    const lastMaint = await store.get("lastMaintenanceMs");

    // Check if idle cleanup is needed
    if (await shouldCleanupIdle(store, cfg.idleMs, lastAccess)) {
      await performIdleCleanup(ctx, env, logger);
      return;
    }

    // Check if maintenance is due
    if (isMaintenanceDue(lastMaint, now, cfg.maintMs)) {
      await performMaintenance(ctx, env, cfg.keepPacks, now, logger);
    }

    // Schedule next alarm via unified scheduler
    await ensureScheduled(ctx, env, now);
  } catch (e) {
    logger?.error("alarm:error", { error: String(e) });
  }
}

/**
 * Determines if the repository should be cleaned up due to idleness
 * A repo is considered for cleanup if it's been idle beyond the threshold
 * AND appears empty (no refs, unborn/missing HEAD, no packs)
 * @param store - The typed storage instance
 * @param idleMs - Idle threshold in milliseconds
 * @param lastAccess - Last access timestamp
 * @returns true if cleanup should proceed
 */
async function shouldCleanupIdle(
  store: ReturnType<typeof asTypedStorage<RepoStateSchema>>,
  idleMs: number,
  lastAccess: number | undefined
): Promise<boolean> {
  const now = Date.now();
  const idleExceeded = !lastAccess || now - lastAccess >= idleMs;
  if (!idleExceeded) return false;

  // Check if repo looks empty
  const refs = (await store.get("refs")) ?? [];
  const head = await store.get("head");
  const lastPackKey = await store.get("lastPackKey");

  return refs.length === 0 && (!head || head.unborn || !head.target) && !lastPackKey;
}

/**
 * Performs complete cleanup of an idle repository
 * Deletes all DO storage and purges the R2 mirror
 * @param ctx - Durable Object state context
 * @param env - Worker environment
 * @param logger - Logger instance
 */
async function performIdleCleanup(
  ctx: DurableObjectState,
  env: Env,
  logger?: Logger
): Promise<void> {
  const storage = ctx.storage;

  // Purge DO storage
  try {
    await storage.deleteAll();
  } catch (e) {
    logger?.error("cleanup:delete-storage-failed", { error: String(e) });
  }

  // Purge R2 mirror
  const prefix = doPrefix(ctx.id.toString());
  await purgeR2Mirror(env, prefix, logger);

  // Clear the alarm after cleanup
  try {
    await storage.deleteAlarm();
  } catch (e) {
    logger?.warn("cleanup:delete-alarm-failed", { error: String(e) });
  }
}

/**
 * Purges all R2 objects under this DO's prefix
 * Continues even if individual deletes fail
 * @param env - Worker environment
 * @param prefix - Repository prefix (do/<id>)
 * @param logger - Logger instance
 */
async function purgeR2Mirror(env: Env, prefix: string, logger?: Logger): Promise<void> {
  try {
    const pfx = `${prefix}/`;
    let cursor: string | undefined = undefined;

    do {
      const res: R2Objects = await env.REPO_BUCKET.list({ prefix: pfx, cursor });
      const objects: R2Object[] = (res && res.objects) || [];

      for (const obj of objects) {
        try {
          await env.REPO_BUCKET.delete(obj.key);
        } catch (e) {
          logger?.warn("cleanup:delete-r2-object-failed", {
            key: obj.key,
            error: String(e),
          });
        }
      }

      cursor = res.truncated ? res.cursor : undefined;
    } while (cursor);
  } catch (e) {
    logger?.error("cleanup:purge-r2-failed", { error: String(e) });
  }
}

/**
 * Check if maintenance is due
 * @param lastMaint - Last maintenance timestamp
 * @param now - Current timestamp
 * @param maintMs - Maintenance interval in milliseconds
 * @returns true if maintenance is due
 */
function isMaintenanceDue(lastMaint: number | undefined, now: number, maintMs: number): boolean {
  return !lastMaint || now - lastMaint >= maintMs;
}

/**
 * Perform periodic maintenance
 * @param ctx - Durable Object state context
 * @param env - Worker environment
 * @param keepPacks - Number of packs to keep
 * @param now - Current timestamp
 * @param logger - Logger instance
 */
async function performMaintenance(
  ctx: DurableObjectState,
  env: Env,
  keepPacks: number,
  now: number,
  logger?: Logger
): Promise<void> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const repoStorageMode = (await store.get("repoStorageMode")) || "legacy";
  if (repoStorageMode === "streaming") {
    logger?.debug("maintenance:skipped-streaming-mode", {});
    return;
  }
  try {
    // Deletes older packs beyond the keep-window from both DO metadata and R2,
    // and keeps `lastPackKey/lastPackOids` consistent
    const prefix = doPrefix(ctx.id.toString());
    await runMaintenance(ctx, env, prefix, keepPacks, logger);
    await store.put("lastMaintenanceMs", now);
  } catch (e) {
    logger?.error("maintenance:failed", { error: String(e) });
  }
}

/**
 * Run pack maintenance, pruning old packs
 * @param ctx - Durable Object state context
 * @param env - Worker environment
 * @param prefix - Repository prefix
 * @param keepPacks - Number of packs to keep
 * @param logger - Logger instance
 */
async function runMaintenance(
  ctx: DurableObjectState,
  env: Env,
  prefix: string,
  keepPacks: number,
  logger?: Logger
): Promise<void> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const db = getDb(ctx.storage);

  // Ensure packList exists
  const packList = (await store.get("packList")) ?? [];
  if (packList.length === 0) return;

  // Prune safety: avoid pruning before hydration has produced at least one segment.
  // If no hydration packs exist (basename starts with 'pack-hydr-'), skip pruning now.
  try {
    const hasHydration = Array.isArray(packList)
      ? packList.some((k: string) => normalizePackKey(k).startsWith("pack-hydr-"))
      : false;
    if (!hasHydration) {
      logger?.warn?.("maintenance:prune-skipped:no-hydration", { count: packList.length });
      return;
    }
  } catch {}

  // Determine which packs to keep using epoch-aware selection with soft KEEP_PACKS
  const currentLast = (await store.get("lastPackKey")) || undefined;
  const { keepSet } = calculateStableEpochs(packList, keepPacks, currentLast);
  const removed = packList.filter((k) => !keepSet.has(k));
  const newList = packList.filter((k) => keepSet.has(k));
  // Track whether any hydration packs (pack-hydr-*) were pruned; used to decide whether we
  // need to enqueue a follow-up hydration job. This avoids oscillation when only normal packs
  // are pruned but hydration coverage remains intact.
  const removedHydra = removed.filter((k) => normalizePackKey(k).startsWith("pack-hydr-"));

  // Trim packList in storage while preserving additional kept keys
  if (removed.length > 0) await store.put("packList", newList);

  // Adjust lastPackKey/lastPackOids if needed
  const lastPackKey = await store.get("lastPackKey");
  if (!lastPackKey || !keepSet.has(lastPackKey)) {
    // Choose the newest kept pack as the latest reference
    const newest = newList[0];
    if (newest) {
      await store.put("lastPackKey", newest);
      // Load OIDs from SQLite for the newest pack via DAL
      const oids = await getPackOidsHelper(db, newest);
      await store.put("lastPackOids", oids.slice(0, 10000));
    } else {
      // No packs remain
      await store.delete("lastPackKey");
      await store.delete("lastPackOids");
    }
  }

  // Delete pack_objects entries for removed packs from SQLite
  for (const k of removed) {
    try {
      await deletePackObjects(db, k);
    } catch (e) {
      logger?.warn("maintenance:delete-packObjects-failed", { key: k, error: String(e) });
    }
  }

  // Proactively delete removed packs (.pack and .idx) by base key
  for (const base of removed) {
    try {
      await env.REPO_BUCKET.delete(base);
    } catch {}
    try {
      await env.REPO_BUCKET.delete(packIndexKey(base));
    } catch {}
  }

  // Sweep R2 pack files not in keep set
  try {
    const pfx = r2PackDirPrefix(prefix);
    let cursor: string | undefined = undefined;
    const packKeys: string[] = [];

    do {
      const res: any = await env.REPO_BUCKET.list({ prefix: pfx, cursor });
      const objects: any[] = (res && res.objects) || [];
      for (const obj of objects) {
        const key: string = obj.key;
        if (isPackKey(key)) packKeys.push(key);
      }
      cursor = res && res.truncated ? res.cursor : undefined;
    } while (cursor);

    for (const packKey of packKeys) {
      if (!keepSet.has(packKey)) {
        try {
          await env.REPO_BUCKET.delete(packKey);
        } catch {}
        try {
          await env.REPO_BUCKET.delete(packIndexKey(packKey));
        } catch {}
      }
    }
  } catch {}

  // Enqueue a hydration job only when hydration packs were pruned. If pruning removed
  // only normal packs, hydration coverage remains and we avoid unnecessary re-hydration.
  if (removedHydra.length > 0) {
    try {
      await enqueueHydrationTask(ctx, env, { reason: "post-maint" });
    } catch (e) {
      logger?.warn?.("maintenance:enqueue-hydration-failed", { error: String(e) });
    }
  }
}
