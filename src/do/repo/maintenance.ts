/**
 * Repository idle cleanup
 *
 * This module handles idle cleanup and R2 mirror purging
 * to maintain repository health when repos are no longer in use.
 */

import type { RepoStateSchema } from "./repoState.ts";
import type { Logger } from "@/common/logger.ts";

import { asTypedStorage } from "./repoState.ts";
import { getDb } from "./db/client.ts";
import { getActivePackCatalogCount } from "./db/index.ts";
import { doPrefix } from "@/keys.ts";
import { ensureScheduled } from "./scheduler.ts";
import { getConfig } from "./repoConfig.ts";

/**
 * Handles idle cleanup after alarm fires.
 * Checks if the repository should be cleaned up due to idleness
 * and reschedules the next alarm.
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

    // Check if idle cleanup is needed
    if (await shouldCleanupIdle(ctx, cfg.idleMs, lastAccess)) {
      await performIdleCleanup(ctx, env, logger);
      return;
    }

    // Schedule next alarm via unified scheduler
    await ensureScheduled(ctx, env, now);
  } catch (e) {
    logger?.error("alarm:error", { error: String(e) });
  }
}

/**
 * Determines if the repository should be cleaned up due to idleness.
 * A repo is considered for cleanup if it's been idle beyond the threshold
 * AND appears empty (no refs, unborn/missing HEAD, no active packs in catalog).
 * @param ctx - Durable Object state context
 * @param idleMs - Idle threshold in milliseconds
 * @param lastAccess - Last access timestamp
 * @returns true if cleanup should proceed
 */
async function shouldCleanupIdle(
  ctx: DurableObjectState,
  idleMs: number,
  lastAccess: number | undefined
): Promise<boolean> {
  const now = Date.now();
  const idleExceeded = !lastAccess || now - lastAccess >= idleMs;
  if (!idleExceeded) return false;

  // Check if repo looks empty
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const refs = (await store.get("refs")) ?? [];
  const head = await store.get("head");
  const db = getDb(ctx.storage);
  const catalogCount = await getActivePackCatalogCount(db);

  return refs.length === 0 && (!head || head.unborn || !head.target) && catalogCount === 0;
}

/**
 * Performs complete cleanup of an idle repository.
 * Deletes all DO storage and purges the R2 mirror.
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
 * Purges all R2 objects under this DO's prefix.
 * Continues even if individual deletes fail.
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
