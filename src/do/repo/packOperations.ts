/**
 * Pack operations for repository maintenance
 *
 * This module provides operations for managing packs including
 * removal of specific packs and complete repository purging.
 */

import type { RepoStateSchema } from "./repoState.ts";

import { createLogger } from "@/common";
import { doPrefix, packIndexKey } from "@/keys.ts";
import { asTypedStorage } from "./repoState.ts";
import { removePackFromList } from "./packs.ts";
import {
  deletePackCatalogRows,
  getDb,
  getPackCatalogCount,
  getPackCatalogRow,
} from "./db/index.ts";
import { getActivePackCatalogSnapshot } from "./catalog.ts";

/**
 * Remove a specific pack file and its associated data
 * @param ctx - Durable Object state context
 * @param env - Worker environment
 * @param packKey - The pack key to remove (can be either short name or full R2 key)
 * @returns Object with removal statistics
 */
export async function removePack(
  ctx: DurableObjectState,
  env: Env,
  packKey: string
): Promise<{
  removed: boolean;
  deletedPack: boolean;
  deletedIndex: boolean;
  deletedMetadata: boolean;
  rejected?: "active-pack" | "non-superseded-pack";
  packState?: "active" | "superseded" | "unknown";
}> {
  const log = createLogger(env.LOG_LEVEL, {
    service: "packOperations:removePack",
    doId: ctx.id.toString(),
  });

  const result: {
    removed: boolean;
    deletedPack: boolean;
    deletedIndex: boolean;
    deletedMetadata: boolean;
    rejected?: "active-pack" | "non-superseded-pack";
    packState?: "active" | "superseded" | "unknown";
  } = {
    removed: false,
    deletedPack: false,
    deletedIndex: false,
    deletedMetadata: false,
  };

  try {
    const prefix = doPrefix(ctx.id.toString());
    let fullPackKey = packKey;
    const db = getDb(ctx.storage);

    if (!packKey.startsWith(prefix)) {
      const store = asTypedStorage<RepoStateSchema>(ctx.storage);
      const packList = (await store.get("packList")) || [];
      const matchingKey = packList.find((k) => k.endsWith(packKey) || k.endsWith(`/${packKey}`));

      if (matchingKey) {
        fullPackKey = matchingKey;
      } else {
        fullPackKey = `${prefix}/objects/pack/${packKey}`;
      }
    }

    if ((await getPackCatalogCount(db)) === 0) {
      await getActivePackCatalogSnapshot(ctx, env, prefix, log);
    }

    const packCatalogRow = await getPackCatalogRow(db, fullPackKey);
    const packState: "active" | "superseded" | "unknown" =
      packCatalogRow?.state === "active"
        ? "active"
        : packCatalogRow?.state === "superseded"
          ? "superseded"
          : "unknown";
    result.packState = packState;
    if (packState !== "superseded") {
      const rejected = packState === "active" ? "active-pack" : "non-superseded-pack";
      log.warn("reject-pack-delete", {
        packKey: fullPackKey,
        packState,
        rejected,
      });
      return {
        ...result,
        rejected,
      };
    }

    log.info("removing-pack", { packKey: fullPackKey });

    // Delete the pack file from R2
    try {
      await env.REPO_BUCKET.delete(fullPackKey);
      result.deletedPack = true;
      log.info("deleted-pack-file", { key: fullPackKey });
    } catch (e) {
      log.error("failed-to-delete-pack", { key: fullPackKey, error: String(e) });
    }

    // Delete the index file from R2 if it exists
    const indexKey = packIndexKey(fullPackKey);
    try {
      await env.REPO_BUCKET.delete(indexKey);
      result.deletedIndex = true;
      log.info("deleted-index-file", { key: indexKey });
    } catch (e) {
      log.debug("no-index-to-delete", { key: indexKey });
    }

    // Remove from DO metadata. The legacy mirrors and deprecated pack_objects
    // table still need cleanup during the rollout window, but the pack catalog
    // remains the authoritative state machine.
    await deletePackCatalogRows(db, [fullPackKey]);
    await removePackFromList(ctx, fullPackKey);
    result.deletedMetadata = true;

    result.removed = result.deletedPack || result.deletedMetadata;

    log.info("pack-removal-complete", result);
  } catch (e) {
    log.error("pack-removal-error", { packKey, error: String(e) });
    throw e;
  }

  return result;
}

/**
 * DANGEROUS: Completely purge all repository data
 * Deletes all R2 objects and all DO storage
 * @param ctx - Durable Object state context
 * @param env - Worker environment
 * @returns Statistics about deleted objects
 */
export async function purgeRepo(
  ctx: DurableObjectState,
  env: Env
): Promise<{ deletedR2: number; deletedDO: boolean }> {
  const log = createLogger(env.LOG_LEVEL, {
    service: "packOperations:purgeRepo",
    doId: ctx.id.toString(),
  });

  let deletedR2 = 0;
  const prefix = doPrefix(ctx.id.toString());

  // Delete all R2 objects for this repo
  try {
    // List and delete all objects under do/<id>/
    let cursor: string | undefined;
    do {
      const res = await env.REPO_BUCKET.list({ prefix, cursor });
      const objects = res.objects || [];

      if (objects.length > 0) {
        // Delete in batches
        const keys = objects.map((o) => o.key);
        await env.REPO_BUCKET.delete(keys);
        deletedR2 += keys.length;
        log.info("purge:deleted-r2-batch", { count: keys.length });
      }

      cursor = res.truncated ? res.cursor : undefined;
    } while (cursor);
  } catch (e) {
    log.error("purge:r2-delete-error", { error: String(e) });
  }

  // Delete all DO storage
  await ctx.storage.deleteAll();
  log.info("purge:deleted-do-storage");

  return { deletedR2, deletedDO: true };
}
