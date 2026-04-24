/**
 * Pack operations for repository maintenance
 *
 * This module provides operations for managing packs including
 * removal of specific packs and complete repository purging.
 */

import type { Logger } from "@/common/logger.ts";

import { createLogger } from "@/common";
import { MAX_SIMULTANEOUS_CONNECTIONS, SubrequestLimiter } from "@/git/operations/limits.ts";
import { doPrefix, packIndexKey, packRefsKey } from "@/keys.ts";
import {
  deletePackCatalogRows,
  getDb,
  getPackCatalogCount,
  getPackCatalogRow,
} from "./db/index.ts";
import { getActivePackCatalogSnapshot } from "./catalog.ts";

export type RemovePackResult = {
  removed: boolean;
  deletedPack: boolean;
  deletedIndex: boolean;
  deletedRefs: boolean;
  deletedMetadata: boolean;
  rejected?: "active-pack" | "non-superseded-pack";
  packState?: "active" | "superseded" | "unknown";
};

async function deletePackArtifact(args: {
  bucket: R2Bucket;
  limiter: SubrequestLimiter;
  key: string;
  op: string;
  log: Logger;
  deletedMessage: string;
  failedMessage: string;
}): Promise<boolean> {
  try {
    await args.limiter.run(args.op, async () => {
      await args.bucket.delete(args.key);
    });
    args.log.info(args.deletedMessage, { key: args.key, op: args.op });
    return true;
  } catch (error) {
    args.log.error(args.failedMessage, {
      key: args.key,
      op: args.op,
      error: String(error),
    });
    return false;
  }
}

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
): Promise<RemovePackResult> {
  const log = createLogger(env.LOG_LEVEL, {
    service: "packOperations:removePack",
    doId: ctx.id.toString(),
  });
  const limiter = new SubrequestLimiter(MAX_SIMULTANEOUS_CONNECTIONS);

  const result: RemovePackResult = {
    removed: false,
    deletedPack: false,
    deletedIndex: false,
    deletedRefs: false,
    deletedMetadata: false,
  };

  try {
    const prefix = doPrefix(ctx.id.toString());
    let fullPackKey = packKey;
    const db = getDb(ctx.storage);

    if (!packKey.startsWith(prefix)) {
      fullPackKey = `${prefix}/objects/pack/${packKey}`;
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

    result.deletedPack = await deletePackArtifact({
      bucket: env.REPO_BUCKET,
      limiter,
      key: fullPackKey,
      op: "r2:delete-pack",
      log,
      deletedMessage: "deleted-pack-file",
      failedMessage: "failed-to-delete-pack",
    });

    const indexKey = packIndexKey(fullPackKey);
    result.deletedIndex = await deletePackArtifact({
      bucket: env.REPO_BUCKET,
      limiter,
      key: indexKey,
      op: "r2:delete-pack-idx",
      log,
      deletedMessage: "deleted-index-file",
      failedMessage: "failed-to-delete-index",
    });

    const refsKey = packRefsKey(fullPackKey);
    result.deletedRefs = await deletePackArtifact({
      bucket: env.REPO_BUCKET,
      limiter,
      key: refsKey,
      op: "r2:delete-pack-refs",
      log,
      deletedMessage: "deleted-ref-index-file",
      failedMessage: "failed-to-delete-ref-index",
    });

    // Remove from pack catalog metadata
    await deletePackCatalogRows(db, [fullPackKey]);
    result.deletedMetadata = true;

    result.removed =
      result.deletedPack || result.deletedIndex || result.deletedRefs || result.deletedMetadata;

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
