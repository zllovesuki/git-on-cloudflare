import type { CacheContext } from "@/cache/index.ts";
import type {
  LegacyCompatBackfillBatch,
  LegacyCompatBackfillCursor,
} from "@/do/repo/catalog/index.ts";
import type { RepoDurableObject } from "@/do/repo/repoDO.ts";
import type { Logger } from "@/common/logger.ts";

import { createLogger, getRepoStub } from "@/common/index.ts";
import { encodeGitObject } from "@/git/core/objects.ts";
import {
  MAX_SIMULTANEOUS_CONNECTIONS,
  SubrequestLimiter,
  countSubrequest,
} from "@/git/operations/limits.ts";
import { getOidHexAt, loadIdxView, readObject } from "@/git/object-store/index.ts";
import { doPrefix, r2LooseKey } from "@/keys.ts";

const BACKFILL_OBJECT_LIMIT = 512;
const BACKFILL_BATCH_OBJECT_LIMIT = 128;
const BACKFILL_BATCH_BYTES_LIMIT = 4 * 1024 * 1024;
const BACKFILL_SUBREQUEST_BUDGET = 5_000;
const BACKFILL_SUBREQUEST_FLOOR = 64;

export type LegacyCompatBackfillQueueMessage = {
  kind: "legacy-backfill";
  repoId: string;
  jobId: string;
  targetPacksetVersion: number;
};

type RepoMaintenanceMessage<Body> = MessageBatch<Body>["messages"][number];

function buildNextCursor(packIndex: number, objectIndex: number): LegacyCompatBackfillCursor {
  return { packIndex, objectIndex };
}

async function flushLegacyCompatBatch(args: {
  stub: DurableObjectStub<RepoDurableObject>;
  message: LegacyCompatBackfillQueueMessage;
  batch: LegacyCompatBackfillBatch;
  nextProgress: LegacyCompatBackfillCursor;
  cacheCtx: CacheContext;
  log: Logger;
}): Promise<{ status: "ok" } | { status: "stale" | "not_running"; message: string }> {
  if (args.batch.objects.length === 0 && args.batch.packObjects.length === 0) {
    return { status: "ok" };
  }

  countSubrequest(args.cacheCtx);
  const result = await args.stub.storeLegacyCompatBatch({
    jobId: args.message.jobId,
    targetPacksetVersion: args.message.targetPacksetVersion,
    batch: args.batch,
    nextProgress: args.nextProgress,
  });
  if (result.status !== "ok") {
    args.log.info("legacy-compat:stop", {
      status: result.status,
      message: result.message,
      nextPackIndex: args.nextProgress.packIndex,
      nextObjectIndex: args.nextProgress.objectIndex,
    });
    args.batch.objects.length = 0;
    args.batch.packObjects.length = 0;
    return result;
  }

  args.log.debug("legacy-compat:flush", {
    objectCount: args.batch.objects.length,
    packRowCount: args.batch.packObjects.length,
    nextPackIndex: args.nextProgress.packIndex,
    nextObjectIndex: args.nextProgress.objectIndex,
  });
  args.batch.objects.length = 0;
  args.batch.packObjects.length = 0;
  return { status: "ok" };
}

export async function handleLegacyCompatBackfillMessage(
  message: Omit<RepoMaintenanceMessage<LegacyCompatBackfillQueueMessage>, "body">,
  body: LegacyCompatBackfillQueueMessage,
  env: Env,
  ctx: ExecutionContext
): Promise<void> {
  const repoId = body.repoId;
  const stub = getRepoStub(env, repoId);
  const log = createLogger(env.LOG_LEVEL, {
    service: "LegacyCompatBackfill",
    repoId,
  });

  const doId = env.REPO_DO.idFromName(repoId);
  const prefix = doPrefix(doId.toString());
  const limiter = new SubrequestLimiter(MAX_SIMULTANEOUS_CONNECTIONS);
  const cacheCtx: CacheContext = {
    req: new Request(`https://queue.internal/${encodeURIComponent(repoId)}/legacy-compat`),
    ctx,
    memo: {
      repoId,
      limiter,
      subreqBudget: BACKFILL_SUBREQUEST_BUDGET,
    },
  };

  try {
    countSubrequest(cacheCtx);
    const begin = await stub.beginLegacyCompatBackfill(body.jobId, body.targetPacksetVersion);
    if (begin.status !== "ok") {
      log.info("legacy-compat:skip", {
        status: begin.status,
        message: begin.message,
      });
      message.ack();
      return;
    }

    cacheCtx.memo!.packCatalog = begin.activeCatalog;

    const stagedBatch: LegacyCompatBackfillBatch = {
      objects: [],
      packObjects: [],
    };
    let stagedBatchBytes = 0;
    let processedObjects = 0;
    let nextCursor = buildNextCursor(begin.progress.packIndex, begin.progress.objectIndex);
    let needsContinuation = false;

    for (
      let packIndex = begin.progress.packIndex;
      packIndex < begin.activeCatalog.length;
      packIndex++
    ) {
      const pack = begin.activeCatalog[packIndex];
      const idx = await loadIdxView(env, pack.packKey, cacheCtx, pack.packBytes);
      if (!idx) {
        throw new Error(`Missing idx for active pack ${pack.packKey}`);
      }

      const startObjectIndex =
        packIndex === begin.progress.packIndex ? begin.progress.objectIndex : 0;
      let packObjectOids: string[] = [];

      for (let objectIndex = startObjectIndex; objectIndex < idx.count; objectIndex++) {
        if (
          processedObjects >= BACKFILL_OBJECT_LIMIT ||
          (cacheCtx.memo?.subreqBudget ?? 0) < BACKFILL_SUBREQUEST_FLOOR
        ) {
          nextCursor = buildNextCursor(packIndex, objectIndex);
          needsContinuation = true;
          break;
        }

        const oid = getOidHexAt(idx, objectIndex);
        const object = await readObject(env, repoId, oid, cacheCtx);
        if (!object) {
          throw new Error(`Unable to reconstruct packed object ${oid} from ${pack.packKey}`);
        }

        const encoded = await encodeGitObject(object.type, object.payload);
        if (!countSubrequest(cacheCtx, 1)) {
          needsContinuation = true;
          nextCursor = buildNextCursor(packIndex, objectIndex);
          break;
        }
        await limiter.run("r2:put-legacy-loose", async () => {
          await env.REPO_BUCKET.put(r2LooseKey(prefix, oid), encoded.zdata);
        });

        stagedBatch.objects.push({ oid, zdata: encoded.zdata });
        stagedBatchBytes += encoded.zdata.byteLength;
        packObjectOids.push(oid);
        processedObjects++;
        nextCursor = buildNextCursor(packIndex, objectIndex + 1);

        if (
          stagedBatch.objects.length >= BACKFILL_BATCH_OBJECT_LIMIT ||
          stagedBatchBytes >= BACKFILL_BATCH_BYTES_LIMIT
        ) {
          if (packObjectOids.length > 0) {
            stagedBatch.packObjects.push({ packKey: pack.packKey, oids: packObjectOids });
            packObjectOids = [];
          }

          const flushResult = await flushLegacyCompatBatch({
            stub,
            message: body,
            batch: stagedBatch,
            nextProgress: nextCursor,
            cacheCtx,
            log,
          });
          if (flushResult.status !== "ok") {
            message.ack();
            return;
          }
          stagedBatchBytes = 0;
        }
      }

      if (packObjectOids.length > 0) {
        stagedBatch.packObjects.push({ packKey: pack.packKey, oids: packObjectOids });
      }

      if (needsContinuation) break;

      nextCursor = buildNextCursor(packIndex + 1, 0);
    }

    const flushResult = await flushLegacyCompatBatch({
      stub,
      message: body,
      batch: stagedBatch,
      nextProgress: nextCursor,
      cacheCtx,
      log,
    });
    if (flushResult.status !== "ok") {
      message.ack();
      return;
    }

    if (needsContinuation) {
      await env.REPO_MAINT_QUEUE.send(body);
      log.info("legacy-compat:requeue", {
        nextPackIndex: nextCursor.packIndex,
        nextObjectIndex: nextCursor.objectIndex,
        processedObjects,
      });
      message.ack();
      return;
    }

    countSubrequest(cacheCtx);
    const complete = await stub.completeLegacyCompatBackfill(body.jobId, body.targetPacksetVersion);
    if (complete.status !== "ok") {
      log.info("legacy-compat:skip", {
        status: complete.status,
        message: complete.message,
      });
      message.ack();
      return;
    }

    log.info("legacy-compat:done", {
      processedObjects,
    });
    message.ack();
  } catch (error) {
    log.error("legacy-compat:error", { error: String(error) });
    countSubrequest(cacheCtx);
    await stub.failLegacyCompatBackfill(body.jobId, body.targetPacksetVersion, String(error));
    message.ack();
  }
}
