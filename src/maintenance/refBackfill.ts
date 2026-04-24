import type { CacheContext } from "@/cache/index.ts";
import type { Logger } from "@/common/logger.ts";
import type { RepoDurableObject } from "@/do/repo/repoDO.ts";

import { createLogger, getRepoStubByDoId } from "@/common/index.ts";
import {
  MAX_SIMULTANEOUS_CONNECTIONS,
  SubrequestLimiter,
  countSubrequest,
} from "@/git/operations/limits.ts";
import { loadIdxView } from "@/git/object-store/index.ts";
import { resolveDeltasAndWriteIdx, scanPack } from "@/git/pack/indexer/index.ts";
import { loadPackRefView } from "@/git/pack/refIndex.ts";

const REF_BACKFILL_SUBREQUEST_BUDGET = 7_500;
const REF_BACKFILL_RETRY_DELAY_SECONDS = 30;

export type PackRefBackfillQueueMessage = {
  kind: "pack-ref-backfill";
  doId: string;
  repoId?: string;
  packKey: string;
};

type RepoMaintenanceMessage<Body> = MessageBatch<Body>["messages"][number];

function buildBackfillCacheContext(args: { repoLabel: string; ctx: ExecutionContext }): {
  cacheCtx: CacheContext;
  limiter: SubrequestLimiter;
} {
  const limiter = new SubrequestLimiter(MAX_SIMULTANEOUS_CONNECTIONS);
  return {
    cacheCtx: {
      req: new Request(`https://queue.internal/${encodeURIComponent(args.repoLabel)}/pack-refs`),
      ctx: args.ctx,
      memo: {
        repoId: args.repoLabel,
        limiter,
        subreqBudget: REF_BACKFILL_SUBREQUEST_BUDGET,
      },
    },
    limiter,
  };
}

function countBackfillSubrequest(cacheCtx: CacheContext, log: Logger, op: string, n = 1): void {
  if (countSubrequest(cacheCtx, n)) return;
  cacheCtx.memo = cacheCtx.memo || {};
  cacheCtx.memo.flags = cacheCtx.memo.flags || new Set();
  const flag = `ref-backfill-soft-budget:${op}`;
  if (cacheCtx.memo.flags.has(flag)) return;
  cacheCtx.memo.flags.add(flag);
  log.warn("soft-budget-exhausted", { op });
}

function queueRetry(
  message: { retry: (options?: { delaySeconds?: number }) => void },
  seconds: number
): void {
  message.retry({ delaySeconds: seconds });
}

function isDeterministicPackFailure(error: unknown): boolean {
  const message = String(error);
  return (
    message.includes("invalid") ||
    message.includes("mismatch") ||
    message.includes("unsupported") ||
    message.includes("truncated") ||
    message.includes("cannot fit") ||
    message.includes("could not be resolved")
  );
}

export async function handlePackRefBackfillMessage(
  message: Omit<RepoMaintenanceMessage<PackRefBackfillQueueMessage>, "body">,
  body: PackRefBackfillQueueMessage,
  env: Env,
  ctx: ExecutionContext
): Promise<void> {
  const repoLabel = body.repoId || `do:${body.doId}`;
  const log = createLogger(env.LOG_LEVEL, {
    service: "PackRefBackfillQueue",
    repoId: repoLabel,
    doId: body.doId,
  });
  const stub = getRepoStubByDoId(env, body.doId) as DurableObjectStub<RepoDurableObject>;
  const { cacheCtx, limiter } = buildBackfillCacheContext({ repoLabel, ctx });

  try {
    log.info("ref-index:backfill-start", { packKey: body.packKey });

    countBackfillSubrequest(cacheCtx, log, "do:get-active-pack-catalog");
    const activeCatalog = await limiter.run("do:get-active-pack-catalog", async () => {
      return await stub.getActivePackCatalog();
    });
    cacheCtx.memo = cacheCtx.memo || {};
    cacheCtx.memo.packCatalog = activeCatalog;

    const target = activeCatalog.find((row) => row.packKey === body.packKey);
    if (!target) {
      log.info("ref-index:backfill-stale-pack", { packKey: body.packKey });
      message.ack();
      return;
    }

    const idxView = await loadIdxView(env, target.packKey, cacheCtx, target.packBytes);
    if (!idxView) {
      log.warn("ref-index:backfill-invalid-pack", {
        packKey: target.packKey,
        reason: "missing-or-invalid-idx",
      });
      message.ack();
      return;
    }

    const existing = await loadPackRefView(env, target.packKey, idxView, cacheCtx);
    if (existing.type === "Ready") {
      log.info("ref-index:backfill-complete", {
        packKey: target.packKey,
        result: "already-present",
      });
      message.ack();
      return;
    }

    const scanResult = await scanPack({
      env,
      packKey: target.packKey,
      packSize: target.packBytes,
      limiter,
      countSubrequest: (n = 1) => countBackfillSubrequest(cacheCtx, log, "r2:scan-pack", n),
      log,
    });

    const resolveResult = await resolveDeltasAndWriteIdx({
      env,
      packKey: target.packKey,
      packSize: target.packBytes,
      limiter,
      countSubrequest: (n = 1) => countBackfillSubrequest(cacheCtx, log, "r2:resolve-pack", n),
      log,
      scanResult,
      activeCatalog,
      cacheCtx,
      repoId: repoLabel,
      writeIdx: false,
      existingIdxView: idxView,
    });

    log.info("ref-index:backfill-complete", {
      packKey: target.packKey,
      objectCount: resolveResult.objectCount,
      refIndexBytes: resolveResult.refIndexBytes,
    });
    message.ack();
  } catch (error) {
    if (isDeterministicPackFailure(error)) {
      log.warn("ref-index:backfill-invalid-pack", {
        packKey: body.packKey,
        error: String(error),
      });
      message.ack();
      return;
    }

    log.warn("ref-index:backfill-retry", {
      packKey: body.packKey,
      error: String(error),
    });
    queueRetry(message, REF_BACKFILL_RETRY_DELAY_SECONDS);
  }
}
