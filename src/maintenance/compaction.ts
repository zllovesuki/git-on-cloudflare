import type { CacheContext } from "@/cache/index.ts";
import type { Logger } from "@/common/logger.ts";
import type { RepoDurableObject } from "@/do/repo/repoDO.ts";

import { createLogger, getRepoStubByDoId } from "@/common/index.ts";
import { buildCompactionNeededOids } from "@/git/compaction/plan.ts";
import {
  MAX_SIMULTANEOUS_CONNECTIONS,
  SubrequestLimiter,
  countSubrequest,
} from "@/git/operations/limits.ts";
import { scanPack, resolveDeltasAndWriteIdx } from "@/git/pack/indexer/index.ts";
import { rewritePack } from "@/git/pack/rewrite.ts";
import { loadOrderedPackSnapshot } from "@/git/pack/snapshot.ts";
import { deleteStagedPack, stagePackToR2, type StagedPackUpload } from "@/git/receive/r2Upload.ts";
import { doPrefix, packIndexKey, r2PackKey } from "@/keys.ts";

const COMPACTION_SUBREQUEST_BUDGET = 7_500;
const COMPACTION_RETRY_DELAY_SECONDS = 30;
const COMPACTION_CONFLICT_RETRY_DELAY_SECONDS = 10;
const COMPACTION_DELETE_DELAY_SECONDS = 60;

export type CompactionQueueMessage = {
  kind: "compaction";
  doId: string;
  repoId?: string;
};

export type CompactionDeleteQueueMessage = {
  kind: "compaction-delete";
  doId: string;
  repoId?: string;
  packKeys: string[];
};

type RepoMaintenanceMessage<Body> = MessageBatch<Body>["messages"][number];

function buildQueueCacheContext(args: { repoLabel: string; ctx: ExecutionContext }): {
  cacheCtx: CacheContext;
  limiter: SubrequestLimiter;
} {
  const limiter = new SubrequestLimiter(MAX_SIMULTANEOUS_CONNECTIONS);
  return {
    cacheCtx: {
      req: new Request(`https://queue.internal/${encodeURIComponent(args.repoLabel)}/compaction`),
      ctx: args.ctx,
      memo: {
        repoId: args.repoLabel,
        limiter,
        subreqBudget: COMPACTION_SUBREQUEST_BUDGET,
      },
    },
    limiter,
  };
}

function countCompactionSubrequest(cacheCtx: CacheContext, log: Logger, op: string, n = 1): void {
  if (countSubrequest(cacheCtx, n)) return;
  cacheCtx.memo = cacheCtx.memo || {};
  cacheCtx.memo.flags = cacheCtx.memo.flags || new Set();
  const flag = `compaction-soft-budget:${op}`;
  if (cacheCtx.memo.flags.has(flag)) return;
  cacheCtx.memo.flags.add(flag);
  log.warn("soft-budget-exhausted", { op });
}

async function cleanupStagedCompaction(args: {
  stagedUpload: StagedPackUpload | undefined;
  log: ReturnType<typeof createLogger>;
  reason: string;
}) {
  if (!args.stagedUpload) return;
  try {
    await deleteStagedPack(args.stagedUpload);
  } catch (error) {
    args.log.warn("compaction:cleanup-failed", {
      reason: args.reason,
      packKey: args.stagedUpload.packKey,
      error: String(error),
    });
  }
}

async function abortCompactionLease(args: {
  stub: DurableObjectStub<RepoDurableObject>;
  leaseToken: string | undefined;
  log: ReturnType<typeof createLogger>;
  reason: string;
}) {
  if (!args.leaseToken) return;
  try {
    const cleared = await args.stub.abortCompaction(args.leaseToken);
    if (!cleared) {
      args.log.warn("compaction:abort-missed", {
        reason: args.reason,
        leaseToken: args.leaseToken,
      });
    }
  } catch (error) {
    args.log.warn("compaction:abort-failed", {
      reason: args.reason,
      leaseToken: args.leaseToken,
      error: String(error),
    });
  }
}

function queueRetry(
  message: { retry: (options?: { delaySeconds?: number }) => void },
  seconds: number
) {
  message.retry({ delaySeconds: seconds });
}

export async function handleCompactionMessage(
  message: Omit<RepoMaintenanceMessage<CompactionQueueMessage>, "body">,
  body: CompactionQueueMessage,
  env: Env,
  ctx: ExecutionContext
): Promise<void> {
  const repoLabel = body.repoId || `do:${body.doId}`;
  const log = createLogger(env.LOG_LEVEL, {
    service: "CompactionQueue",
    repoId: repoLabel,
    doId: body.doId,
  });
  const stub = getRepoStubByDoId(env, body.doId) as DurableObjectStub<RepoDurableObject>;
  const { cacheCtx, limiter } = buildQueueCacheContext({ repoLabel, ctx });

  let stagedUpload: StagedPackUpload | undefined;
  let leaseToken: string | undefined;

  try {
    countCompactionSubrequest(cacheCtx, log, "do:begin-compaction");
    const begin = await stub.beginCompaction();
    if (!begin.ok) {
      if (begin.status === "busy" && begin.reason === "receive-active") {
        log.info("compaction:busy-retry", { reason: begin.reason });
        queueRetry(message, COMPACTION_CONFLICT_RETRY_DELAY_SECONDS);
        return;
      }

      log.info("compaction:skip", {
        status: begin.status,
        reason: "reason" in begin ? begin.reason : undefined,
      });
      message.ack();
      return;
    }

    leaseToken = begin.lease.token;
    cacheCtx.memo = cacheCtx.memo || {};
    cacheCtx.memo.packCatalog = begin.activeCatalog;

    const snapshotLoad = await loadOrderedPackSnapshot(env, repoLabel, cacheCtx, log);
    if (snapshotLoad.type !== "Ready") {
      log.warn("compaction:snapshot-unavailable", { reason: snapshotLoad.reason });
      await abortCompactionLease({
        stub,
        leaseToken,
        log,
        reason: snapshotLoad.reason,
      });
      queueRetry(message, COMPACTION_RETRY_DELAY_SECONDS);
      return;
    }

    const snapshot = snapshotLoad.snapshot;
    const sourcePackMap = new Map(snapshot.packs.map((pack) => [pack.packKey, pack]));
    const sourcePacks = begin.sourcePacks
      .map((row) => sourcePackMap.get(row.packKey))
      .filter((pack): pack is (typeof snapshot.packs)[number] => pack !== undefined);
    if (sourcePacks.length !== begin.sourcePacks.length) {
      log.warn("compaction:source-pack-missing", {
        expected: begin.sourcePacks.length,
        actual: sourcePacks.length,
      });
      await abortCompactionLease({
        stub,
        leaseToken,
        log,
        reason: "source-pack-missing",
      });
      queueRetry(message, COMPACTION_CONFLICT_RETRY_DELAY_SECONDS);
      return;
    }

    const neededOids = buildCompactionNeededOids(sourcePacks);
    log.info("compaction:rewrite-start", {
      sourceTier: begin.targetTier - 1,
      targetTier: begin.targetTier,
      sourceCount: begin.sourcePacks.length,
      neededCount: neededOids.length,
    });

    const packStream = await rewritePack(env, snapshot, neededOids, {
      limiter,
      countSubrequest: (n) => countCompactionSubrequest(cacheCtx, log, "r2:rewrite-pack", n),
    });
    if (!packStream) {
      throw new Error("Compaction rewrite did not produce an output stream.");
    }

    const packKey = r2PackKey(doPrefix(body.doId), `pack-cmp-${begin.lease.token}.pack`);
    stagedUpload = await stagePackToR2({
      env,
      request: new Request(`https://queue.internal/${encodeURIComponent(repoLabel)}/compact-pack`),
      packStream,
      packKey,
      bytesConsumed: 0,
      limiter,
      countSubrequest: (op, n = 1) => countCompactionSubrequest(cacheCtx, log, op, n),
    });

    const scanResult = await scanPack({
      env,
      packKey: stagedUpload.packKey,
      packSize: stagedUpload.packBytes,
      limiter,
      countSubrequest: (n = 1) => countCompactionSubrequest(cacheCtx, log, "r2:scan-pack", n),
      log,
    });
    const resolveResult = await resolveDeltasAndWriteIdx({
      env,
      packKey: stagedUpload.packKey,
      packSize: stagedUpload.packBytes,
      limiter,
      countSubrequest: (n = 1) => countCompactionSubrequest(cacheCtx, log, "r2:resolve-pack", n),
      log,
      scanResult,
      activeCatalog: begin.activeCatalog,
      cacheCtx,
      repoId: repoLabel,
    });

    countCompactionSubrequest(cacheCtx, log, "do:commit-compaction");
    const commit = await stub.commitCompaction({
      token: begin.lease.token,
      sourcePacks: begin.sourcePacks,
      targetTier: begin.targetTier,
      packsetVersion: begin.packsetVersion,
      stagedPack: {
        packKey: stagedUpload.packKey,
        packBytes: stagedUpload.packBytes,
        idxBytes: resolveResult.idxBytes,
        objectCount: resolveResult.objectCount,
      },
    });

    if (commit.status === "retry") {
      await cleanupStagedCompaction({
        stagedUpload,
        log,
        reason: commit.reason,
      });
      leaseToken = undefined;
      log.info("compaction:retry", { reason: commit.reason });
      queueRetry(message, COMPACTION_CONFLICT_RETRY_DELAY_SECONDS);
      return;
    }

    leaseToken = undefined;
    if (commit.shouldRequeue) {
      ctx.waitUntil(
        env.REPO_MAINT_QUEUE.send({
          kind: "compaction",
          doId: body.doId,
          repoId: body.repoId,
        }).catch((error) => {
          log.warn("compaction:follow-up-enqueue-failed", { error: String(error) });
        })
      );
    }

    if (commit.supersededPackKeys.length > 0) {
      ctx.waitUntil(
        env.REPO_MAINT_QUEUE.send(
          {
            kind: "compaction-delete",
            doId: body.doId,
            repoId: body.repoId,
            packKeys: commit.supersededPackKeys,
          },
          { delaySeconds: COMPACTION_DELETE_DELAY_SECONDS }
        ).catch((error) => {
          log.warn("compaction:delete-enqueue-failed", { error: String(error) });
        })
      );
    }

    log.info("compaction:done", {
      targetPackKey: commit.targetPackKey,
      supersededCount: commit.supersededPackKeys.length,
      shouldRequeue: commit.shouldRequeue,
    });
    message.ack();
  } catch (error) {
    log.error("compaction:error", { error: String(error) });
    await cleanupStagedCompaction({
      stagedUpload,
      log,
      reason: "error",
    });
    await abortCompactionLease({
      stub,
      leaseToken,
      log,
      reason: "error",
    });
    queueRetry(message, COMPACTION_RETRY_DELAY_SECONDS);
  }
}

export async function handleCompactionDeleteMessage(
  message: Omit<RepoMaintenanceMessage<CompactionDeleteQueueMessage>, "body">,
  body: CompactionDeleteQueueMessage,
  env: Env,
  _ctx: ExecutionContext
): Promise<void> {
  const repoLabel = body.repoId || `do:${body.doId}`;
  const log = createLogger(env.LOG_LEVEL, {
    service: "CompactionDeleteQueue",
    repoId: repoLabel,
    doId: body.doId,
  });
  const limiter = new SubrequestLimiter(MAX_SIMULTANEOUS_CONNECTIONS);

  try {
    const keysToDelete: string[] = [];
    for (const packKey of body.packKeys) {
      keysToDelete.push(packKey, packIndexKey(packKey));
    }

    await limiter.run("r2:delete-superseded-packs", async () => {
      await env.REPO_BUCKET.delete(keysToDelete);
    });
    log.info("compaction:delete-complete", {
      packCount: body.packKeys.length,
    });
    message.ack();
  } catch (error) {
    log.warn("compaction:delete-failed", { error: String(error) });
    queueRetry(message, COMPACTION_RETRY_DELAY_SECONDS);
  }
}
