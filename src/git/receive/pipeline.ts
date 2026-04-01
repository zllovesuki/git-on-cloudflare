import type { CacheContext } from "@/cache/index.ts";
import type { Logger } from "@/common/logger.ts";
import type { RepoDurableObject } from "@/do/index.ts";
import type { PackCatalogRow } from "@/do/repo/db/schema.ts";
import type { ReceiveCommand, ReceiveStatus } from "@/git/operations/validation.ts";

import { SubrequestLimiter } from "@/git/operations/limits.ts";
import {
  resolveDeltasAndWriteIdx,
  runPackConnectivityCheck,
  scanPack,
} from "@/git/pack/indexer/index.ts";
import { doPrefix, r2PackKey } from "@/keys.ts";
import { deleteStagedPack, stagePackToR2, type StagedPackUpload } from "./r2Upload.ts";
import { buildReceiveReportStatus, isReceiveAbort, throwIfReceiveAborted } from "./support.ts";

type RepoStub = DurableObjectStub<RepoDurableObject>;

export type ReceivePipelineResult = {
  reportStatusBody: Uint8Array;
  changed: boolean;
  empty: boolean;
  packKey?: string;
  packBytes?: number;
};

export class ReceivePipelineHttpError extends Error {
  readonly status: number;
  readonly reason: string;

  constructor(status: number, reason: string, message: string) {
    super(message);
    this.name = "ReceivePipelineHttpError";
    this.status = status;
    this.reason = reason;
  }
}

type ReceiveCleanupAttempt = "inline" | "retry";

async function abortReceiveLease(args: {
  stub: RepoStub;
  leaseToken: string;
  log: Logger;
  reason: string;
  attempt: ReceiveCleanupAttempt;
}): Promise<boolean> {
  try {
    const cleared = await args.stub.abortReceive(args.leaseToken);
    if (!cleared) {
      args.log.warn("receive:abort-missed", {
        reason: args.reason,
        attempt: args.attempt,
        leaseToken: args.leaseToken,
      });
    }
    return cleared;
  } catch (error) {
    args.log.warn("receive:abort-failed", {
      reason: args.reason,
      attempt: args.attempt,
      leaseToken: args.leaseToken,
      error: String(error),
    });
    return false;
  }
}

async function cleanupStagedPack(args: {
  stagedUpload: StagedPackUpload | undefined;
  log: Logger;
  reason: string;
  attempt: ReceiveCleanupAttempt;
}): Promise<boolean> {
  if (!args.stagedUpload) return true;

  try {
    await deleteStagedPack(args.stagedUpload);
    return true;
  } catch (error) {
    args.log.warn("receive:staged-pack-cleanup-failed", {
      reason: args.reason,
      attempt: args.attempt,
      packKey: args.stagedUpload.packKey,
      error: String(error),
    });
    return false;
  }
}

async function cleanupFailedReceive(args: {
  ctx: ExecutionContext;
  stub: RepoStub;
  leaseToken: string;
  stagedUpload: StagedPackUpload | undefined;
  log: Logger;
  reason: string;
}): Promise<void> {
  const leaseCleared = await abortReceiveLease({
    stub: args.stub,
    leaseToken: args.leaseToken,
    log: args.log,
    reason: args.reason,
    attempt: "inline",
  });
  const stagedPackDeleted = await cleanupStagedPack({
    stagedUpload: args.stagedUpload,
    log: args.log,
    reason: args.reason,
    attempt: "inline",
  });

  if (leaseCleared && stagedPackDeleted) return;

  args.log.warn("receive:cleanup-retry-scheduled", {
    reason: args.reason,
    leaseToken: args.leaseToken,
    packKey: args.stagedUpload?.packKey,
  });
  args.ctx.waitUntil(
    (async () => {
      const retryLeaseCleared =
        leaseCleared ||
        (await abortReceiveLease({
          stub: args.stub,
          leaseToken: args.leaseToken,
          log: args.log,
          reason: args.reason,
          attempt: "retry",
        }));
      const retryStagedPackDeleted =
        stagedPackDeleted ||
        (await cleanupStagedPack({
          stagedUpload: args.stagedUpload,
          log: args.log,
          reason: args.reason,
          attempt: "retry",
        }));

      if (!retryLeaseCleared || !retryStagedPackDeleted) {
        args.log.error("receive:cleanup-retry-incomplete", {
          reason: args.reason,
          leaseCleared: retryLeaseCleared,
          stagedPackDeleted: retryStagedPackDeleted,
          leaseToken: args.leaseToken,
          packKey: args.stagedUpload?.packKey,
        });
      }
    })()
  );
}

type ExecuteReceivePipelineArgs = {
  env: Env;
  repoId: string;
  request: Request;
  ctx: ExecutionContext;
  packStream: ReadableStream<Uint8Array>;
  bytesConsumed: number;
  stub: RepoStub;
  leaseToken: string;
  activeCatalog: PackCatalogRow[];
  commands: ReceiveCommand[];
  log: Logger;
  cacheCtx: CacheContext;
  limiter: SubrequestLimiter;
  countSubrequest(op: string, n?: number): void;
  onProgress?: (message: string) => void;
};

function buildReceiveResult(args: {
  unpackOk: boolean;
  unpackMessage?: string;
  commands: ReceiveCommand[];
  statuses: ReceiveStatus[];
  changed: boolean;
  empty: boolean;
  packKey?: string;
  packBytes?: number;
}): ReceivePipelineResult {
  return {
    reportStatusBody: buildReceiveReportStatus({
      unpackOk: args.unpackOk,
      unpackMessage: args.unpackMessage,
      commands: args.commands,
      statuses: args.statuses,
    }),
    changed: args.changed,
    empty: args.empty,
    packKey: args.packKey,
    packBytes: args.packBytes,
  };
}

export async function executeReceivePipeline(
  args: ExecuteReceivePipelineArgs
): Promise<ReceivePipelineResult> {
  let stagedUpload: StagedPackUpload | undefined;

  try {
    const hasNonDelete = args.commands.some((command) => !/^0{40}$/i.test(command.newOid));
    let stagedPack:
      | {
          packKey: string;
          packBytes: number;
          idxBytes: number;
          objectCount: number;
        }
      | undefined;

    if (hasNonDelete) {
      const packKey = r2PackKey(
        doPrefix(args.stub.id.toString()),
        `pack-rx-${args.leaseToken}.pack`
      );
      stagedUpload = await stagePackToR2({
        env: args.env,
        request: args.request,
        packStream: args.packStream,
        packKey,
        bytesConsumed: args.bytesConsumed,
        limiter: args.limiter,
        countSubrequest: args.countSubrequest,
        onProgress: args.onProgress,
      });
      throwIfReceiveAborted(args.request, args.log, "stage-pack");

      const scanResult = await scanPack({
        env: args.env,
        packKey: stagedUpload.packKey,
        packSize: stagedUpload.packBytes,
        limiter: args.limiter,
        countSubrequest: (n = 1) => args.countSubrequest("r2:scan-pack", n),
        log: args.log,
        signal: args.request.signal,
        onProgress: args.onProgress,
      });
      throwIfReceiveAborted(args.request, args.log, "scan-pack");

      const resolveResult = await resolveDeltasAndWriteIdx({
        env: args.env,
        packKey: stagedUpload.packKey,
        packSize: stagedUpload.packBytes,
        limiter: args.limiter,
        countSubrequest: (n = 1) => args.countSubrequest("r2:resolve-pack", n),
        log: args.log,
        scanResult,
        activeCatalog: args.activeCatalog,
        cacheCtx: args.cacheCtx,
        repoId: args.repoId,
        signal: args.request.signal,
        onProgress: args.onProgress,
      });
      throwIfReceiveAborted(args.request, args.log, "resolve-pack");

      const connectivityStatuses = args.commands.map((command) => ({
        ref: command.ref,
        ok: true,
      }));
      args.onProgress?.("Checking received object connectivity\n");
      await runPackConnectivityCheck({
        env: args.env,
        repoId: args.repoId,
        newPackKey: stagedUpload.packKey,
        newIdxView: resolveResult.idxView,
        newPackSize: stagedUpload.packBytes,
        activeCatalog: args.activeCatalog,
        commands: args.commands,
        statuses: connectivityStatuses,
        log: args.log,
        cacheCtx: args.cacheCtx,
      });
      throwIfReceiveAborted(args.request, args.log, "connectivity-check");

      if (!connectivityStatuses.every((status) => status.ok)) {
        args.countSubrequest("do:abort-receive");
        await cleanupFailedReceive({
          ctx: args.ctx,
          stub: args.stub,
          leaseToken: args.leaseToken,
          stagedUpload,
          log: args.log,
          reason: "connectivity-rejected",
        });
        args.log.warn("receive:connectivity-rejected", {
          conflictCount: connectivityStatuses.filter((status) => !status.ok).length,
        });
        return buildReceiveResult({
          unpackOk: true,
          commands: args.commands,
          statuses: connectivityStatuses,
          changed: false,
          empty: false,
        });
      }

      stagedPack = {
        packKey: stagedUpload.packKey,
        packBytes: stagedUpload.packBytes,
        idxBytes: resolveResult.idxBytes,
        objectCount: resolveResult.objectCount,
      };
    }

    args.countSubrequest("do:finalize-receive");
    throwIfReceiveAborted(args.request, args.log, "finalize-receive");
    args.onProgress?.("Updating refs\n");
    const finalize = await args.stub.finalizeReceive({
      token: args.leaseToken,
      commands: args.commands,
      stagedPack,
    });

    if (finalize.status === "lease_mismatch") {
      await cleanupStagedPack({
        stagedUpload,
        log: args.log,
        reason: "finalize-lease-mismatch",
        attempt: "inline",
      });
      args.log.warn("receive:lease-mismatch", { leaseToken: args.leaseToken });
      throw new ReceivePipelineHttpError(
        503,
        "lease-mismatch",
        "Repository receive lease expired before commit."
      );
    }

    if (finalize.status === "mode_mismatch") {
      await cleanupStagedPack({
        stagedUpload,
        log: args.log,
        reason: "finalize-mode-mismatch",
        attempt: "inline",
      });
      args.log.warn("receive:mode-mismatch", { currentMode: finalize.currentMode });
      throw new ReceivePipelineHttpError(409, "mode-mismatch", finalize.message);
    }

    if (finalize.status === "ref_conflict") {
      await cleanupStagedPack({
        stagedUpload,
        log: args.log,
        reason: "finalize-ref-conflict",
        attempt: "inline",
      });
      args.log.warn("receive:ref-conflict", {
        conflictCount: finalize.statuses.filter((status) => !status.ok).length,
        stage: "finalize",
      });
      return buildReceiveResult({
        unpackOk: true,
        commands: args.commands,
        statuses: finalize.statuses,
        changed: false,
        empty: false,
      });
    }

    if (finalize.status !== "committed") {
      await cleanupStagedPack({
        stagedUpload,
        log: args.log,
        reason: "unexpected-finalize-result",
        attempt: "inline",
      });
      args.log.error("receive:unexpected-finalize-result", { status: finalize.status });
      throw new Error("Unexpected receive finalization result.");
    }

    if (finalize.shouldQueueCompaction) {
      args.log.info("receive:compaction-requested", { repoId: args.repoId });
    }

    return buildReceiveResult({
      unpackOk: true,
      commands: args.commands,
      statuses: finalize.statuses,
      changed: finalize.changed,
      empty: finalize.empty,
      packKey: stagedPack?.packKey,
      packBytes: stagedPack?.packBytes,
    });
  } catch (error) {
    args.countSubrequest("do:abort-receive");
    const aborted = isReceiveAbort(args.request, error);
    await cleanupFailedReceive({
      ctx: args.ctx,
      stub: args.stub,
      leaseToken: args.leaseToken,
      stagedUpload,
      log: args.log,
      reason: aborted ? "receive-aborted" : "receive-error",
    });
    throw error;
  }
}
