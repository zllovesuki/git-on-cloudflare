import type { CacheContext } from "@/cache/index.ts";
import type { Logger } from "@/common/logger.ts";

import { asBodyInit, clientAbortedResponse, createLogger, getRepoStub } from "@/common/index.ts";
import {
  MAX_SIMULTANEOUS_CONNECTIONS,
  SubrequestLimiter,
  countSubrequest,
} from "@/git/operations/limits.ts";
import {
  isValidRefName,
  type ReceiveCommand,
  validateReceiveCommands,
} from "@/git/operations/validation.ts";
import {
  resolveDeltasAndWriteIdx,
  runPackConnectivityCheck,
  scanPack,
} from "@/git/pack/indexer/index.ts";
import { logOnce } from "@/git/object-store/support.ts";
import { doPrefix, r2PackKey } from "@/keys.ts";
import { readPktSectionStream } from "./pktSectionStream.ts";
import { deleteStagedPack, stagePackToR2, type StagedPackUpload } from "./r2Upload.ts";
import {
  buildReceiveReportStatus,
  invalidRefReport,
  isReceiveAbort,
  parseReceiveCommands,
  throwIfReceiveAborted,
} from "./support.ts";

const RECEIVE_SUBREQUEST_BUDGET = 5_000;

function countReceiveSubrequest(cacheCtx: CacheContext, log: Logger, op: string, n = 1) {
  if (countSubrequest(cacheCtx, n)) return;
  logOnce(cacheCtx, `receive-soft-budget:${op}`, () => {
    log.warn("soft-budget-exhausted", { op });
  });
}

function logReceiveEnd(log: Logger, status: number, extra?: Record<string, unknown>) {
  log.info("receive:end", { status, ...extra });
}

function buildReceiveCacheContext(
  request: Request,
  ctx: ExecutionContext,
  repoId: string
): { cacheCtx: CacheContext; limiter: SubrequestLimiter } {
  const limiter = new SubrequestLimiter(MAX_SIMULTANEOUS_CONNECTIONS);
  return {
    cacheCtx: {
      req: request,
      ctx,
      memo: {
        repoId,
        limiter,
        subreqBudget: RECEIVE_SUBREQUEST_BUDGET,
      },
    },
    limiter,
  };
}

type ReceiveCleanupAttempt = "inline" | "retry";

async function abortReceiveLease(args: {
  stub: ReturnType<typeof getRepoStub>;
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
  stub: ReturnType<typeof getRepoStub>;
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

export async function handleStreamingReceivePackPOST(
  env: Env,
  repoId: string,
  request: Request,
  ctx: ExecutionContext
): Promise<Response> {
  const stub = getRepoStub(env, repoId);
  const log = createLogger(env.LOG_LEVEL, {
    service: "StreamingReceivePack",
    repoId,
  });
  log.info("receive:start", { mode: "streaming" });

  if (!request.body) {
    logReceiveEnd(log, 400, { reason: "missing-body" });
    return new Response("Missing receive-pack request body\n", { status: 400 });
  }
  if (request.signal.aborted) {
    logReceiveEnd(log, 499, { reason: "client-aborted" });
    return clientAbortedResponse();
  }

  const { cacheCtx, limiter } = buildReceiveCacheContext(request, ctx, repoId);

  countReceiveSubrequest(cacheCtx, log, "do:begin-receive");
  const begin = await stub.beginReceive();
  if (!begin.ok) {
    log.warn("receive:block-busy", { retryAfter: begin.retryAfter, mode: "streaming" });
    logReceiveEnd(log, 503, { reason: "receive-lease-active" });
    return new Response("Repository is busy receiving; please retry shortly.\n", {
      status: 503,
      headers: {
        "Retry-After": String(begin.retryAfter),
        "Content-Type": "text/plain; charset=utf-8",
      },
    });
  }

  if (begin.repoStorageMode !== "streaming") {
    countReceiveSubrequest(cacheCtx, log, "do:abort-receive");
    await cleanupFailedReceive({
      ctx,
      stub,
      leaseToken: begin.lease.token,
      stagedUpload: undefined,
      log,
      reason: "mode-mismatch-before-read",
    });
    log.warn("receive:mode-mismatch", {
      expectedMode: "streaming",
      currentMode: begin.repoStorageMode,
    });
    logReceiveEnd(log, 409, { reason: "mode-mismatch" });
    return new Response("Repository is not currently configured for streaming receive.\n", {
      status: 409,
      headers: { "Content-Type": "text/plain; charset=utf-8" },
    });
  }

  let stagedUpload: StagedPackUpload | undefined;
  try {
    const { lines, bytesConsumed, packStream } = await readPktSectionStream(request.body);
    throwIfReceiveAborted(request, log, "read-command-section");
    const commands = parseReceiveCommands(lines);

    const invalidCommand = commands.find((command) => !isValidRefName(command.ref));
    if (invalidCommand) {
      countReceiveSubrequest(cacheCtx, log, "do:abort-receive");
      await cleanupFailedReceive({
        ctx,
        stub,
        leaseToken: begin.lease.token,
        stagedUpload: undefined,
        log,
        reason: "invalid-ref",
      });
      log.warn("receive:invalid-ref", { ref: invalidCommand.ref });
      logReceiveEnd(log, 200, { reason: "invalid-ref", changed: false, empty: false });
      return invalidRefReport(commands, "invalid-ref");
    }

    const preflightStatuses = validateReceiveCommands(begin.refs, commands);
    if (!preflightStatuses.every((status) => status.ok)) {
      countReceiveSubrequest(cacheCtx, log, "do:abort-receive");
      await cleanupFailedReceive({
        ctx,
        stub,
        leaseToken: begin.lease.token,
        stagedUpload: undefined,
        log,
        reason: "preflight-ref-conflict",
      });
      log.warn("receive:ref-conflict", {
        conflictCount: preflightStatuses.filter((status) => !status.ok).length,
        stage: "preflight",
      });
      const response = new Response(
        asBodyInit(
          buildReceiveReportStatus({
            unpackOk: true,
            commands,
            statuses: preflightStatuses,
          })
        ),
        {
          status: 200,
          headers: {
            "Content-Type": "application/x-git-receive-pack-result",
            "Cache-Control": "no-cache",
            "X-Repo-Changed": "0",
            "X-Repo-Empty": "0",
          },
        }
      );
      logReceiveEnd(log, response.status, { reason: "preflight-ref-conflict", changed: false });
      return response;
    }

    const hasNonDelete = commands.some((command) => !/^0{40}$/i.test(command.newOid));
    let stagedPack:
      | {
          packKey: string;
          packBytes: number;
          idxBytes: number;
          objectCount: number;
        }
      | undefined;

    if (hasNonDelete) {
      const packKey = r2PackKey(doPrefix(stub.id.toString()), `pack-rx-${begin.lease.token}.pack`);
      stagedUpload = await stagePackToR2({
        env,
        request,
        packStream,
        packKey,
        bytesConsumed,
        limiter,
        countSubrequest: (op, n = 1) => countReceiveSubrequest(cacheCtx, log, op, n),
      });
      throwIfReceiveAborted(request, log, "stage-pack");

      const scanResult = await scanPack({
        env,
        packKey: stagedUpload.packKey,
        packSize: stagedUpload.packBytes,
        limiter,
        countSubrequest: (n = 1) => countReceiveSubrequest(cacheCtx, log, "r2:scan-pack", n),
        log,
        signal: request.signal,
      });
      throwIfReceiveAborted(request, log, "scan-pack");

      const resolveResult = await resolveDeltasAndWriteIdx({
        env,
        packKey: stagedUpload.packKey,
        packSize: stagedUpload.packBytes,
        limiter,
        countSubrequest: (n = 1) => countReceiveSubrequest(cacheCtx, log, "r2:resolve-pack", n),
        log,
        scanResult,
        activeCatalog: begin.activeCatalog,
        cacheCtx,
        repoId,
        signal: request.signal,
      });
      throwIfReceiveAborted(request, log, "resolve-pack");

      const connectivityStatuses = preflightStatuses.map((status) => ({ ...status }));
      await runPackConnectivityCheck({
        env,
        repoId,
        newPackKey: stagedUpload.packKey,
        newIdxView: resolveResult.idxView,
        newPackSize: stagedUpload.packBytes,
        activeCatalog: begin.activeCatalog,
        commands,
        statuses: connectivityStatuses,
        log,
        cacheCtx,
      });
      throwIfReceiveAborted(request, log, "connectivity-check");

      if (!connectivityStatuses.every((status) => status.ok)) {
        countReceiveSubrequest(cacheCtx, log, "do:abort-receive");
        await cleanupFailedReceive({
          ctx,
          stub,
          leaseToken: begin.lease.token,
          stagedUpload,
          log,
          reason: "connectivity-rejected",
        });
        log.warn("receive:connectivity-rejected", {
          conflictCount: connectivityStatuses.filter((status) => !status.ok).length,
        });
        const response = new Response(
          asBodyInit(
            buildReceiveReportStatus({
              unpackOk: true,
              commands,
              statuses: connectivityStatuses,
            })
          ),
          {
            status: 200,
            headers: {
              "Content-Type": "application/x-git-receive-pack-result",
              "Cache-Control": "no-cache",
              "X-Repo-Changed": "0",
              "X-Repo-Empty": "0",
            },
          }
        );
        logReceiveEnd(log, response.status, { reason: "connectivity-rejected", changed: false });
        return response;
      }

      stagedPack = {
        packKey: stagedUpload.packKey,
        packBytes: stagedUpload.packBytes,
        idxBytes: resolveResult.idxBytes,
        objectCount: resolveResult.objectCount,
      };
    }

    countReceiveSubrequest(cacheCtx, log, "do:finalize-receive");
    throwIfReceiveAborted(request, log, "finalize-receive");
    const finalize = await stub.finalizeReceive({
      token: begin.lease.token,
      commands,
      stagedPack,
    });

    if (finalize.status === "lease_mismatch") {
      await cleanupStagedPack({
        stagedUpload,
        log,
        reason: "finalize-lease-mismatch",
        attempt: "inline",
      });
      log.warn("receive:lease-mismatch", { leaseToken: begin.lease.token });
      logReceiveEnd(log, 503, { reason: "lease-mismatch" });
      return new Response("Repository receive lease expired before commit.\n", {
        status: 503,
        headers: {
          "Retry-After": "10",
          "Content-Type": "text/plain; charset=utf-8",
        },
      });
    }

    if (finalize.status === "mode_mismatch") {
      await cleanupStagedPack({
        stagedUpload,
        log,
        reason: "finalize-mode-mismatch",
        attempt: "inline",
      });
      log.warn("receive:mode-mismatch", { currentMode: finalize.currentMode });
      const response = new Response(`${finalize.message}\n`, {
        status: 409,
        headers: { "Content-Type": "text/plain; charset=utf-8" },
      });
      logReceiveEnd(log, response.status, { reason: "mode-mismatch-after-begin" });
      return response;
    }

    if (finalize.status === "ref_conflict") {
      await cleanupStagedPack({
        stagedUpload,
        log,
        reason: "finalize-ref-conflict",
        attempt: "inline",
      });
      log.warn("receive:ref-conflict", {
        conflictCount: finalize.statuses.filter((status) => !status.ok).length,
        stage: "finalize",
      });
      const response = new Response(
        asBodyInit(
          buildReceiveReportStatus({
            unpackOk: true,
            commands,
            statuses: finalize.statuses,
          })
        ),
        {
          status: 200,
          headers: {
            "Content-Type": "application/x-git-receive-pack-result",
            "Cache-Control": "no-cache",
            "X-Repo-Changed": "0",
            "X-Repo-Empty": "0",
          },
        }
      );
      logReceiveEnd(log, response.status, { reason: "finalize-ref-conflict", changed: false });
      return response;
    }

    if (finalize.status !== "committed") {
      await cleanupStagedPack({
        stagedUpload,
        log,
        reason: "unexpected-finalize-result",
        attempt: "inline",
      });
      log.error("receive:unexpected-finalize-result", { status: finalize.status });
      const response = new Response("Unexpected receive finalization result.\n", { status: 500 });
      logReceiveEnd(log, response.status, { reason: "unexpected-finalize-result" });
      return response;
    }

    if (finalize.shouldQueueCompaction) {
      log.info("receive:compaction-requested", { repoId });
    }

    const response = new Response(
      asBodyInit(
        buildReceiveReportStatus({
          unpackOk: true,
          commands,
          statuses: finalize.statuses,
        })
      ),
      {
        status: 200,
        headers: {
          "Content-Type": "application/x-git-receive-pack-result",
          "Cache-Control": "no-cache",
          "X-Repo-Changed": finalize.changed ? "1" : "0",
          "X-Repo-Empty": finalize.empty ? "1" : "0",
        },
      }
    );
    logReceiveEnd(log, response.status, {
      changed: finalize.changed,
      empty: finalize.empty,
      packKey: stagedPack?.packKey,
      packBytes: stagedPack?.packBytes,
    });
    return response;
  } catch (error) {
    countReceiveSubrequest(cacheCtx, log, "do:abort-receive");
    const aborted = isReceiveAbort(request, error);
    await cleanupFailedReceive({
      ctx,
      stub,
      leaseToken: begin.lease.token,
      stagedUpload,
      log,
      reason: aborted ? "receive-aborted" : "receive-error",
    });
    if (aborted) {
      log.info("receive:aborted", { error: String(error) });
      logReceiveEnd(log, 499, { reason: "client-aborted" });
      return clientAbortedResponse();
    }

    log.error("receive:error", { error: String(error) });

    const message = String(error);
    const lower = message.toLowerCase();
    const status =
      lower.includes("unsupported pack version") || lower.includes("pack header")
        ? 415
        : lower.includes("malformed") ||
            lower.includes("missing") ||
            lower.includes("ended before") ||
            lower.includes("could not be resolved") ||
            lower.includes("delta")
          ? 400
          : 500;

    const response = new Response(`${message}\n`, {
      status,
      headers: { "Content-Type": "text/plain; charset=utf-8" },
    });
    logReceiveEnd(log, response.status, { reason: "error" });
    return response;
  }
}
