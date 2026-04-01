import type { CacheContext } from "@/cache/index.ts";
import type { Logger } from "@/common/logger.ts";
import type { RepoDurableObject } from "@/do/index.ts";
import type { PackCatalogRow } from "@/do/repo/db/schema.ts";
import type { ReceiveStatus } from "@/git/operations/validation.ts";

import { clientAbortedResponse, createLogger, getRepoStub } from "@/common/index.ts";
import {
  MAX_SIMULTANEOUS_CONNECTIONS,
  SubrequestLimiter,
  countSubrequest,
} from "@/git/operations/limits.ts";
import { isValidRefName, validateReceiveCommands } from "@/git/operations/validation.ts";
import { logOnce } from "@/git/object-store/support.ts";
import { executeReceivePipeline, ReceivePipelineHttpError } from "./pipeline.ts";
import { readPktSectionStream } from "./pktSectionStream.ts";
import {
  parseReceiveRequest,
  type ParsedReceiveRequest,
  type ReceiveCommandList,
  type ReceiveNegotiatedCapabilities,
} from "./request.ts";
import {
  buildReceiveResultResponse,
  ReceiveSidebandWriter,
  type ReceiveResponseMode,
} from "./response.ts";
import {
  buildReceiveReportStatus,
  buildReceiveUnpackFailureReport,
  isReceiveAbort,
  throwIfReceiveAborted,
} from "./support.ts";

const RECEIVE_SUBREQUEST_BUDGET = 5_000;

type RepoStub = DurableObjectStub<RepoDurableObject>;
type RepoStateChangeHandler = (change: {
  changed: boolean;
  empty: boolean;
}) => Promise<void> | void;

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

function selectReceiveResponseMode(
  capabilities: ReceiveNegotiatedCapabilities
): ReceiveResponseMode {
  return capabilities.sideBand64k ? "side-band-64k" : "plain";
}

function scheduleRepoStateChange(
  ctx: ExecutionContext,
  onRepoStateChanged: RepoStateChangeHandler | undefined,
  change: {
    changed: boolean;
    empty: boolean;
  }
): void {
  if (!onRepoStateChanged || !change.changed) return;
  ctx.waitUntil(Promise.resolve().then(() => onRepoStateChanged(change)));
}

function buildInvalidRefResponse(args: {
  mode: ReceiveResponseMode;
  commands: ReceiveCommandList;
}): Response {
  return buildReceiveResultResponse({
    mode: args.mode,
    reportStatusBody: buildReceiveUnpackFailureReport(args.commands, "invalid-ref", "invalid"),
    changed: false,
    empty: false,
  });
}

function buildPreflightConflictResponse(args: {
  mode: ReceiveResponseMode;
  commands: ReceiveCommandList;
  statuses: ReceiveStatus[];
}): Response {
  return buildReceiveResultResponse({
    mode: args.mode,
    reportStatusBody: buildReceiveReportStatus({
      unpackOk: true,
      commands: args.commands,
      statuses: args.statuses,
    }),
    changed: false,
    empty: false,
  });
}

function getErrorStatus(error: unknown): number {
  if (error instanceof ReceivePipelineHttpError) {
    return error.status;
  }

  const message = String(error);
  const lower = message.toLowerCase();
  if (lower.includes("unsupported pack version") || lower.includes("pack header")) {
    return 415;
  }
  if (
    lower.includes("malformed") ||
    lower.includes("missing") ||
    lower.includes("ended before") ||
    lower.includes("could not be resolved") ||
    lower.includes("delta")
  ) {
    return 400;
  }
  return 500;
}

function createSidebandReceiveResponse(args: {
  env: Env;
  repoId: string;
  request: Request;
  ctx: ExecutionContext;
  stub: RepoStub;
  log: Logger;
  cacheCtx: CacheContext;
  limiter: SubrequestLimiter;
  leaseToken: string;
  activeCatalog: PackCatalogRow[];
  commands: ParsedReceiveRequest["commands"];
  capabilities: ReceiveNegotiatedCapabilities;
  packStream: ReadableStream<Uint8Array>;
  bytesConsumed: number;
  onRepoStateChanged?: RepoStateChangeHandler | undefined;
}): Response {
  const responseStream = new ReadableStream<Uint8Array>({
    async start(controller) {
      const writer = new ReceiveSidebandWriter(controller);
      const onProgress = args.capabilities.quiet
        ? undefined
        : (message: string) => writer.progress(message);
      let closed = false;
      const close = () => {
        if (closed) return;
        closed = true;
        controller.close();
      };

      try {
        const result = await executeReceivePipeline({
          env: args.env,
          repoId: args.repoId,
          request: args.request,
          ctx: args.ctx,
          packStream: args.packStream,
          bytesConsumed: args.bytesConsumed,
          stub: args.stub,
          leaseToken: args.leaseToken,
          activeCatalog: args.activeCatalog,
          commands: args.commands,
          log: args.log,
          cacheCtx: args.cacheCtx,
          limiter: args.limiter,
          countSubrequest: (op, n = 1) => countReceiveSubrequest(args.cacheCtx, args.log, op, n),
          onProgress,
        });

        scheduleRepoStateChange(args.ctx, args.onRepoStateChanged, {
          changed: result.changed,
          empty: result.empty,
        });
        writer.reportStatus(result.reportStatusBody);
        logReceiveEnd(args.log, 200, {
          changed: result.changed,
          empty: result.empty,
          packKey: result.packKey,
          packBytes: result.packBytes,
        });
      } catch (error) {
        if (isReceiveAbort(args.request, error)) {
          logReceiveEnd(args.log, 499, { reason: "client-aborted" });
          close();
          return;
        }

        args.log.error("receive:error", { error: String(error) });
        writer.reportStatus(
          buildReceiveUnpackFailureReport(
            args.commands,
            error instanceof ReceivePipelineHttpError ? error.message : String(error)
          )
        );
        logReceiveEnd(args.log, 200, { reason: "sideband-unpack-error" });
      } finally {
        close();
      }
    },
  });

  return new Response(responseStream, {
    status: 200,
    headers: {
      "Content-Type": "application/x-git-receive-pack-result",
      "Cache-Control": "no-cache",
    },
  });
}

export async function handleStreamingReceivePackPOST(
  env: Env,
  repoId: string,
  request: Request,
  ctx: ExecutionContext,
  options?: {
    onRepoStateChanged?: RepoStateChangeHandler | undefined;
  }
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
    await stub.abortReceive(begin.lease.token).catch(() => {});
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

  let pipelineStarted = false;
  try {
    const { lines, bytesConsumed, packStream } = await readPktSectionStream(request.body);
    throwIfReceiveAborted(request, log, "read-command-section");

    const parsedRequest = parseReceiveRequest(lines);
    const responseMode = selectReceiveResponseMode(parsedRequest.capabilities);

    const invalidCommand = parsedRequest.commands.find((command) => !isValidRefName(command.ref));
    if (invalidCommand) {
      countReceiveSubrequest(cacheCtx, log, "do:abort-receive");
      await stub.abortReceive(begin.lease.token).catch(() => {});
      log.warn("receive:invalid-ref", { ref: invalidCommand.ref });
      const response = buildInvalidRefResponse({
        mode: responseMode,
        commands: parsedRequest.commands,
      });
      logReceiveEnd(log, response.status, { reason: "invalid-ref", changed: false, empty: false });
      return response;
    }

    const preflightStatuses = validateReceiveCommands(begin.refs, parsedRequest.commands);
    if (!preflightStatuses.every((status) => status.ok)) {
      countReceiveSubrequest(cacheCtx, log, "do:abort-receive");
      await stub.abortReceive(begin.lease.token).catch(() => {});
      log.warn("receive:ref-conflict", {
        conflictCount: preflightStatuses.filter((status) => !status.ok).length,
        stage: "preflight",
      });
      const response = buildPreflightConflictResponse({
        mode: responseMode,
        commands: parsedRequest.commands,
        statuses: preflightStatuses,
      });
      logReceiveEnd(log, response.status, { reason: "preflight-ref-conflict", changed: false });
      return response;
    }

    if (responseMode === "side-band-64k") {
      return createSidebandReceiveResponse({
        env,
        repoId,
        request,
        ctx,
        stub,
        log,
        cacheCtx,
        limiter,
        leaseToken: begin.lease.token,
        activeCatalog: begin.activeCatalog,
        commands: parsedRequest.commands,
        capabilities: parsedRequest.capabilities,
        packStream,
        bytesConsumed,
        onRepoStateChanged: options?.onRepoStateChanged,
      });
    }

    pipelineStarted = true;
    const result = await executeReceivePipeline({
      env,
      repoId,
      request,
      ctx,
      packStream,
      bytesConsumed,
      stub,
      leaseToken: begin.lease.token,
      activeCatalog: begin.activeCatalog,
      commands: parsedRequest.commands,
      log,
      cacheCtx,
      limiter,
      countSubrequest: (op, n = 1) => countReceiveSubrequest(cacheCtx, log, op, n),
    });

    scheduleRepoStateChange(ctx, options?.onRepoStateChanged, {
      changed: result.changed,
      empty: result.empty,
    });

    const response = buildReceiveResultResponse({
      mode: "plain",
      reportStatusBody: result.reportStatusBody,
      changed: result.changed,
      empty: result.empty,
    });
    logReceiveEnd(log, response.status, {
      changed: result.changed,
      empty: result.empty,
      packKey: result.packKey,
      packBytes: result.packBytes,
    });
    return response;
  } catch (error) {
    if (!pipelineStarted) {
      countReceiveSubrequest(cacheCtx, log, "do:abort-receive");
      await stub.abortReceive(begin.lease.token).catch(() => {});
    }

    if (isReceiveAbort(request, error)) {
      log.info("receive:aborted", { error: String(error) });
      logReceiveEnd(log, 499, { reason: "client-aborted" });
      return clientAbortedResponse();
    }

    log.error("receive:error", { error: String(error) });

    if (error instanceof ReceivePipelineHttpError) {
      const response = new Response(`${error.message}\n`, {
        status: error.status,
        headers: { "Content-Type": "text/plain; charset=utf-8" },
      });
      logReceiveEnd(log, response.status, { reason: error.reason });
      return response;
    }

    const response = new Response(`${String(error)}\n`, {
      status: getErrorStatus(error),
      headers: { "Content-Type": "text/plain; charset=utf-8" },
    });
    logReceiveEnd(log, response.status, { reason: "error" });
    return response;
  }
}
