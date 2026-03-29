import type { CacheContext } from "@/cache/index.ts";

import { pktLine } from "@/git/core/index.ts";
import { createLogger } from "@/common/index.ts";
import { getPackCandidates } from "../packDiscovery.ts";
import { getLimiter, countSubrequest } from "../limits.ts";
import { parseFetchArgs } from "../args.ts";
import { findCommonHaves } from "../closure.ts";
import { buildAckOnlyResponse } from "../fetch/protocol.ts";
import { repositoryNotReadyResponse } from "../fetch/responses.ts";
import { planUploadPack } from "../fetch/plan.ts";
import { resolvePackStream } from "../fetch/execute.ts";
import {
  SidebandProgressMux,
  emitProgress,
  emitFatal,
  pipePackWithSideband,
} from "../fetch/sideband.ts";

export { computeNeededFast } from "../fetch/neededFast.ts";
export * from "../fetch/types.ts";

export async function handleFetchV2Streaming(
  env: Env,
  repoId: string,
  body: Uint8Array,
  signal?: AbortSignal,
  cacheCtx?: CacheContext
): Promise<Response> {
  const { wants, haves, done } = parseFetchArgs(body);
  const log = createLogger(env.LOG_LEVEL, { service: "StreamFetchV2", repoId });

  if (signal?.aborted) {
    return new Response("client aborted\n", { status: 499 });
  }

  if (wants.length === 0) {
    return buildAckOnlyResponse([]);
  }

  if (!done) {
    let ackOids: string[] = [];
    if (haves.length > 0) {
      ackOids = await findCommonHaves(env, repoId, haves, cacheCtx);
      log.debug("stream:fetch:negotiation", { haves: haves.length, acks: ackOids.length });
    }
    return buildAckOnlyResponse(ackOids);
  }

  const packKeys = await getPackCandidates(env, repoId, cacheCtx);

  if (packKeys.length === 0) {
    log.warn("stream:fetch:repository-not-ready");
    return repositoryNotReadyResponse();
  }

  log.info("stream:fetch:immediate-stream", { wants: wants.length, haves: haves.length });

  const responseStream = new ReadableStream<Uint8Array>({
    async start(controller) {
      const streamLog = createLogger(env.LOG_LEVEL, { service: "StreamFetchV2", repoId });
      try {
        controller.enqueue(pktLine("packfile\n"));
        emitProgress(controller, "remote: Preparing pack...\n");

        const planStart = Date.now();
        const plan = await planUploadPack(env, repoId, wants, haves, done, signal, cacheCtx);

        if (!plan) {
          emitFatal(controller, "Unable to create fetch plan");
          controller.close();
          return;
        }

        if (plan.type === "RepositoryNotReady") {
          emitFatal(controller, "Repository not ready - objects are being packed");
          controller.close();
          return;
        }

        const planTime = Date.now() - planStart;
        streamLog.info("stream:fetch:plan-complete", { type: plan.type, timeMs: planTime });

        const progressMux = new SidebandProgressMux();
        const limiter = plan.cacheCtx ? getLimiter(plan.cacheCtx) : undefined;

        const packStream = await resolvePackStream(env, plan, {
          signal: plan.signal,
          limiter,
          countSubrequest: (n?: number) => countSubrequest(plan.cacheCtx, n),
          onProgress: (msg) => progressMux.push(msg),
        });

        if (!packStream) {
          emitFatal(controller, "Unable to assemble pack");
          controller.close();
          return;
        }

        await pipePackWithSideband(packStream, controller, {
          signal: plan.signal,
          progressMux,
          log: streamLog,
        });

        controller.close();
      } catch (error) {
        streamLog.error("stream:response:error", { error: String(error) });
        try {
          emitFatal(controller, String(error));
        } catch {}
        controller.error(error);
      }
    },
  });

  return new Response(responseStream, {
    status: 200,
    headers: {
      "Content-Type": "application/x-git-upload-pack-result",
      "Cache-Control": "no-cache",
    },
  });
}
