import type { OrderedPackSnapshot } from "@/git/operations/fetch/types.ts";

import { createLogger } from "@/common/index.ts";
import {
  buildSelection,
  buildOutputOrder,
  canPassthroughSinglePack,
  computeHeaderLengths,
} from "./rewrite/plan.ts";
import {
  ensurePackReadState,
  type RewriteFailure,
  type RewriteFailureRecorder,
  type RewriteOptions,
} from "./rewrite/shared.ts";
import { createPassthroughStream, createRewriteStream } from "./rewrite/stream.ts";

export type PackRewriteResult =
  | { status: "ok"; stream: ReadableStream<Uint8Array> }
  | { status: "failed"; failure: RewriteFailure };

export async function rewritePackResult(
  env: Env,
  snapshot: OrderedPackSnapshot,
  neededOids: string[],
  options?: RewriteOptions
): Promise<PackRewriteResult> {
  const log = createLogger(env.LOG_LEVEL, { service: "PackRewrite" });
  const startedAt = Date.now();
  const warnedFlags = new Set<string>();
  const failure: RewriteFailureRecorder = options?.failure || {};
  const rewriteOptions: RewriteOptions = { ...options, failure };

  function failed(reason: string, retryable: boolean, details?: Record<string, unknown>) {
    return {
      status: "failed" as const,
      failure: failure.value || { reason, retryable, details },
    };
  }

  if (rewriteOptions.signal?.aborted) {
    return failed("aborted", true);
  }
  if (!rewriteOptions.limiter) {
    throw new Error("rewrite: limiter required");
  }
  if (!rewriteOptions.countSubrequest) {
    throw new Error("rewrite: countSubrequest required");
  }

  const selection = await buildSelection(
    env,
    snapshot,
    neededOids,
    log,
    warnedFlags,
    rewriteOptions
  );
  if (!selection) {
    return failed("selection-failed", true, { needed: neededOids.length });
  }

  const { table, readerStates } = selection;

  if (canPassthroughSinglePack(snapshot, table)) {
    const readState = await ensurePackReadState(
      env,
      snapshot.packs[0]!,
      0,
      readerStates,
      log,
      warnedFlags,
      rewriteOptions
    );

    log.info("rewrite:passthrough", {
      packKey: snapshot.packs[0]?.packKey,
      objects: table.count,
    });

    return {
      status: "ok",
      stream: createPassthroughStream({
        env,
        snapshotPack: snapshot.packs[0]!,
        readState,
        log,
        warnedFlags,
        options: rewriteOptions,
        onComplete: () => {
          log.info("rewrite:stream-complete", {
            passthrough: true,
            wholePackLoads: countWholePackLoads(readerStates),
            timeMs: Date.now() - startedAt,
          });
        },
      }),
    };
  }

  if (!buildOutputOrder(table, log)) {
    return failed("topology-incomplete", false, { selected: table.count });
  }
  if (!computeHeaderLengths(table, log)) {
    return failed("header-lengths-did-not-converge", false, { selected: table.count });
  }

  return {
    status: "ok",
    stream: createRewriteStream(table, snapshot, readerStates, log, rewriteOptions, () => {
      log.info("rewrite:stream-complete", {
        passthrough: false,
        wholePackLoads: countWholePackLoads(readerStates),
        timeMs: Date.now() - startedAt,
      });
    }),
  };
}

export async function rewritePack(
  env: Env,
  snapshot: OrderedPackSnapshot,
  neededOids: string[],
  options?: RewriteOptions
): Promise<ReadableStream<Uint8Array> | undefined> {
  const result = await rewritePackResult(env, snapshot, neededOids, options);
  return result.status === "ok" ? result.stream : undefined;
}

function countWholePackLoads(readerStates: Map<number, { wholePack?: Uint8Array }>): number {
  let count = 0;
  for (const state of readerStates.values()) {
    if (state.wholePack) count++;
  }
  return count;
}
