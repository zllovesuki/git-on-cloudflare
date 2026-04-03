import type { OrderedPackSnapshot } from "@/git/operations/fetch/types.ts";

import { createLogger } from "@/common/index.ts";
import {
  buildSelection,
  buildOutputOrder,
  canPassthroughSinglePack,
  computeHeaderLengths,
} from "./rewrite/plan.ts";
import { ensurePackReadState, type RewriteOptions } from "./rewrite/shared.ts";
import { createPassthroughStream, createRewriteStream } from "./rewrite/stream.ts";

export async function rewritePack(
  env: Env,
  snapshot: OrderedPackSnapshot,
  neededOids: string[],
  options?: RewriteOptions
): Promise<ReadableStream<Uint8Array> | undefined> {
  const log = createLogger(env.LOG_LEVEL, { service: "PackRewrite" });
  const startedAt = Date.now();
  const warnedFlags = new Set<string>();

  if (options?.signal?.aborted) return undefined;
  if (!options?.limiter) {
    throw new Error("rewrite: limiter required");
  }
  if (!options?.countSubrequest) {
    throw new Error("rewrite: countSubrequest required");
  }

  const selection = await buildSelection(env, snapshot, neededOids, log, warnedFlags, options);
  if (!selection) return undefined;

  const { table, readerStates } = selection;

  if (canPassthroughSinglePack(snapshot, table)) {
    const readState = await ensurePackReadState(
      env,
      snapshot.packs[0]!,
      0,
      readerStates,
      log,
      warnedFlags,
      options
    );

    log.info("rewrite:passthrough", {
      packKey: snapshot.packs[0]?.packKey,
      objects: table.count,
    });

    return createPassthroughStream({
      env,
      snapshotPack: snapshot.packs[0]!,
      readState,
      log,
      warnedFlags,
      options,
      onComplete: () => {
        log.info("rewrite:stream-complete", {
          passthrough: true,
          wholePackLoads: countWholePackLoads(readerStates),
          timeMs: Date.now() - startedAt,
        });
      },
    });
  }

  if (!buildOutputOrder(table, log)) return undefined;
  if (!computeHeaderLengths(table, log)) return undefined;

  return createRewriteStream(table, snapshot, readerStates, log, options, () => {
    log.info("rewrite:stream-complete", {
      passthrough: false,
      wholePackLoads: countWholePackLoads(readerStates),
      timeMs: Date.now() - startedAt,
    });
  });
}

function countWholePackLoads(readerStates: Map<number, { wholePack?: Uint8Array }>): number {
  let count = 0;
  for (const state of readerStates.values()) {
    if (state.wholePack) count++;
  }
  return count;
}
