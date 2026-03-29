import type { ResolvedAssemblerPlan } from "./types.ts";

import { createLogger } from "@/common/index.ts";
import { streamPackFromR2, streamPackFromMultiplePacks } from "@/git/pack/assemblerStream.ts";
import { getPackCandidates } from "../packDiscovery.ts";

export async function resolvePackStream(
  env: Env,
  plan: ResolvedAssemblerPlan,
  options?: {
    limiter?: { run<T>(label: string, fn: () => Promise<T>): Promise<T> };
    countSubrequest?: (n?: number) => void;
    onProgress?: (msg: string) => void;
    signal?: AbortSignal;
  }
): Promise<ReadableStream<Uint8Array> | undefined> {
  const log = createLogger(env.LOG_LEVEL, { service: "ResolvePackStream" });
  let packStream: ReadableStream<Uint8Array> | undefined;

  switch (plan.type) {
    case "InitCloneUnion":
    case "IncrementalMulti":
      packStream = await streamPackFromMultiplePacks(env, plan.packKeys, plan.needed, options);
      break;

    case "IncrementalSingle":
      packStream = await streamPackFromR2(env, plan.packKey, plan.needed, options);

      if (!packStream && plan.cacheCtx) {
        const packKeys = await getPackCandidates(env, plan.repoId, plan.cacheCtx);

        if (packKeys.length >= 2) {
          log.debug("pack-stream:single-fallback-to-multi", { packs: packKeys.length });
          packStream = await streamPackFromMultiplePacks(env, packKeys, plan.needed, options);
        }
      }
      break;
  }

  return packStream;
}
