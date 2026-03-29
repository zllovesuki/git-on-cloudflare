import type { CacheContext } from "@/cache/index.ts";
import type { AssemblerPlan } from "./types.ts";

import { createLogger } from "@/common/index.ts";
import { getPackCandidates } from "../packDiscovery.ts";
import { beginClosurePhase, endClosurePhase } from "../heavyMode.ts";
import {
  findCommonHaves,
  buildUnionNeededForKeys,
  countMissingRootTreesFromWants,
} from "../closure.ts";
import { computeNeededFast } from "./neededFast.ts";

export async function planUploadPack(
  env: Env,
  repoId: string,
  wants: string[],
  haves: string[],
  done: boolean,
  signal?: AbortSignal,
  cacheCtx?: CacheContext
): Promise<AssemblerPlan | null> {
  const log = createLogger(env.LOG_LEVEL, { service: "StreamPlan", repoId });
  const packKeys = await getPackCandidates(env, repoId, cacheCtx);

  if (haves.length === 0 && packKeys.length >= 2) {
    let keys = packKeys.slice(0);
    let unionNeeded = await buildUnionNeededForKeys(env, repoId, keys, cacheCtx, log);

    if (unionNeeded.length > 0) {
      try {
        const unionSet = new Set<string>(unionNeeded);
        const missingRoots = await countMissingRootTreesFromWants(
          env,
          repoId,
          wants,
          cacheCtx,
          unionSet
        );
        if (missingRoots > 0) {
          log.info("stream:plan:init-union:missing-roots", { missingRoots, keys: keys.length });
          keys = packKeys.slice(0);
          unionNeeded = await buildUnionNeededForKeys(env, repoId, keys, cacheCtx, log);
        }
      } catch {}
    }

    if (unionNeeded.length > 0) {
      log.info("stream:plan:init-union", { packs: keys.length, union: unionNeeded.length });
      return {
        type: "InitCloneUnion",
        repoId,
        packKeys: keys,
        needed: unionNeeded,
        wants,
        ackOids: [],
        signal,
        cacheCtx,
      };
    }
  }

  beginClosurePhase(cacheCtx, { loaderCap: 400 });
  const needed = await computeNeededFast(env, repoId, wants, haves, cacheCtx);
  endClosurePhase(cacheCtx);

  if (cacheCtx?.memo?.flags?.has("closure-timeout")) {
    log.warn("stream:plan:closure-timeout", { needed: needed.length });

    if (packKeys.length >= 2) {
      const keys = packKeys.slice(0);
      const unionNeeded = await buildUnionNeededForKeys(env, repoId, keys, cacheCtx, log);

      if (unionNeeded.length > 0) {
        const ackOids = done ? [] : await findCommonHaves(env, repoId, haves, cacheCtx);
        return {
          type: "IncrementalMulti",
          repoId,
          packKeys: keys,
          needed: unionNeeded,
          ackOids,
          signal,
          cacheCtx,
        };
      }
    }
    return null;
  }

  const ackOids = done ? [] : await findCommonHaves(env, repoId, haves, cacheCtx);

  if (packKeys.length === 1) {
    log.info("stream:plan:single-pack", {
      packKey: packKeys[0],
      needed: needed.length,
    });

    return {
      type: "IncrementalSingle",
      repoId,
      packKey: packKeys[0],
      needed,
      ackOids,
      signal,
      cacheCtx,
    };
  }

  if (packKeys.length >= 2) {
    log.info("stream:plan:multi-pack-available", {
      packs: packKeys.length,
      needed: needed.length,
    });

    return {
      type: "IncrementalMulti",
      repoId,
      packKeys,
      needed,
      ackOids,
      signal,
      cacheCtx,
    };
  }

  log.warn("stream:plan:no-packs-blocking", { needed: needed.length });
  return { type: "RepositoryNotReady" };
}
