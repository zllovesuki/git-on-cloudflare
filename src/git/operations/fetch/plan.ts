import type { CacheContext } from "@/cache/index.ts";
import type { UploadPackPlan } from "./types.ts";

import { createLogger } from "@/common/index.ts";
import { buildInitialCloneNeeded, loadOrderedPackSnapshot } from "@/git/pack/snapshot.ts";
import { findCommonHaves } from "../closure.ts";
import { computeNeededFast } from "./neededFast.ts";

export async function planUploadPack(
  env: Env,
  repoId: string,
  wants: string[],
  haves: string[],
  done: boolean,
  signal?: AbortSignal,
  cacheCtx?: CacheContext
): Promise<UploadPackPlan> {
  const log = createLogger(env.LOG_LEVEL, { service: "StreamPlan", repoId });
  const snapshotLoad = await loadOrderedPackSnapshot(env, repoId, cacheCtx, log);
  if (snapshotLoad.type === "RepositoryNotReady") {
    log.warn("stream:plan:repository-not-ready", { reason: snapshotLoad.reason });
    return { type: "RepositoryNotReady" };
  }
  const snapshot = snapshotLoad.snapshot;

  if (haves.length === 0) {
    const neededOids = buildInitialCloneNeeded(snapshot);
    log.info("stream:plan:init-clone", {
      packs: snapshot.packs.length,
      needed: neededOids.length,
    });
    return {
      type: "Serve",
      repoId,
      snapshot,
      neededOids,
      ackOids: [],
      signal,
      cacheCtx,
    };
  }

  const neededOids = await computeNeededFast(env, repoId, wants, haves, cacheCtx);
  const closureTimedOut = cacheCtx?.memo?.flags?.has("closure-timeout") === true;
  if (closureTimedOut) {
    log.warn("stream:plan:closure-timeout", { needed: neededOids.length });
  }

  const ackOids = done ? [] : await findCommonHaves(env, repoId, haves, cacheCtx);

  log.info("stream:plan:serve", {
    packs: snapshot.packs.length,
    needed: neededOids.length,
    ackOids: ackOids.length,
    closureTimedOut,
  });

  return {
    type: "Serve",
    repoId,
    snapshot,
    neededOids,
    ackOids,
    signal,
    cacheCtx,
  };
}
