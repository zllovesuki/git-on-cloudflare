import type { CacheContext } from "@/cache/index.ts";
import type { SnapshotLoadResult } from "@/git/pack/snapshot.ts";
import type { OrderedPackSnapshot, ServeUploadPackPlan, UploadPackPlan } from "./types.ts";

import { createLogger } from "@/common/index.ts";
import { buildInitialCloneNeeded, loadOrderedPackSnapshot } from "@/git/pack/snapshot.ts";
import { findCommonHaves } from "../closure.ts";
import { computeNeededFast } from "./neededFast.ts";

export async function loadUploadPackSnapshot(
  env: Env,
  repoId: string,
  cacheCtx?: CacheContext
): Promise<SnapshotLoadResult> {
  // Snapshot readiness stays outside the streaming response so callers can
  // still convert "not ready" into an HTTP retry signal before headers commit.
  const log = createLogger(env.LOG_LEVEL, { service: "StreamPlan", repoId });
  const snapshotLoad = await loadOrderedPackSnapshot(env, repoId, cacheCtx, log);
  if (snapshotLoad.type === "RepositoryNotReady") {
    log.warn("stream:plan:repository-not-ready", { reason: snapshotLoad.reason });
  }
  return snapshotLoad;
}

export async function buildServeUploadPackPlan(
  env: Env,
  repoId: string,
  snapshot: OrderedPackSnapshot,
  wants: string[],
  haves: string[],
  signal?: AbortSignal,
  cacheCtx?: CacheContext,
  onProgress?: (message: string) => void
): Promise<ServeUploadPackPlan> {
  const log = createLogger(env.LOG_LEVEL, { service: "StreamPlan", repoId });

  if (haves.length === 0) {
    onProgress?.("Selecting objects to send...\n");
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

  const neededOids = await computeNeededFast(env, repoId, wants, haves, cacheCtx, onProgress);
  const closureTimedOut = cacheCtx?.memo?.flags?.has("closure-timeout") === true;
  if (closureTimedOut) {
    log.warn("stream:plan:closure-timeout", { needed: neededOids.length });
  }

  log.info("stream:plan:serve", {
    packs: snapshot.packs.length,
    needed: neededOids.length,
    ackOids: 0,
    closureTimedOut,
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

export async function planUploadPack(
  env: Env,
  repoId: string,
  wants: string[],
  haves: string[],
  done: boolean,
  signal?: AbortSignal,
  cacheCtx?: CacheContext
): Promise<UploadPackPlan> {
  const snapshotLoad = await loadUploadPackSnapshot(env, repoId, cacheCtx);
  if (snapshotLoad.type === "RepositoryNotReady") {
    return { type: "RepositoryNotReady" };
  }

  const servePlan = await buildServeUploadPackPlan(
    env,
    repoId,
    snapshotLoad.snapshot,
    wants,
    haves,
    signal,
    cacheCtx
  );

  if (done || haves.length === 0) {
    return servePlan;
  }

  const ackOids = await findCommonHaves(env, repoId, haves, cacheCtx);
  return {
    ...servePlan,
    ackOids,
  };
}
