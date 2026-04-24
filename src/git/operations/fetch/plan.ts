import type { CacheContext } from "@/cache/index.ts";
import type { Logger } from "@/common/logger.ts";
import type { SnapshotLoadResult } from "@/git/pack/snapshot.ts";
import type { OrderedPackSnapshot, ServeUploadPackPlan, UploadPackPlan } from "./types.ts";
import type { PackRefSnapshotEntry, PackRefSnapshotLoadResult } from "@/git/pack/refIndex.ts";

import { createLogger } from "@/common/index.ts";
import { buildInitialCloneNeeded, loadOrderedPackSnapshot } from "@/git/pack/snapshot.ts";
import { getDoIdFromPath } from "@/keys.ts";
import { findCommonHaves } from "../closure.ts";
import { computeNeededFromPackRefs } from "./refClosure.ts";
import { loadPackRefView } from "@/git/pack/refIndex.ts";

export class FetchPlanRetryError extends Error {
  readonly reason: "missing-ref-index" | "closure-budget-exceeded";
  readonly retryAfterSeconds: number;

  constructor(reason: "missing-ref-index" | "closure-budget-exceeded") {
    super(reason);
    this.name = "FetchPlanRetryError";
    this.reason = reason;
    this.retryAfterSeconds = 10;
  }
}

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

function schedulePackRefBackfill(args: {
  env: Env;
  repoId: string;
  packKey: string;
  cacheCtx?: CacheContext;
  log: Logger;
  reason: string;
}): void {
  const doId = getDoIdFromPath(args.packKey);
  if (!doId) {
    args.log.warn("stream:fetch:ref-index-backfill-skipped", {
      packKey: args.packKey,
      reason: "missing-do-id",
    });
    return;
  }

  const send = args.env.REPO_MAINT_QUEUE.send({
    kind: "pack-ref-backfill",
    doId,
    repoId: args.repoId,
    packKey: args.packKey,
  })
    .then(() => {
      args.log.info("stream:fetch:ref-index-backfill-queued", {
        packKey: args.packKey,
        reason: args.reason,
      });
    })
    .catch((error) => {
      args.log.warn("stream:fetch:ref-index-backfill-enqueue-failed", {
        packKey: args.packKey,
        reason: args.reason,
        error: String(error),
      });
    });

  if (args.cacheCtx) {
    args.cacheCtx.ctx.waitUntil(send);
  } else {
    send.catch(() => {});
  }
}

export async function loadPackRefSnapshot(
  env: Env,
  repoId: string,
  snapshot: OrderedPackSnapshot,
  cacheCtx?: CacheContext
): Promise<PackRefSnapshotLoadResult> {
  const log = createLogger(env.LOG_LEVEL, { service: "StreamPlan", repoId });
  const packs: PackRefSnapshotEntry[] = [];
  const missing: Array<{
    packKey: string;
    packBytes: number;
    reason: "missing" | "corrupt" | "stale";
    detail?: string;
  }> = [];

  for (const pack of snapshot.packs) {
    const load = await loadPackRefView(env, pack.packKey, pack.idx, cacheCtx);
    if (load.type === "Ready") {
      packs.push({
        packKey: pack.packKey,
        packBytes: pack.packBytes,
        idx: pack.idx,
        refs: load.view,
      });
      continue;
    }

    const reason = load.type === "Missing" ? "missing" : load.kind;
    const detail = load.type === "Invalid" ? load.reason : undefined;
    missing.push({
      packKey: pack.packKey,
      packBytes: pack.packBytes,
      reason,
      detail,
    });
    log.warn("stream:fetch:ref-index-missing", {
      packKey: pack.packKey,
      reason,
      detail,
    });
    schedulePackRefBackfill({
      env,
      repoId,
      packKey: pack.packKey,
      cacheCtx,
      log,
      reason,
    });
  }

  log.info("stream:plan:ref-snapshot", {
    packs: snapshot.packs.length,
    loaded: packs.length,
    missing: missing.length,
  });

  if (missing.length > 0) {
    return { type: "Missing", packs: missing };
  }

  return { type: "Ready", packs };
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

  const refSnapshot = await loadPackRefSnapshot(env, repoId, snapshot, cacheCtx);
  if (refSnapshot.type === "Missing") {
    throw new FetchPlanRetryError("missing-ref-index");
  }

  const closure = await computeNeededFromPackRefs({
    logLevel: env.LOG_LEVEL,
    repoId,
    packs: refSnapshot.packs,
    wants,
    haves,
    onProgress,
  });
  if (closure.type === "BudgetExceeded") {
    log.warn("stream:plan:closure-budget-exceeded", {
      reason: closure.reason,
      needed: closure.neededOids.length,
      seen: closure.stats.seen,
      queued: closure.stats.queued,
      missing: closure.stats.missing,
      edgeVisits: closure.stats.edgeVisits,
      duplicateQueueSkips: closure.stats.duplicateQueueSkips,
    });
    throw new FetchPlanRetryError("closure-budget-exceeded");
  }
  const neededOids = closure.neededOids;

  log.info("stream:plan:serve", {
    packs: snapshot.packs.length,
    needed: neededOids.length,
    ackOids: 0,
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

  if (!done) {
    const ackOids = haves.length > 0 ? await findCommonHaves(env, repoId, haves, cacheCtx) : [];
    return {
      type: "Serve",
      repoId,
      snapshot: snapshotLoad.snapshot,
      neededOids: [],
      ackOids,
      signal,
      cacheCtx,
    };
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

  return servePlan;
}
