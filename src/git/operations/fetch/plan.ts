import type { CacheContext } from "@/cache/index.ts";
import type { Logger } from "@/common/logger.ts";
import type { OrderedPackSnapshot, OrderedPackSnapshotEntry, UploadPackPlan } from "./types.ts";

import { createLogger } from "@/common/index.ts";
import { getOidHexAt, loadActivePackCatalog, loadIdxView } from "@/git/object-store/index.ts";
import { findCommonHaves } from "../closure.ts";
import { computeNeededFast } from "./neededFast.ts";

type SnapshotLoadResult =
  | {
      type: "Ready";
      snapshot: OrderedPackSnapshot;
    }
  | {
      type: "RepositoryNotReady";
      reason: "no-active-packs" | "snapshot-missing-idx";
    };

async function loadOrderedPackSnapshot(
  env: Env,
  repoId: string,
  cacheCtx: CacheContext | undefined,
  log: Logger
): Promise<SnapshotLoadResult> {
  const rows = await loadActivePackCatalog(env, repoId, cacheCtx);
  if (rows.length === 0) {
    return {
      type: "RepositoryNotReady",
      reason: "no-active-packs",
    };
  }

  let idxMemoHits = 0;
  let idxLoads = 0;
  let indexedObjects = 0;
  const packs: OrderedPackSnapshotEntry[] = [];

  for (const row of rows) {
    const memoHit = cacheCtx?.memo?.idxViews?.has(row.packKey) === true;
    if (memoHit) idxMemoHits++;

    const idx = await loadIdxView(env, row.packKey, cacheCtx, row.packBytes);
    idxLoads++;
    if (!idx) {
      log.warn("stream:plan:snapshot-missing-idx", { packKey: row.packKey });
      return {
        type: "RepositoryNotReady",
        reason: "snapshot-missing-idx",
      };
    }

    indexedObjects += idx.count;
    packs.push({
      packKey: row.packKey,
      packBytes: row.packBytes,
      idx,
    });
  }

  log.info("stream:plan:snapshot", {
    packs: packs.length,
    idxLoads,
    idxMemoHits,
    idxMemoMisses: idxLoads - idxMemoHits,
    indexedObjects,
  });

  return {
    type: "Ready",
    snapshot: { packs },
  };
}

function buildInitialCloneNeeded(snapshot: OrderedPackSnapshot): string[] {
  const needed = new Set<string>();

  for (const pack of snapshot.packs) {
    for (let index = 0; index < pack.idx.count; index++) {
      needed.add(getOidHexAt(pack.idx, index));
    }
  }

  return Array.from(needed);
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
