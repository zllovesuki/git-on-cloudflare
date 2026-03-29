import type { CacheContext } from "@/cache/index.ts";
import type { RepoStorageMode } from "@/do/repo/repoState.ts";
import type { PackCatalogRow } from "./types.ts";

import { createLogger, getRepoStub } from "@/common/index.ts";
import { countSubrequest, getLimiter } from "@/git/operations/limits.ts";
import { ensureMemo, logOnce } from "./support.ts";

export async function loadActivePackCatalog(
  env: Env,
  repoId: string,
  cacheCtx?: CacheContext
): Promise<PackCatalogRow[]> {
  ensureMemo(cacheCtx, repoId);
  const log = createLogger(env.LOG_LEVEL, { service: "PackedObjectStore", repoId });

  if (cacheCtx?.memo?.packCatalog) {
    logOnce(cacheCtx, "packed-catalog-logged", () => {
      log.debug("pack-catalog:loaded", {
        source: "memo",
        packs: cacheCtx.memo?.packCatalog?.length,
      });
    });
    return cacheCtx.memo.packCatalog;
  }
  if (cacheCtx?.memo?.packCatalogPromise) return await cacheCtx.memo.packCatalogPromise;

  const stub = getRepoStub(env, repoId);
  const limiter = getLimiter(cacheCtx);
  const inflight = limiter.run("do:get-active-pack-catalog", async () => {
    if (!countSubrequest(cacheCtx)) {
      logOnce(cacheCtx, "packed-catalog-soft-budget-warned", () => {
        log.warn("soft-budget-exhausted", {
          op: "do:get-active-pack-catalog",
        });
      });
    }
    return await stub.getActivePackCatalog();
  });
  if (cacheCtx?.memo) cacheCtx.memo.packCatalogPromise = inflight;
  try {
    const rows = await inflight;
    if (cacheCtx?.memo) cacheCtx.memo.packCatalog = rows;
    logOnce(cacheCtx, "packed-catalog-logged", () => {
      log.debug("pack-catalog:loaded", { source: "do", packs: rows.length });
    });
    return rows;
  } catch (error) {
    log.warn("pack-catalog:load-error", { error: String(error) });
    throw error;
  } finally {
    if (cacheCtx?.memo) cacheCtx.memo.packCatalogPromise = undefined;
  }
}

export async function loadRepoStorageMode(
  env: Env,
  repoId: string,
  cacheCtx?: CacheContext
): Promise<RepoStorageMode> {
  ensureMemo(cacheCtx, repoId);
  const log = createLogger(env.LOG_LEVEL, { service: "PackedObjectStore", repoId });
  if (cacheCtx?.memo?.repoStorageMode) return cacheCtx.memo.repoStorageMode;
  if (cacheCtx?.memo?.repoStorageModePromise) return await cacheCtx.memo.repoStorageModePromise;

  const stub = getRepoStub(env, repoId);
  const limiter = getLimiter(cacheCtx);
  const inflight = limiter.run("do:get-repo-storage-mode", async () => {
    if (!countSubrequest(cacheCtx)) {
      logOnce(cacheCtx, "packed-storage-mode-soft-budget-warned", () => {
        log.warn("soft-budget-exhausted", {
          op: "do:get-repo-storage-mode",
        });
      });
    }
    return await stub.getRepoStorageMode();
  });
  if (cacheCtx?.memo) cacheCtx.memo.repoStorageModePromise = inflight;
  try {
    const mode = await inflight;
    if (cacheCtx?.memo) cacheCtx.memo.repoStorageMode = mode;
    return mode;
  } finally {
    if (cacheCtx?.memo) cacheCtx.memo.repoStorageModePromise = undefined;
  }
}
