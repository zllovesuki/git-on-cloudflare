import type { CacheContext } from "@/cache/index.ts";

import { createLogger } from "@/common/index.ts";
import { loadActivePackCatalog } from "@/git/object-store/index.ts";

/**
 * Shared helper to discover candidate pack keys for a repository.
 *
 * The active pack catalog is now the only correctness source for fetch
 * planning. Legacy pack mirrors remain useful for rollback and debugging, but
 * they no longer participate in candidate discovery.
 */
export async function getPackCandidates(
  env: Env,
  repoId: string,
  cacheCtx?: CacheContext
): Promise<string[]> {
  if (cacheCtx?.memo?.packList && Array.isArray(cacheCtx.memo.packList)) {
    return cacheCtx.memo.packList;
  }
  if (cacheCtx?.memo?.packListPromise) {
    try {
      return await cacheCtx.memo.packListPromise;
    } catch {
      // Fall through and retry locally. The underlying catalog loader logs the
      // actual failure details once per request.
    }
  }

  const log = createLogger(env.LOG_LEVEL, { service: "PackDiscovery", repoId });
  const inflight = (async () => {
    const catalog = await loadActivePackCatalog(env, repoId, cacheCtx);
    const packList = catalog.map((row) => row.packKey);
    log.debug("packDiscovery:candidates", { count: packList.length });
    return packList;
  })();

  if (cacheCtx?.memo) cacheCtx.memo.packListPromise = inflight;
  try {
    const packList = await inflight;
    if (cacheCtx?.memo) cacheCtx.memo.packList = packList;
    return packList;
  } finally {
    if (cacheCtx?.memo) cacheCtx.memo.packListPromise = undefined;
  }
}
