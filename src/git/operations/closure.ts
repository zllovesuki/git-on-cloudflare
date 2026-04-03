import type { CacheContext } from "@/cache/index.ts";

import { createLogger } from "@/common/index.ts";
import { hasObjectsBatch } from "@/git/object-store/index.ts";

/**
 * Finds the subset of client-advertised haves that the server can already
 * satisfy from the active pack snapshot.
 */
export async function findCommonHaves(
  env: Env,
  repoId: string,
  haves: string[],
  cacheCtx?: CacheContext
): Promise<string[]> {
  const limit = 128;
  const cappedHaves = haves.slice(0, limit);
  const present = await hasObjectsBatch(env, repoId, cappedHaves, cacheCtx);

  const found: string[] = [];
  const seen = new Set<string>();
  for (let index = 0; index < present.length; index++) {
    const oid = cappedHaves[index]?.toLowerCase();
    if (!oid || seen.has(oid) || !present[index]) continue;
    seen.add(oid);
    found.push(oid);
  }

  const log = createLogger(env.LOG_LEVEL, { service: "FindCommonHaves", repoId });
  log.debug("common:haves", {
    requested: cappedHaves.length,
    found: found.length,
  });
  return found;
}
