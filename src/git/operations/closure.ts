import type { CacheContext } from "@/cache/index.ts";

import { createLogger } from "@/common/index.ts";
import { hasObjectsBatch, iterPackOids } from "@/git/object-store/index.ts";
import { readLooseObjectRaw } from "./read/index.ts";
import { parseCommitRefs } from "@/git/core/index.ts";

/**
 * Finds common commits between server and client.
 * Used for negotiation in fetch protocol.
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
  const fallbackCandidates: string[] = [];
  for (let i = 0; i < present.length; i++) {
    const oid = cappedHaves[i]?.toLowerCase();
    if (!oid || seen.has(oid)) continue;
    if (present[i]) {
      seen.add(oid);
      found.push(oid);
      continue;
    }
    fallbackCandidates.push(oid);
  }

  const log = createLogger(env.LOG_LEVEL, { service: "FindCommonHaves", repoId });
  for (const have of fallbackCandidates.slice(0, 16)) {
    try {
      const obj = await readLooseObjectRaw(env, repoId, have, cacheCtx);
      if (obj && !seen.has(have)) {
        seen.add(have);
        found.push(have);
      }
    } catch {}
  }

  log.debug("common:haves:fallback", { tried: fallbackCandidates.length, found: found.length });
  return found;
}

/**
 * Builds a union of object IDs from multiple pack files.
 * Used for initial clone operations when client has no objects.
 */
export async function buildUnionNeededForKeys(
  env: Env,
  repoId: string,
  keys: string[],
  cacheCtx: CacheContext | undefined,
  log: { debug: (msg: string, data?: any) => void; warn: (msg: string, data?: any) => void }
) {
  const union = new Set<string>();

  if (keys.length === 0) {
    return Array.from(union);
  }

  for (const key of keys) {
    try {
      for await (const oid of iterPackOids(env, repoId, key, cacheCtx)) {
        union.add(oid);
      }
    } catch (error) {
      log.warn("union:iter-pack:error", { key, error: String(error) });
    }
  }

  log.debug("union:iter-pack", { requestedKeys: keys.length, union: union.size });
  return Array.from(union);
}

/**
 * Counts how many wanted commits have a root tree missing from a membership set.
 * Used for coverage validation to ensure pack contains all necessary objects.
 */
export async function countMissingRootTreesFromWants(
  env: Env,
  repoId: string,
  wants: string[],
  cacheCtx: CacheContext | undefined,
  membershipSet: Set<string>
): Promise<number> {
  const log = createLogger(env.LOG_LEVEL, { service: "RootTreeCheck", repoId });
  const checkMax = Math.min(16, wants.length);
  let missingCount = 0;
  const checked: string[] = [];

  for (const wantOid of wants.slice(0, checkMax)) {
    try {
      const obj = await readLooseObjectRaw(env, repoId, wantOid, cacheCtx);
      if (obj && obj.type === "commit") {
        const refs = parseCommitRefs(obj.payload);
        if (refs.tree && !membershipSet.has(refs.tree)) {
          missingCount++;
          log.debug("root-tree:missing", { commit: wantOid, tree: refs.tree });
        }
        checked.push(wantOid);
      }
    } catch (error) {
      log.debug("root-tree:check-error", { commit: wantOid, error: String(error) });
    }
  }

  log.debug("root-tree:check", {
    wants: wants.length,
    checked: checked.length,
    missingTrees: missingCount,
  });

  return missingCount;
}
