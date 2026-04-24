import type { CacheContext } from "@/cache/index.ts";
import type { Logger } from "@/common/logger.ts";
import type { PackedObjectResult } from "./types.ts";

import { createBlobFromBytes } from "@/common/index.ts";
import { parseCommitRefs, parseTagTarget, parseTreeChildOids } from "@/git/core/index.ts";
import {
  MAX_SIMULTANEOUS_CONNECTIONS,
  countSubrequest,
  getLimiter,
} from "@/git/operations/limits.ts";
import { findObject } from "./lookup.ts";
import { materializePackedObjectCandidate } from "./materialize.ts";
import {
  ensureMemo,
  getPackedObjectStoreLogger,
  logOnce,
  type ResolvedLocation,
} from "./support.ts";

function countPackedSubrequest(
  cacheCtx: CacheContext | undefined,
  log: Logger,
  details: { op: string; oid?: string; packKey?: string },
  flag: string,
  n?: number
) {
  if (countSubrequest(cacheCtx, n)) return;
  logOnce(cacheCtx, flag, () => {
    log.warn("soft-budget-exhausted", details);
  });
}

async function readObjectFromLocation(
  env: Env,
  repoId: string,
  location: ResolvedLocation,
  cacheCtx: CacheContext | undefined,
  visited: Set<string>
): Promise<PackedObjectResult | undefined> {
  const limiter = getLimiter(cacheCtx);
  const log = getPackedObjectStoreLogger(env, repoId);

  // Object-store reads intentionally keep first-hit REF_DELTA semantics. The
  // indexer backfill path is the only caller that tries alternate duplicates.
  return await materializePackedObjectCandidate({
    env,
    candidate: location,
    limiter,
    countSubrequest: (n?: number) => {
      countPackedSubrequest(
        cacheCtx,
        log,
        {
          op: "r2:get-pack-entry",
          oid: location.oid,
          packKey: location.source.packKey,
        },
        "packed-read-entry-soft-budget-warned",
        n
      );
    },
    log,
    cyclePolicy: "throw",
    resolveRefBase: async (baseOid, nextVisited) => {
      return await readObject(env, repoId, baseOid, cacheCtx, nextVisited);
    },
    visited,
  });
}

export async function readObject(
  env: Env,
  repoId: string,
  oid: string,
  cacheCtx?: CacheContext,
  visited?: Set<string>
): Promise<PackedObjectResult | undefined> {
  const oidLc = oid.toLowerCase();
  ensureMemo(cacheCtx, repoId);
  const log = getPackedObjectStoreLogger(env, repoId);

  const cached = cacheCtx?.memo?.packedObjects?.get(oidLc);
  if (cached !== undefined) return cached || undefined;

  const inflight = cacheCtx?.memo?.packedObjectPromises?.get(oidLc);
  if (inflight) return await inflight;

  const promise = (async () => {
    const location = await findObject(env, repoId, oidLc, cacheCtx);
    if (!location) return undefined;
    return await readObjectFromLocation(env, repoId, location, cacheCtx, visited || new Set());
  })();

  if (cacheCtx?.memo) {
    cacheCtx.memo.packedObjectPromises = cacheCtx.memo.packedObjectPromises || new Map();
    cacheCtx.memo.packedObjectPromises.set(oidLc, promise);
  }

  try {
    const result = await promise;
    if (cacheCtx?.memo) {
      cacheCtx.memo.packedObjects = cacheCtx.memo.packedObjects || new Map();
      cacheCtx.memo.packedObjects.set(oidLc, result || null);
    }
    if (result) {
      logOnce(cacheCtx, "packed-object-read-logged", () => {
        log.debug("object-read", {
          source: "pack-catalog",
          packKey: result.packKey,
          type: result.type,
        });
      });
    }
    return result;
  } finally {
    cacheCtx?.memo?.packedObjectPromises?.delete(oidLc);
  }
}

export async function hasObjectsBatch(
  env: Env,
  repoId: string,
  oids: string[],
  cacheCtx?: CacheContext
): Promise<boolean[]> {
  const results: boolean[] = [];
  for (let i = 0; i < oids.length; i += MAX_SIMULTANEOUS_CONNECTIONS) {
    const batch = oids.slice(i, i + MAX_SIMULTANEOUS_CONNECTIONS);
    const batchResults = await Promise.all(
      batch.map(async (oid) => {
        const found = await findObject(env, repoId, oid, cacheCtx);
        return !!found;
      })
    );
    results.push(...batchResults);
  }
  return results;
}

export async function readObjectRefsBatch(
  env: Env,
  repoId: string,
  oids: string[],
  cacheCtx?: CacheContext
): Promise<Map<string, string[]>> {
  const out = new Map<string, string[]>();
  for (let index = 0; index < oids.length; index += MAX_SIMULTANEOUS_CONNECTIONS) {
    const batch = oids.slice(index, index + MAX_SIMULTANEOUS_CONNECTIONS);
    const objects = await Promise.all(batch.map((oid) => readObject(env, repoId, oid, cacheCtx)));

    for (let batchIndex = 0; batchIndex < batch.length; batchIndex++) {
      const oid = batch[batchIndex];
      const obj = objects[batchIndex];
      if (!obj) {
        // Omit missing objects so fetch closure can return the partial pack-first
        // result it actually discovered instead of inventing compatibility reads.
        continue;
      }
      if (obj.type === "commit") {
        const refs = parseCommitRefs(obj.payload);
        out.set(
          oid,
          [refs.tree, ...refs.parents].filter((value): value is string => !!value)
        );
        continue;
      }
      if (obj.type === "tree") {
        out.set(oid, parseTreeChildOids(obj.payload));
        continue;
      }
      if (obj.type === "tag") {
        const tag = parseTagTarget(obj.payload);
        out.set(oid, tag?.targetOid ? [tag.targetOid] : []);
        continue;
      }
      out.set(oid, []);
    }
  }
  return out;
}

export async function readBlobStream(
  env: Env,
  repoId: string,
  oid: string,
  cacheCtx?: CacheContext
): Promise<Response | null> {
  const obj = await readObject(env, repoId, oid, cacheCtx);
  if (!obj || obj.type !== "blob") return null;
  return new Response(createBlobFromBytes(obj.payload).stream(), {
    headers: {
      "Content-Type": "application/octet-stream",
      "Cache-Control": "public, max-age=31536000, immutable",
      ETag: `"${obj.oid}"`,
    },
  });
}

export { findObject } from "./lookup.ts";
export { logPackedObjectMismatch } from "./support.ts";
