import type { CacheContext } from "@/cache/index.ts";
import type { Logger } from "@/common/logger.ts";
import type { PackedObjectResult } from "./types.ts";

import { createBlobFromBytes, inflate } from "@/common/index.ts";
import { parseCommitRefs, parseTagTarget, parseTreeChildOids } from "@/git/core/index.ts";
import {
  MAX_SIMULTANEOUS_CONNECTIONS,
  countSubrequest,
  getLimiter,
} from "@/git/operations/limits.ts";
import { readPackHeaderExFromBuf, readPackRange } from "@/git/pack/packMeta.ts";
import { loadActivePackCatalog } from "./catalog.ts";
import { applyGitDelta } from "./delta.ts";
import { getOidHexAt, findOffsetIndex, getNextOffsetByIndex, loadIdxView } from "./idxView.ts";
import { findObject } from "./lookup.ts";
import {
  ensureMemo,
  getPackedObjectStoreLogger,
  logOnce,
  logPackedObjectMismatch,
  type ResolvedLocation,
  toPackedObjectResult,
  typeCodeToObjectType,
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

async function readPackedEntry(
  env: Env,
  location: ResolvedLocation,
  limiter: ReturnType<typeof getLimiter>,
  cacheCtx: CacheContext | undefined,
  log: Logger
): Promise<
  | {
      header: NonNullable<ReturnType<typeof readPackHeaderExFromBuf>>;
      compressed: Uint8Array;
    }
  | undefined
> {
  const entryLength = location.nextOffset - location.offset;
  if (entryLength <= 0) return undefined;

  // Read one contiguous entry span instead of "header + compressed payload" as
  // separate range reads. That keeps the common read path to a single R2 fetch
  // per packed base object and still lets us parse delta metadata locally.
  const entry = await readPackRange(env, location.pack.packKey, location.offset, entryLength, {
    limiter,
    countSubrequest: (n?: number) => {
      countPackedSubrequest(
        cacheCtx,
        log,
        {
          op: "r2:get-pack-entry",
          oid: location.oid,
          packKey: location.pack.packKey,
        },
        "packed-read-entry-soft-budget-warned",
        n
      );
    },
  });
  if (!entry) return undefined;

  const header = readPackHeaderExFromBuf(entry, 0);
  if (!header) return undefined;
  const compressed = entry.subarray(header.headerLen);
  return { header, compressed };
}

async function readObjectFromLocation(
  env: Env,
  repoId: string,
  location: ResolvedLocation,
  cacheCtx: CacheContext | undefined,
  visited: Set<string>
): Promise<PackedObjectResult | undefined> {
  const visitKey = `${location.pack.packKey}#${location.objectIndex}`;
  if (visited.has(visitKey)) throw new Error("pack object recursion cycle");
  visited.add(visitKey);
  try {
    const limiter = getLimiter(cacheCtx);
    const log = getPackedObjectStoreLogger(env, repoId);
    const entry = await readPackedEntry(env, location, limiter, cacheCtx, log);
    if (!entry) return undefined;
    const inflated = await inflate(entry.compressed);

    const baseType = typeCodeToObjectType(entry.header.type);
    if (baseType) return toPackedObjectResult(location, baseType, inflated);

    let base: PackedObjectResult | undefined;
    if (entry.header.type === 6) {
      const baseOffset = location.offset - (entry.header.baseRel || 0);
      const baseIndex = findOffsetIndex(location.idx, baseOffset);
      if (baseIndex === undefined) return undefined;
      const baseNextOffset = getNextOffsetByIndex(location.idx, baseIndex);
      if (baseNextOffset === undefined) return undefined;
      base = await readObjectFromLocation(
        env,
        repoId,
        {
          pack: location.pack,
          idx: location.idx,
          objectIndex: baseIndex,
          offset: baseOffset,
          nextOffset: baseNextOffset,
          oid: getOidHexAt(location.idx, baseIndex),
        },
        cacheCtx,
        visited
      );
    } else if (entry.header.type === 7 && entry.header.baseOid) {
      base = await readObject(env, repoId, entry.header.baseOid, cacheCtx, visited);
    }
    if (!base) return undefined;

    return toPackedObjectResult(location, base.type, applyGitDelta(base.payload, inflated));
  } finally {
    visited.delete(visitKey);
  }
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
  for (const oid of oids) {
    const obj = await readObject(env, repoId, oid, cacheCtx);
    if (!obj) {
      // Omit missing objects so callers can still fall back to the compatibility
      // loose-object shim during mixed shadow-read rollout states.
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

export async function* iterPackOids(
  env: Env,
  repoId: string,
  packKey: string,
  cacheCtx?: CacheContext
): AsyncGenerator<string> {
  const packs = await loadActivePackCatalog(env, repoId, cacheCtx);
  const pack = packs.find((row) => row.packKey === packKey);
  if (!pack) return;
  const idx = await loadIdxView(env, packKey, cacheCtx, pack.packBytes);
  if (!idx) return;
  for (let i = 0; i < idx.count; i++) yield getOidHexAt(idx, i);
}

export { findObject } from "./lookup.ts";
export { logPackedObjectMismatch } from "./support.ts";
