import type { CacheContext } from "@/cache/index.ts";
import type { PackedObjectResult } from "./types.ts";

import { createBlobFromBytes, inflate } from "@/common/index.ts";
import { parseCommitRefs, parseTagTarget, parseTreeChildOids } from "@/git/core/index.ts";
import { readPackHeaderEx, readPackRange } from "@/git/pack/packMeta.ts";
import { loadActivePackCatalog } from "./catalog.ts";
import { applyGitDelta } from "./delta.ts";
import { getOidHexAt, loadIdxView } from "./idxView.ts";
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
    const header = await readPackHeaderEx(env, location.pack.packKey, location.offset);
    if (!header) return undefined;

    const payloadStart = location.offset + header.headerLen;
    const payloadLen = location.nextOffset - payloadStart;
    if (payloadLen < 0) return undefined;
    const compressed = await readPackRange(env, location.pack.packKey, payloadStart, payloadLen);
    if (!compressed) return undefined;
    const inflated = await inflate(compressed);

    const baseType = typeCodeToObjectType(header.type);
    if (baseType) return toPackedObjectResult(location, baseType, inflated);

    let base: PackedObjectResult | undefined;
    if (header.type === 6) {
      // OFS_DELTA stays inside the same pack, so once we map the base offset back to an
      // idx entry we can recurse without any extra catalog search.
      const baseOffset = location.offset - (header.baseRel || 0);
      const baseIndex = location.idx.offsetToIndex.get(baseOffset);
      if (baseIndex === undefined) return undefined;
      const baseNextOffset = location.idx.nextOffset.get(baseOffset);
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
    } else if (header.type === 7 && header.baseOid) {
      // REF_DELTA may cross pack boundaries, so fall back to the catalog-based resolver.
      base = await readObject(env, repoId, header.baseOid, cacheCtx, visited);
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
  return await Promise.all(
    oids.map(async (oid) => {
      const found = await findObject(env, repoId, oid, cacheCtx);
      return !!found;
    })
  );
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
      out.set(oid, []);
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
  if (!packs.some((pack) => pack.packKey === packKey)) return;
  const idx = await loadIdxView(env, packKey, cacheCtx);
  if (!idx) return;
  for (let i = 0; i < idx.count; i++) yield getOidHexAt(idx, i);
}

export { findObject } from "./lookup.ts";
