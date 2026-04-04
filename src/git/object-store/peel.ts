import type { CacheContext } from "@/cache/index.ts";
import type { GitObjectType } from "@/git/core/index.ts";
import type { IdxView, PackCatalogRow } from "./types.ts";

import { inflate } from "@/common/index.ts";
import { parseTagTarget } from "@/git/core/index.ts";
import { countSubrequest, getLimiter } from "@/git/operations/limits.ts";
import { SequentialReader } from "@/git/pack/indexer/resolve/reader.ts";
import { readPackHeaderExFromBuf } from "@/git/pack/packMeta.ts";
import { loadActivePackCatalog } from "./catalog.ts";
import { applyGitDelta } from "./delta.ts";
import { findOidIndex, findOffsetIndex, getNextOffsetByIndex, loadIdxView } from "./idxView.ts";
import { readObject } from "./store.ts";
import {
  ensureMemo,
  getPackedObjectStoreLogger,
  logOnce,
  typeCodeToObjectType,
} from "./support.ts";

type TagRef = {
  name: string;
  oid: string;
};

type PackedObjectPayload = {
  type: GitObjectType;
  payload: Uint8Array;
};

type PackedTagLocation = {
  pack: PackCatalogRow;
  idx: IdxView;
  objectIndex: number;
  offset: number;
  nextOffset: number;
  oid: string;
  refNames: string[];
};

const LS_REFS_PEELED_CHUNK_SIZE = 1_048_576;

function countPeelSubrequest(
  cacheCtx: CacheContext | undefined,
  log: ReturnType<typeof getPackedObjectStoreLogger>,
  packKey: string
): boolean {
  const allowed = countSubrequest(cacheCtx);
  if (allowed) return true;
  const flag = `ls-refs:peel-soft-budget:${packKey}`;
  logOnce(cacheCtx, flag, () => {
    log.warn("soft-budget-exhausted", {
      op: "r2:ls-refs-peel",
      packKey,
    });
  });
  return false;
}

function buildPackedTagLocation(
  pack: PackCatalogRow,
  idx: IdxView,
  oid: string,
  refNames: string[]
): PackedTagLocation | undefined {
  const objectIndex = findOidIndex(idx, oid);
  if (objectIndex < 0) return undefined;
  const offset = idx.offsets[objectIndex];
  const nextOffset = getNextOffsetByIndex(idx, objectIndex);
  if (nextOffset === undefined) return undefined;
  return {
    pack,
    idx,
    objectIndex,
    offset,
    nextOffset,
    oid,
    refNames,
  };
}

async function readPackedObjectFromLocation(args: {
  env: Env;
  repoId: string;
  reader: SequentialReader;
  cacheCtx: CacheContext | undefined;
  location: PackedTagLocation;
  cache: Map<number, PackedObjectPayload>;
  visited: Set<string>;
}): Promise<PackedObjectPayload | undefined> {
  const cached = args.cache.get(args.location.objectIndex);
  if (cached) return cached;

  const visitKey = `${args.location.pack.packKey}#${args.location.objectIndex}`;
  if (args.visited.has(visitKey)) {
    throw new Error(`ls-refs peel recursion cycle for ${visitKey}`);
  }
  args.visited.add(visitKey);

  try {
    const entryLength = args.location.nextOffset - args.location.offset;
    if (entryLength <= 0) return undefined;

    // Read the full pack entry in one shot. The SequentialReader keeps a
    // large sliding window, so nearby tag entries in offset order collapse
    // into a small number of sequential R2 range reads.
    const entry = await args.reader.readRange(args.location.offset, entryLength);
    const header = readPackHeaderExFromBuf(entry, 0);
    if (!header) return undefined;

    const inflated = await inflate(entry.subarray(header.headerLen));
    const baseType = typeCodeToObjectType(header.type);
    if (baseType) {
      const resolved = { type: baseType, payload: inflated };
      args.cache.set(args.location.objectIndex, resolved);
      return resolved;
    }

    if (header.type === 6) {
      const baseOffset = args.location.offset - (header.baseRel || 0);
      const baseIndex = findOffsetIndex(args.location.idx, baseOffset);
      if (baseIndex === undefined) return undefined;
      const baseNextOffset = getNextOffsetByIndex(args.location.idx, baseIndex);
      if (baseNextOffset === undefined) return undefined;
      const base = await readPackedObjectFromLocation({
        ...args,
        location: {
          pack: args.location.pack,
          idx: args.location.idx,
          objectIndex: baseIndex,
          offset: baseOffset,
          nextOffset: baseNextOffset,
          oid: args.location.oid,
          refNames: args.location.refNames,
        },
      });
      if (!base) return undefined;
      const resolved = {
        type: base.type,
        payload: applyGitDelta(base.payload, inflated),
      };
      args.cache.set(args.location.objectIndex, resolved);
      return resolved;
    }

    if (header.type === 7 && header.baseOid) {
      const samePackBase = buildPackedTagLocation(
        args.location.pack,
        args.location.idx,
        header.baseOid,
        args.location.refNames
      );
      if (samePackBase) {
        const base = await readPackedObjectFromLocation({
          ...args,
          location: samePackBase,
        });
        if (!base) return undefined;
        const resolved = {
          type: base.type,
          payload: applyGitDelta(base.payload, inflated),
        };
        args.cache.set(args.location.objectIndex, resolved);
        return resolved;
      }

      const base = await readObject(
        args.env,
        args.repoId,
        header.baseOid,
        args.cacheCtx,
        args.visited
      );
      if (!base) return undefined;
      const resolved = {
        type: base.type,
        payload: applyGitDelta(base.payload, inflated),
      };
      args.cache.set(args.location.objectIndex, resolved);
      return resolved;
    }

    return undefined;
  } finally {
    args.visited.delete(visitKey);
  }
}

export async function loadPeeledTagTargets(
  env: Env,
  repoId: string,
  refs: TagRef[],
  cacheCtx?: CacheContext
): Promise<Map<string, string>> {
  const peeledByRef = new Map<string, string>();
  if (refs.length === 0) return peeledByRef;

  ensureMemo(cacheCtx, repoId);
  const log = getPackedObjectStoreLogger(env, repoId);
  const packs = await loadActivePackCatalog(env, repoId, cacheCtx);
  if (packs.length === 0) return peeledByRef;

  const unresolved = new Map<string, string[]>();
  for (const ref of refs) {
    const oid = ref.oid.toLowerCase();
    const existing = unresolved.get(oid);
    if (existing) {
      existing.push(ref.name);
    } else {
      unresolved.set(oid, [ref.name]);
    }
  }

  const locationsByPack = new Map<string, PackedTagLocation[]>();
  for (const pack of packs) {
    if (unresolved.size === 0) break;
    const idx = await loadIdxView(env, pack.packKey, cacheCtx, pack.packBytes);
    if (!idx) continue;

    for (const [oid, refNames] of Array.from(unresolved.entries())) {
      const location = buildPackedTagLocation(pack, idx, oid, refNames);
      if (!location) continue;
      const locations = locationsByPack.get(pack.packKey) || [];
      locations.push(location);
      locationsByPack.set(pack.packKey, locations);
      unresolved.delete(oid);
    }
  }

  log.debug("ls-refs:peel-batch", {
    requestedRefs: refs.length,
    matchedPacks: locationsByPack.size,
    unresolvedTags: unresolved.size,
  });

  const limiter = getLimiter(cacheCtx);
  for (const [packKey, locations] of locationsByPack) {
    const pack = locations[0]?.pack;
    if (!pack) continue;

    locations.sort((left, right) => left.offset - right.offset);
    const reader = new SequentialReader(
      env,
      packKey,
      pack.packBytes,
      LS_REFS_PEELED_CHUNK_SIZE,
      limiter,
      () => countPeelSubrequest(cacheCtx, log, packKey),
      log,
      cacheCtx?.req.signal
    );
    const cache = new Map<number, PackedObjectPayload>();

    for (const location of locations) {
      const object = await readPackedObjectFromLocation({
        env,
        repoId,
        reader,
        cacheCtx,
        location,
        cache,
        visited: new Set<string>(),
      });
      if (!object || object.type !== "tag") continue;
      const peeled = parseTagTarget(object.payload);
      if (!peeled?.targetOid) continue;
      for (const refName of location.refNames) {
        peeledByRef.set(refName, peeled.targetOid);
      }
    }
  }

  return peeledByRef;
}
