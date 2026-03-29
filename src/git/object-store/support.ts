import type { CacheContext } from "@/cache/index.ts";
import type { IdxView, PackCatalogRow, PackedObjectResult } from "./types.ts";

import { createLogger } from "@/common/index.ts";

export type ResolvedLocation = {
  pack: PackCatalogRow;
  idx: IdxView;
  objectIndex: number;
  offset: number;
  nextOffset: number;
  oid: string;
};

export function typeCodeToObjectType(typeCode: number) {
  switch (typeCode) {
    case 1:
      return "commit" as const;
    case 2:
      return "tree" as const;
    case 3:
      return "blob" as const;
    case 4:
      return "tag" as const;
    default:
      return null;
  }
}

export function ensureMemo(cacheCtx: CacheContext | undefined, repoId: string) {
  if (!cacheCtx) return;
  if (!cacheCtx.memo || (cacheCtx.memo.repoId && cacheCtx.memo.repoId !== repoId)) {
    cacheCtx.memo = { repoId };
    return;
  }
  if (!cacheCtx.memo.repoId) cacheCtx.memo.repoId = repoId;
}

export function logOnce(cacheCtx: CacheContext | undefined, flag: string, fn: () => void) {
  if (!cacheCtx) {
    fn();
    return;
  }
  cacheCtx.memo = cacheCtx.memo || {};
  cacheCtx.memo.flags = cacheCtx.memo.flags || new Set<string>();
  if (cacheCtx.memo.flags.has(flag)) return;
  fn();
  cacheCtx.memo.flags.add(flag);
}

export function getPackedObjectStoreLogger(env: Env, repoId: string) {
  return createLogger(env.LOG_LEVEL, {
    service: "PackedObjectStore",
    repoId,
  });
}

export function toPackedObjectResult(
  location: ResolvedLocation,
  type: PackedObjectResult["type"],
  payload: Uint8Array
): PackedObjectResult {
  return {
    packKey: location.pack.packKey,
    objectIndex: location.objectIndex,
    offset: location.offset,
    nextOffset: location.nextOffset,
    oid: location.oid,
    type,
    payload,
  };
}

export function logPackedObjectMismatch(args: {
  env: Env;
  repoId: string;
  oid: string;
  reason: string;
  details?: Record<string, unknown>;
}) {
  const log = createLogger(args.env.LOG_LEVEL, {
    service: "PackedObjectShadow",
    repoId: args.repoId,
  });
  log.warn("shadow:mismatch", {
    oid: args.oid,
    reason: args.reason,
    ...(args.details || {}),
  });
}
