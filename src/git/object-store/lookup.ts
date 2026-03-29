import type { CacheContext } from "@/cache/index.ts";

import { getOidHexAt, findOidIndex, getNextOffsetByIndex, loadIdxView } from "./idxView.ts";
import { loadActivePackCatalog } from "./catalog.ts";
import {
  ensureMemo,
  getPackedObjectStoreLogger,
  logOnce,
  type ResolvedLocation,
} from "./support.ts";

export async function findObject(
  env: Env,
  repoId: string,
  oid: string,
  cacheCtx?: CacheContext
): Promise<ResolvedLocation | undefined> {
  ensureMemo(cacheCtx, repoId);
  const log = getPackedObjectStoreLogger(env, repoId);
  const packs = await loadActivePackCatalog(env, repoId, cacheCtx);
  let packsScanned = 0;

  for (const pack of packs) {
    packsScanned++;
    const idx = await loadIdxView(env, pack.packKey, cacheCtx, pack.packBytes);
    if (!idx) continue;

    const objectIndex = findOidIndex(idx, oid);
    if (objectIndex < 0) continue;

    const offset = idx.offsets[objectIndex];
    const noff = getNextOffsetByIndex(idx, objectIndex);
    if (noff === undefined) continue;

    logOnce(cacheCtx, "packed-chosen-pack-logged", () => {
      log.debug("chosen-pack", {
        oid: oid.toLowerCase(),
        packKey: pack.packKey,
        packsScanned,
      });
    });

    return {
      pack,
      idx,
      objectIndex,
      offset,
      nextOffset: noff,
      oid: getOidHexAt(idx, objectIndex),
    };
  }

  log.debug("packed-object:miss", { oid: oid.toLowerCase(), packsScanned });
  return undefined;
}
