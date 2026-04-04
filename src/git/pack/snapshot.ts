import type { CacheContext } from "@/cache/index.ts";
import type { Logger } from "@/common/logger.ts";
import type {
  OrderedPackSnapshot,
  OrderedPackSnapshotEntry,
} from "@/git/operations/fetch/types.ts";

import { getOidHexAt, loadActivePackCatalog, loadIdxView } from "@/git/object-store/index.ts";

export type SnapshotLoadResult =
  | {
      type: "Ready";
      snapshot: OrderedPackSnapshot;
    }
  | {
      type: "RepositoryNotReady";
      reason: "no-active-packs" | "snapshot-missing-idx";
    };

export async function loadOrderedPackSnapshot(
  env: Env,
  repoId: string,
  cacheCtx: CacheContext | undefined,
  log: Logger
): Promise<SnapshotLoadResult> {
  const rows = await loadActivePackCatalog(env, repoId, cacheCtx);
  if (rows.length === 0) {
    return {
      type: "RepositoryNotReady",
      reason: "no-active-packs",
    };
  }

  let idxMemoHits = 0;
  let idxLoads = 0;
  let indexedObjects = 0;
  const packs: OrderedPackSnapshotEntry[] = [];

  for (const row of rows) {
    const memoHit = cacheCtx?.memo?.idxViews?.has(row.packKey) === true;
    if (memoHit) idxMemoHits++;

    const idx = await loadIdxView(env, row.packKey, cacheCtx, row.packBytes);
    idxLoads++;
    if (!idx) {
      log.warn("stream:plan:snapshot-missing-idx", { packKey: row.packKey });
      return {
        type: "RepositoryNotReady",
        reason: "snapshot-missing-idx",
      };
    }

    indexedObjects += idx.count;
    packs.push({
      packKey: row.packKey,
      packBytes: row.packBytes,
      idx,
    });
  }

  log.info("stream:plan:snapshot", {
    packs: packs.length,
    idxLoads,
    idxMemoHits,
    idxMemoMisses: idxLoads - idxMemoHits,
    indexedObjects,
  });

  return {
    type: "Ready",
    snapshot: { packs },
  };
}

export function buildInitialCloneNeeded(snapshot: OrderedPackSnapshot): string[] {
  const needed = new Set<string>();

  for (const pack of snapshot.packs) {
    for (let index = 0; index < pack.idx.count; index++) {
      needed.add(getOidHexAt(pack.idx, index));
    }
  }

  return Array.from(needed);
}
