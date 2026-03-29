import type { Logger } from "@/common/logger.ts";
import type { PackCatalogRow } from "../db/schema.ts";
import type { RepoStorageMode } from "../repoState.ts";

import { asTypedStorage } from "../repoState.ts";
import type { RepoStateSchema } from "../repoState.ts";
import { getDb, getPackCatalogCount, listActivePackCatalog } from "../db/index.ts";
import { hydrateLegacyCatalog } from "./legacyBackfill.ts";
import { ensureRepoMetadataDefaults } from "./shared.ts";

export async function getActivePackCatalogSnapshot(
  ctx: DurableObjectState,
  env: Env,
  prefix: string,
  logger?: Logger
): Promise<PackCatalogRow[]> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  await ensureRepoMetadataDefaults(store);
  const db = getDb(ctx.storage);
  if ((await getPackCatalogCount(db)) === 0) {
    return await hydrateLegacyCatalog(ctx, store, env, prefix, logger);
  }

  // After the initial backfill, the pack catalog becomes the only authoritative
  // source for active versus superseded pack state.
  return await listActivePackCatalog(db);
}

export async function getRepoStorageModeValue(ctx: DurableObjectState): Promise<RepoStorageMode> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  return await ensureRepoMetadataDefaults(store);
}

export async function setRepoStorageModeValue(
  ctx: DurableObjectState,
  mode: RepoStorageMode,
  logger?: Logger
): Promise<RepoStorageMode> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const previous = await ensureRepoMetadataDefaults(store);
  if (previous === mode) return mode;
  await store.put("repoStorageMode", mode);
  logger?.info("repo-storage-mode:set", { from: previous, to: mode });
  return mode;
}
