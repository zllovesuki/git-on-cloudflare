import type { Logger } from "@/common/logger.ts";
import type { PackCatalogRow } from "../db/schema.ts";
import type { RepoStateSchema } from "../repoState.ts";

import { asTypedStorage } from "../repoState.ts";
import { getActivePackCatalogSnapshot } from "../catalog.ts";

export async function previewCompactionState(args: {
  ctx: DurableObjectState;
  env: Env;
  prefix: string;
  logger: Logger;
}): Promise<{
  action: "preview";
  message: string;
  queued: boolean;
  wantedAt?: number;
  activeCatalog: PackCatalogRow[];
  packCatalogVersion: number;
}> {
  const store = asTypedStorage<RepoStateSchema>(args.ctx.storage);
  const wantedAt = await store.get("compactionWantedAt");
  const packCatalogVersion = (await store.get("packsetVersion")) || 0;
  const activeCatalog = await getActivePackCatalogSnapshot(
    args.ctx,
    args.env,
    args.prefix,
    args.logger
  );
  args.logger.info("compaction:preview", {
    queued: typeof wantedAt === "number",
    wantedAt,
    packCatalogVersion,
    activePackCount: activeCatalog.length,
  });
  return {
    action: "preview",
    message: "Previewed compaction request state. No new request was recorded.",
    queued: typeof wantedAt === "number",
    wantedAt,
    activeCatalog,
    packCatalogVersion,
  };
}

export async function requestCompactionState(args: {
  ctx: DurableObjectState;
  env: Env;
  prefix: string;
  logger: Logger;
}): Promise<{
  action: "queued";
  message: string;
  queued: true;
  wantedAt: number;
  activeCatalog: PackCatalogRow[];
  packCatalogVersion: number;
}> {
  const store = asTypedStorage<RepoStateSchema>(args.ctx.storage);
  const wantedAt = Date.now();
  await store.put("compactionWantedAt", wantedAt);
  const packCatalogVersion = (await store.get("packsetVersion")) || 0;
  const activeCatalog = await getActivePackCatalogSnapshot(
    args.ctx,
    args.env,
    args.prefix,
    args.logger
  );
  args.logger.info("compaction:request", {
    wantedAt,
    packCatalogVersion,
    activePackCount: activeCatalog.length,
  });
  return {
    action: "queued",
    message:
      "Recorded a compaction request in repo metadata. Background compaction is not wired yet.",
    queued: true,
    wantedAt,
    activeCatalog,
    packCatalogVersion,
  };
}

export async function clearCompactionRequestState(args: {
  ctx: DurableObjectState;
  logger: Logger;
}): Promise<{
  action: "cleared";
  cleared: boolean;
  message: string;
}> {
  const store = asTypedStorage<RepoStateSchema>(args.ctx.storage);
  const hadQueuedWork = typeof (await store.get("compactionWantedAt")) === "number";
  await store.delete("compactionWantedAt");
  const packCatalogVersion = (await store.get("packsetVersion")) || 0;
  args.logger.info("compaction:clear", {
    hadQueuedWork,
    packCatalogVersion,
  });
  return {
    action: "cleared",
    cleared: hadQueuedWork,
    message: hadQueuedWork
      ? "Cleared the recorded compaction request."
      : "No recorded compaction request was present.",
  };
}
