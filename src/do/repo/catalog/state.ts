import type { Logger } from "@/common/logger.ts";
import type { PackCatalogRow } from "../db/schema.ts";

import { getDb, listActivePackCatalog } from "../db/index.ts";

export async function getActivePackCatalogSnapshot(
  ctx: DurableObjectState,
  env: Env,
  prefix: string,
  logger?: Logger
): Promise<PackCatalogRow[]> {
  const db = getDb(ctx.storage);
  return await listActivePackCatalog(db);
}
