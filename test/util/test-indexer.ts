/**
 * Test helper: indexes a pack file using the streaming indexer.
 *
 * Replaces the legacy `indexPackOnly` (isomorphic-git) for test seeders.
 * Uses a no-op limiter and a minimal logger since tests run in miniflare
 * with no real concurrency or subrequest limits.
 */
import { scanPack, resolveDeltasAndWriteIdx } from "@/git/pack/indexer/index.ts";
import { createLogger } from "@/common/logger.ts";
import type { Limiter } from "@/git/operations/limits.ts";
import type { ResolveResult } from "@/git/pack/indexer/types.ts";
import type { PackCatalogRow } from "@/do/repo/db/schema.ts";

const noopLimiter: Limiter = { run: (_label, fn) => fn() };
const testLog = createLogger(undefined, { service: "test-seed" });

/**
 * Index a pack that is already uploaded to R2.
 *
 * Produces a `.idx` file in R2 and returns the resolve result containing
 * `objectCount` and `idxBytes` needed for `pack_catalog` row insertion.
 *
 * Pass `activeCatalog` when the pack contains REF_DELTA objects whose bases
 * live in previously indexed packs.
 */
export async function indexTestPack(
  env: Env,
  packKey: string,
  packSize: number,
  activeCatalog?: PackCatalogRow[]
): Promise<ResolveResult> {
  const scanResult = await scanPack({
    env,
    packKey,
    packSize,
    limiter: noopLimiter,
    countSubrequest: () => {},
    log: testLog,
  });
  return await resolveDeltasAndWriteIdx({
    env,
    packKey,
    packSize,
    limiter: noopLimiter,
    countSubrequest: () => {},
    log: testLog,
    scanResult,
    repoId: "test",
    activeCatalog,
  });
}
