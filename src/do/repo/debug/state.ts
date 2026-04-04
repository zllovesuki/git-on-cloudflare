import type { RepoStateSchema } from "../repoState.ts";
import type { DebugStateSnapshot } from "./types.ts";

import { asTypedStorage } from "../repoState.ts";
import { doPrefix, r2LooseKey } from "@/keys.ts";
import { getDb, listPackCatalog } from "../db/index.ts";
import { activeLeaseOrUndefined, getActivePackCatalogSnapshot } from "../catalog.ts";
import { toDebugPackState } from "./types.ts";

async function listLooseSample(ctx: DurableObjectState): Promise<string[]> {
  const out: string[] = [];
  try {
    const it = await ctx.storage.list({ prefix: "obj:", limit: 10 });
    for (const key of it.keys()) out.push(String(key).slice(4));
  } catch {}
  return out;
}

function getDatabaseSize(ctx: DurableObjectState): number | undefined {
  try {
    const size = ctx.storage.sql.databaseSize;
    return typeof size === "number" ? size : undefined;
  } catch {
    return undefined;
  }
}

async function sampleR2Loose(
  prefix: string,
  env: Env
): Promise<{ bytes?: number; count?: number; truncated?: boolean }> {
  try {
    const list = await env.REPO_BUCKET.list({ prefix: r2LooseKey(prefix, ""), limit: 250 });
    let bytes = 0;
    for (const obj of list.objects || []) bytes += obj.size || 0;
    return {
      bytes,
      count: (list.objects || []).length,
      truncated: !!list.truncated,
    };
  } catch {
    return {};
  }
}

export async function debugState(ctx: DurableObjectState, env: Env): Promise<DebugStateSnapshot> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const db = getDb(ctx.storage);
  const refs = (await store.get("refs")) ?? [];
  const head = await store.get("head");
  const prefix = doPrefix(ctx.id.toString());
  const now = Date.now();

  const activeCatalogRows = await getActivePackCatalogSnapshot(ctx, env, prefix);
  const catalogRows = await listPackCatalog(db);
  const activePacks = activeCatalogRows.map(toDebugPackState);
  const supersededPacks = catalogRows
    .filter((row) => row.state === "superseded")
    .map(toDebugPackState);
  const packStats = catalogRows.map(toDebugPackState);

  const receiveLease = activeLeaseOrUndefined(await store.get("receiveLease"), now);
  const compactLease = activeLeaseOrUndefined(await store.get("compactLease"), now);
  const compactionWantedAt = await store.get("compactionWantedAt");

  const looseSample = await listLooseSample(ctx);
  const dbSizeBytes = getDatabaseSize(ctx);
  const {
    bytes: looseR2SampleBytes,
    count: looseR2SampleCount,
    truncated: looseR2Truncated,
  } = await sampleR2Loose(prefix, env);

  return {
    meta: { doId: ctx.id.toString(), prefix },
    head,
    refsCount: refs.length,
    refs: refs.slice(0, 20),
    packStats: packStats.length > 0 ? packStats : undefined,
    activePacks,
    supersededPacks,
    packCatalogVersion: (await store.get("packsetVersion")) || 0,
    receiveLease,
    compaction: {
      running: !!compactLease,
      queued: typeof compactionWantedAt === "number",
      startedAt: compactLease?.createdAt,
      wantedAt: compactionWantedAt,
      lease: compactLease,
    },
    looseSample,
    dbSizeBytes,
    looseR2SampleBytes,
    looseR2SampleCount,
    looseR2Truncated,
  };
}
