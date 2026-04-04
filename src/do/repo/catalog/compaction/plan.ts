import type { Logger } from "@/common/logger.ts";
import type { PackCatalogRow } from "../../db/schema.ts";
import type { RepoLease, RepoStateSchema, RepoStorageMode, TypedStorage } from "../../repoState.ts";

import { asTypedStorage } from "../../repoState.ts";
import { scheduleAlarmIfSooner } from "../../scheduler.ts";
import { getActivePackCatalogSnapshot } from "../state.ts";
import { COMPACTION_WAKE_DELAY_MS, ensureRepoMetadataDefaults } from "../shared.ts";

const COMPACTION_FAN_IN = 4;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type CompactionTierState = {
  tier: number;
  activePackCount: number;
};

export type CompactionPlan = {
  sourceTier: number;
  targetTier: number;
  sourcePacks: PackCatalogRow[];
  sourceBytes: number;
  sourceObjects: number;
  tiers: CompactionTierState[];
};

export type PreviewCompactionResult = {
  action: "preview";
  status: "ok" | "no_work";
  currentMode: RepoStorageMode;
  queued: boolean;
  wantedAt?: number;
  activeCatalog: PackCatalogRow[];
  packCatalogVersion: number;
  plan?: CompactionPlan;
  reason?: "below-threshold";
  message: string;
};

export type RequestCompactionResult = {
  action: "request";
  status: "queued" | "no_work" | "ineligible";
  currentMode: RepoStorageMode;
  queued: boolean;
  shouldEnqueue: boolean;
  wantedAt?: number;
  activeCatalog: PackCatalogRow[];
  packCatalogVersion: number;
  plan?: CompactionPlan;
  reason?: "mode-mismatch" | "below-threshold";
  message: string;
};

export type ClearCompactionRequestResult = {
  action: "cleared";
  cleared: boolean;
  message: string;
};

export type BeginCompactionResult =
  | {
      ok: true;
      currentMode: RepoStorageMode;
      lease: RepoLease;
      packsetVersion: number;
      activeCatalog: PackCatalogRow[];
      sourcePacks: PackCatalogRow[];
      targetTier: number;
    }
  | {
      ok: false;
      status: "busy";
      currentMode: RepoStorageMode;
      retryAfter: number;
      reason: "receive-active" | "compact-active";
      message: string;
    }
  | {
      ok: false;
      status: "no_work";
      currentMode: RepoStorageMode;
      reason: "not-requested" | "below-threshold";
      message: string;
    }
  | {
      ok: false;
      status: "ineligible";
      currentMode: RepoStorageMode;
      reason: "mode-mismatch";
      message: string;
    };

export type CommitCompactionResult =
  | {
      status: "committed";
      currentMode: RepoStorageMode;
      packCatalogVersion: number;
      shouldRequeue: boolean;
      supersededPackKeys: string[];
      targetPackKey: string;
    }
  | {
      status: "retry";
      currentMode: RepoStorageMode;
      reason: "receive-active" | "lease-mismatch" | "packset-changed" | "source-changed";
      message: string;
    }
  | {
      status: "ineligible";
      currentMode: RepoStorageMode;
      reason: "mode-mismatch";
      message: string;
    };

// ---------------------------------------------------------------------------
// Plan selection
// ---------------------------------------------------------------------------

function summarizeTierCounts(activeCatalog: PackCatalogRow[]): CompactionTierState[] {
  const counts = new Map<number, number>();
  for (const pack of activeCatalog) {
    counts.set(pack.tier, (counts.get(pack.tier) || 0) + 1);
  }

  return Array.from(counts.entries())
    .map(([tier, activePackCount]) => ({ tier, activePackCount }))
    .sort((left, right) => left.tier - right.tier);
}

function sumSourceBytes(sourcePacks: PackCatalogRow[]): number {
  let total = 0;
  for (const pack of sourcePacks) total += pack.packBytes;
  return total;
}

function sumSourceObjects(sourcePacks: PackCatalogRow[]): number {
  let total = 0;
  for (const pack of sourcePacks) total += pack.objectCount;
  return total;
}

export function selectCompactionPlan(activeCatalog: PackCatalogRow[]): CompactionPlan | undefined {
  const tiers = summarizeTierCounts(activeCatalog);
  const overflowingTier = tiers.find((tier) => tier.activePackCount > COMPACTION_FAN_IN);
  if (!overflowingTier) return undefined;

  const sourcePacks = activeCatalog
    .filter((pack) => pack.tier === overflowingTier.tier)
    .sort((left, right) => left.seqLo - right.seqLo)
    .slice(0, COMPACTION_FAN_IN);
  if (sourcePacks.length < COMPACTION_FAN_IN) return undefined;

  return {
    sourceTier: overflowingTier.tier,
    targetTier: overflowingTier.tier + 1,
    sourcePacks,
    sourceBytes: sumSourceBytes(sourcePacks),
    sourceObjects: sumSourceObjects(sourcePacks),
    tiers,
  };
}

export function catalogNeedsCompaction(activeCatalog: PackCatalogRow[]): boolean {
  return selectCompactionPlan(activeCatalog) !== undefined;
}

// ---------------------------------------------------------------------------
// Shared helpers used by requests.ts and lease.ts
// ---------------------------------------------------------------------------

/** Returns true if every source pack in the commit request still matches the current catalog. */
export function rowsMatchForCommit(
  sourcePacks: PackCatalogRow[],
  currentRows: PackCatalogRow[]
): boolean {
  if (sourcePacks.length !== currentRows.length) return false;
  for (let index = 0; index < sourcePacks.length; index++) {
    const expected = sourcePacks[index];
    const current = currentRows[index];
    if (!current) return false;
    if (current.packKey !== expected.packKey) return false;
    if (current.state !== "active") return false;
    if (current.kind !== expected.kind) return false;
    if (current.tier !== expected.tier) return false;
    if (current.seqLo !== expected.seqLo || current.seqHi !== expected.seqHi) return false;
    if (current.objectCount !== expected.objectCount) return false;
    if (current.packBytes !== expected.packBytes || current.idxBytes !== expected.idxBytes)
      return false;
  }
  return true;
}

export async function scheduleCompactionAlarm(
  ctx: DurableObjectState,
  env: Env,
  delayMs: number
): Promise<void> {
  await scheduleAlarmIfSooner(ctx, env, Date.now() + delayMs);
}

export async function scheduleCompactionWake(ctx: DurableObjectState, env: Env): Promise<void> {
  await scheduleCompactionAlarm(ctx, env, COMPACTION_WAKE_DELAY_MS);
}

/** Load shared compaction context: storage mode, catalog, plan, and queued state. */
export async function loadCompactionContext(args: {
  ctx: DurableObjectState;
  env: Env;
  prefix: string;
  logger?: Logger;
}): Promise<{
  store: TypedStorage<RepoStateSchema>;
  currentMode: RepoStorageMode;
  packCatalogVersion: number;
  wantedAt: number | undefined;
  activeCatalog: PackCatalogRow[];
  plan: CompactionPlan | undefined;
}> {
  const store = asTypedStorage<RepoStateSchema>(args.ctx.storage);
  const currentMode = await ensureRepoMetadataDefaults(store);
  const packCatalogVersion = (await store.get("packsetVersion")) || 0;
  const wantedAt = await store.get("compactionWantedAt");
  const activeCatalog = await getActivePackCatalogSnapshot(
    args.ctx,
    args.env,
    args.prefix,
    args.logger
  );
  const plan = selectCompactionPlan(activeCatalog);

  return {
    store,
    currentMode,
    packCatalogVersion,
    wantedAt,
    activeCatalog,
    plan,
  };
}
