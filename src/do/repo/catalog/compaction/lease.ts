/**
 * Queue-facing compaction state transitions: begin, commit, abort, and alarm rearm.
 *
 * These operations manage the compaction lease lifecycle. The queue consumer
 * acquires a lease via `beginCompactionState`, performs the pack rewrite in
 * worker code, and then atomically commits the result via `commitCompactionState`.
 */
import type { Logger } from "@/common/logger.ts";
import type { PackCatalogRow } from "../../db/schema.ts";
import type { RepoLease, RepoStateSchema } from "../../repoState.ts";

import { asTypedStorage } from "../../repoState.ts";
import {
  getDb,
  getPackCatalogRow,
  listActivePackCatalog,
  supersedePackCatalogRows,
  upsertPackCatalogRow,
} from "../../db/index.ts";
import { clearExpiredLeases } from "../leases.ts";
import { getActivePackCatalogSnapshot } from "../state.ts";
import {
  bumpPacksetVersion,
  COMPACT_LEASE_TTL_MS,
  COMPACTION_REARM_DELAY_MS,
  ensureRepoMetadataDefaults,
  LEASE_RETRY_AFTER_SECONDS,
} from "../shared.ts";
import { activeLeaseOrUndefined } from "../activity.ts";
import {
  selectCompactionPlan,
  catalogNeedsCompaction,
  scheduleCompactionWake,
  scheduleCompactionAlarm,
  rowsMatchForCommit,
  type BeginCompactionResult,
  type CommitCompactionResult,
} from "./plan.ts";

/**
 * Acquire a compaction lease and select source packs for compaction.
 *
 * Rejects when: no compaction request is recorded, a receive or compaction
 * lease is already active, or the active catalog is already within the
 * compaction policy.
 */
export async function beginCompactionState(args: {
  ctx: DurableObjectState;
  env: Env;
  prefix: string;
  logger?: Logger;
}): Promise<BeginCompactionResult> {
  const store = asTypedStorage<RepoStateSchema>(args.ctx.storage);
  await clearExpiredLeases(args.ctx, args.logger);
  await ensureRepoMetadataDefaults(store);

  const wantedAt = await store.get("compactionWantedAt");
  if (typeof wantedAt !== "number") {
    return {
      ok: false,
      status: "no_work",
      reason: "not-requested",
      message: "No compaction request is currently recorded for this repository.",
    };
  }

  const now = Date.now();
  const receiveLease = activeLeaseOrUndefined(await store.get("receiveLease"), now);
  if (receiveLease) {
    return {
      ok: false,
      status: "busy",
      retryAfter: LEASE_RETRY_AFTER_SECONDS,
      reason: "receive-active",
      message: "A receive lease is active, so compaction must retry later.",
    };
  }

  const compactLease = activeLeaseOrUndefined(await store.get("compactLease"), now);
  if (compactLease) {
    return {
      ok: false,
      status: "busy",
      retryAfter: LEASE_RETRY_AFTER_SECONDS,
      reason: "compact-active",
      message: "A compaction lease is already active for this repository.",
    };
  }

  const activeCatalog = await getActivePackCatalogSnapshot(
    args.ctx,
    args.env,
    args.prefix,
    args.logger
  );
  const plan = selectCompactionPlan(activeCatalog);
  if (!plan) {
    await store.delete("compactionWantedAt");
    args.logger?.info("compaction:begin-no-work", {
      reason: "below-threshold",
    });
    return {
      ok: false,
      status: "no_work",
      reason: "below-threshold",
      message: "The active pack catalog is already within the compaction policy.",
    };
  }

  const lease: RepoLease = {
    token: crypto.randomUUID(),
    createdAt: now,
    expiresAt: now + COMPACT_LEASE_TTL_MS,
  };
  await store.put("compactLease", lease);

  args.logger?.info("compaction:begin", {
    leaseToken: lease.token,
    sourceTier: plan.sourceTier,
    targetTier: plan.targetTier,
    sourceCount: plan.sourcePacks.length,
  });
  return {
    ok: true,
    lease,
    packsetVersion: (await store.get("packsetVersion")) || 0,
    activeCatalog,
    sourcePacks: plan.sourcePacks,
    targetTier: plan.targetTier,
  };
}

/**
 * Atomically commit a compaction result: insert the new pack, supersede source
 * packs, bump the packset version, and mirror legacy keys.
 *
 * Rejects with `status: "retry"` when the lease is stale, a receive lease
 * appeared, the packset version changed, or source packs were modified since
 * `beginCompactionState`.
 */
export async function commitCompactionState(args: {
  ctx: DurableObjectState;
  env: Env;
  token: string;
  sourcePacks: PackCatalogRow[];
  targetTier: number;
  packsetVersion: number;
  stagedPack: {
    packKey: string;
    packBytes: number;
    idxBytes: number;
    objectCount: number;
  };
  logger?: Logger;
}): Promise<CommitCompactionResult> {
  const store = asTypedStorage<RepoStateSchema>(args.ctx.storage);
  await ensureRepoMetadataDefaults(store);

  const lease = await store.get("compactLease");
  if (!lease || lease.token !== args.token) {
    return {
      status: "retry",
      reason: "lease-mismatch",
      message: "Compaction lease is no longer active for this request.",
    };
  }

  const receiveLease = activeLeaseOrUndefined(await store.get("receiveLease"), Date.now());
  if (receiveLease) {
    await store.delete("compactLease");
    return {
      status: "retry",
      reason: "receive-active",
      message: "A receive lease became active before compaction could commit.",
    };
  }

  const currentPacksetVersion = (await store.get("packsetVersion")) || 0;
  if (currentPacksetVersion !== args.packsetVersion) {
    await store.delete("compactLease");
    return {
      status: "retry",
      reason: "packset-changed",
      message: "The active pack catalog changed before compaction could commit.",
    };
  }

  const db = getDb(args.ctx.storage);
  const currentRows: PackCatalogRow[] = [];
  for (const sourcePack of args.sourcePacks) {
    const row = await getPackCatalogRow(db, sourcePack.packKey);
    if (row) currentRows.push(row);
  }
  if (!rowsMatchForCommit(args.sourcePacks, currentRows)) {
    await store.delete("compactLease");
    return {
      status: "retry",
      reason: "source-changed",
      message: "One or more source packs changed before compaction could commit.",
    };
  }

  let seqLo = args.sourcePacks[0]!.seqLo;
  let seqHi = args.sourcePacks[0]!.seqHi;
  for (const sourcePack of args.sourcePacks) {
    if (sourcePack.seqLo < seqLo) seqLo = sourcePack.seqLo;
    if (sourcePack.seqHi > seqHi) seqHi = sourcePack.seqHi;
  }

  await upsertPackCatalogRow(db, {
    packKey: args.stagedPack.packKey,
    kind: "compact",
    state: "active",
    tier: args.targetTier,
    seqLo,
    seqHi,
    objectCount: args.stagedPack.objectCount,
    packBytes: args.stagedPack.packBytes,
    idxBytes: args.stagedPack.idxBytes,
    createdAt: Date.now(),
    supersededBy: null,
  });
  await supersedePackCatalogRows(
    db,
    args.sourcePacks.map((row) => row.packKey),
    args.stagedPack.packKey
  );

  const activeCatalog = await listActivePackCatalog(db);
  const nextPackCatalogVersion = await bumpPacksetVersion(store);

  const shouldRequeue = catalogNeedsCompaction(activeCatalog);
  if (shouldRequeue) {
    await store.put("compactionWantedAt", Date.now());
    await scheduleCompactionWake(args.ctx, args.env);
  } else {
    await store.delete("compactionWantedAt");
  }

  await store.delete("compactLease");
  args.logger?.info("compaction:commit", {
    targetPackKey: args.stagedPack.packKey,
    supersededCount: args.sourcePacks.length,
    shouldRequeue,
    packCatalogVersion: nextPackCatalogVersion,
  });
  return {
    status: "committed",
    packCatalogVersion: nextPackCatalogVersion,
    shouldRequeue,
    supersededPackKeys: args.sourcePacks.map((row) => row.packKey),
    targetPackKey: args.stagedPack.packKey,
  };
}

/**
 * Called from the DO alarm handler for streaming repos. If `compactionWantedAt`
 * is set and no leases are active, enqueue a compaction message to the
 * maintenance queue. Reschedules the alarm on queue send failure.
 */
export async function rearmCompactionQueueFromAlarm(args: {
  ctx: DurableObjectState;
  env: Env;
  logger?: Logger;
}): Promise<boolean> {
  const store = asTypedStorage<RepoStateSchema>(args.ctx.storage);
  await ensureRepoMetadataDefaults(store);

  const wantedAt = await store.get("compactionWantedAt");
  if (typeof wantedAt !== "number") return false;

  const now = Date.now();
  if (activeLeaseOrUndefined(await store.get("receiveLease"), now)) return false;
  if (activeLeaseOrUndefined(await store.get("compactLease"), now)) return false;

  const doId = args.ctx.id.toString();
  try {
    await args.env.REPO_MAINT_QUEUE.send({
      kind: "compaction",
      doId,
    });
    args.logger?.info("compaction:alarm-rearm-enqueued", { doId });
    return true;
  } catch (error) {
    args.logger?.warn("compaction:alarm-rearm-failed", {
      doId,
      error: String(error),
    });
    await scheduleCompactionAlarm(args.ctx, args.env, COMPACTION_REARM_DELAY_MS);
    return true;
  }
}
