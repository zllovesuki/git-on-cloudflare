import type { RollbackCompatControl } from "@/contracts/repoStorageMode.ts";
import type { Logger } from "@/common/logger.ts";
import type { PackCatalogRow } from "../db/schema.ts";
import type { LegacyCompatBackfillState, RepoStateSchema, TypedStorage } from "../repoState.ts";

import { asTypedStorage, objKey } from "../repoState.ts";
import { getDb, insertPackOids } from "../db/index.ts";
import { getActivePackCatalogSnapshot } from "./state.ts";
import { ensureRepoMetadataDefaults } from "./shared.ts";

export type LegacyCompatBackfillCursor = {
  packIndex: number;
  objectIndex: number;
};

export type LegacyCompatBackfillBatch = {
  objects: Array<{ oid: string; zdata: Uint8Array }>;
  packObjects: Array<{ packKey: string; oids: string[] }>;
};

type LegacyCompatBackfillRequestResult =
  | {
      status: "already_ready";
      shouldEnqueue: false;
      jobId?: string;
      targetPacksetVersion: number;
      message: string;
      rollbackCompat: RollbackCompatControl;
    }
  | {
      status: "queued";
      shouldEnqueue: boolean;
      jobId: string;
      targetPacksetVersion: number;
      message: string;
      rollbackCompat: RollbackCompatControl;
    };

type BeginLegacyCompatBackfillResult =
  | {
      status: "ok";
      jobId: string;
      targetPacksetVersion: number;
      activeCatalog: PackCatalogRow[];
      progress: LegacyCompatBackfillCursor;
    }
  | {
      status: "already_ready" | "already_running" | "stale" | "not_queued";
      message: string;
      rollbackCompat: RollbackCompatControl;
    };

type StoreLegacyCompatBatchResult =
  | {
      status: "ok";
      nextProgress: LegacyCompatBackfillCursor;
    }
  | {
      status: "stale" | "not_running";
      message: string;
      rollbackCompat: RollbackCompatControl;
    };

type CompleteLegacyCompatBackfillResult =
  | {
      status: "ok";
      rollbackCompat: RollbackCompatControl;
    }
  | {
      status: "stale" | "not_running";
      message: string;
      rollbackCompat: RollbackCompatControl;
    };

const INITIAL_BACKFILL_CURSOR: LegacyCompatBackfillCursor = {
  packIndex: 0,
  objectIndex: 0,
};

function cloneCursor(cursor: LegacyCompatBackfillCursor | undefined): LegacyCompatBackfillCursor {
  if (!cursor) return { ...INITIAL_BACKFILL_CURSOR };
  return {
    packIndex: cursor.packIndex,
    objectIndex: cursor.objectIndex,
  };
}

export function buildRollbackCompatControl(
  currentPacksetVersion: number,
  state: LegacyCompatBackfillState | undefined
): RollbackCompatControl {
  if (!state) {
    return {
      status: "not_requested",
      currentPacksetVersion,
      message:
        "Rollback compatibility data has not been prepared for the current pack catalog yet.",
    };
  }

  if (state.targetPacksetVersion !== currentPacksetVersion) {
    return {
      status: "stale",
      currentPacksetVersion,
      targetPacksetVersion: state.targetPacksetVersion,
      requestedAt: state.requestedAt,
      startedAt: state.startedAt,
      completedAt: state.completedAt,
      error: state.error,
      message: "Rollback compatibility data is stale because the active pack catalog has changed.",
    };
  }

  if (state.status === "queued") {
    return {
      status: "queued",
      currentPacksetVersion,
      targetPacksetVersion: state.targetPacksetVersion,
      requestedAt: state.requestedAt,
      message: "Rollback compatibility data has been queued for preparation.",
    };
  }

  if (state.status === "running") {
    return {
      status: "running",
      currentPacksetVersion,
      targetPacksetVersion: state.targetPacksetVersion,
      requestedAt: state.requestedAt,
      startedAt: state.startedAt,
      message: "Rollback compatibility data is being rebuilt from active packs.",
    };
  }

  if (state.status === "ready") {
    return {
      status: "ready",
      currentPacksetVersion,
      targetPacksetVersion: state.targetPacksetVersion,
      requestedAt: state.requestedAt,
      startedAt: state.startedAt,
      completedAt: state.completedAt,
      message: "Rollback compatibility data is ready for the current pack catalog.",
    };
  }

  return {
    status: "failed",
    currentPacksetVersion,
    targetPacksetVersion: state.targetPacksetVersion,
    requestedAt: state.requestedAt,
    startedAt: state.startedAt,
    completedAt: state.completedAt,
    error: state.error,
    message: "Rollback compatibility data preparation failed for the current pack catalog.",
  };
}

export async function getRollbackCompatControlFromStore(
  store: TypedStorage<RepoStateSchema>
): Promise<RollbackCompatControl> {
  await ensureRepoMetadataDefaults(store);
  const currentPacksetVersion = (await store.get("packsetVersion")) || 0;
  const state = await store.get("legacyCompatBackfill");
  return buildRollbackCompatControl(currentPacksetVersion, state);
}

export async function requestLegacyCompatBackfillState(
  ctx: DurableObjectState,
  logger?: Logger
): Promise<LegacyCompatBackfillRequestResult> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  await ensureRepoMetadataDefaults(store);

  const currentPacksetVersion = (await store.get("packsetVersion")) || 0;
  const currentState = await store.get("legacyCompatBackfill");
  const rollbackCompat = buildRollbackCompatControl(currentPacksetVersion, currentState);
  if (rollbackCompat.status === "ready") {
    return {
      status: "already_ready",
      shouldEnqueue: false,
      jobId: currentState?.jobId,
      targetPacksetVersion: currentPacksetVersion,
      message: rollbackCompat.message || "Rollback compatibility data is already ready.",
      rollbackCompat,
    };
  }

  if (
    currentState &&
    currentState.targetPacksetVersion === currentPacksetVersion &&
    (currentState.status === "queued" || currentState.status === "running")
  ) {
    return {
      status: "queued",
      shouldEnqueue: currentState.status === "queued",
      jobId: currentState.jobId,
      targetPacksetVersion: currentPacksetVersion,
      message:
        currentState.status === "running"
          ? "Rollback compatibility data is already being rebuilt for the current pack catalog."
          : "Rollback compatibility data is already queued for the current pack catalog.",
      rollbackCompat,
    };
  }

  const nextState: LegacyCompatBackfillState = {
    jobId: crypto.randomUUID(),
    status: "queued",
    targetPacksetVersion: currentPacksetVersion,
    requestedAt: Date.now(),
    progress: { ...INITIAL_BACKFILL_CURSOR },
  };
  await store.put("legacyCompatBackfill", nextState);
  logger?.info("legacy-compat:queued", {
    targetPacksetVersion: currentPacksetVersion,
    jobId: nextState.jobId,
  });

  return {
    status: "queued",
    shouldEnqueue: true,
    jobId: nextState.jobId,
    targetPacksetVersion: currentPacksetVersion,
    message: "Queued rollback compatibility data preparation for the current pack catalog.",
    rollbackCompat: buildRollbackCompatControl(currentPacksetVersion, nextState),
  };
}

export async function beginLegacyCompatBackfillState(args: {
  ctx: DurableObjectState;
  env: Env;
  prefix: string;
  jobId: string;
  targetPacksetVersion: number;
  logger?: Logger;
}): Promise<BeginLegacyCompatBackfillResult> {
  const store = asTypedStorage<RepoStateSchema>(args.ctx.storage);
  await ensureRepoMetadataDefaults(store);

  const currentPacksetVersion = (await store.get("packsetVersion")) || 0;
  const currentState = await store.get("legacyCompatBackfill");
  const rollbackCompat = buildRollbackCompatControl(currentPacksetVersion, currentState);

  if (rollbackCompat.status === "ready") {
    return {
      status: "already_ready",
      message: rollbackCompat.message || "Rollback compatibility data is already ready.",
      rollbackCompat,
    };
  }

  if (
    currentPacksetVersion !== args.targetPacksetVersion ||
    !currentState ||
    currentState.jobId !== args.jobId ||
    currentState.targetPacksetVersion !== args.targetPacksetVersion
  ) {
    return {
      status: "stale",
      message: "Rollback compatibility request is stale for the current pack catalog.",
      rollbackCompat,
    };
  }

  if (currentState.status === "running") {
    return {
      status: "already_running",
      message: "Rollback compatibility request is already running for the current pack catalog.",
      rollbackCompat,
    };
  }

  if (currentState.status !== "queued") {
    return {
      status: "not_queued",
      message: "Rollback compatibility request is not queued for execution.",
      rollbackCompat,
    };
  }

  const nextState: LegacyCompatBackfillState = {
    ...currentState,
    status: "running",
    startedAt: currentState.startedAt || Date.now(),
  };
  await store.put("legacyCompatBackfill", nextState);

  const activeCatalog = await getActivePackCatalogSnapshot(
    args.ctx,
    args.env,
    args.prefix,
    args.logger
  );
  args.logger?.info("legacy-compat:begin", {
    targetPacksetVersion: args.targetPacksetVersion,
    jobId: args.jobId,
    activePackCount: activeCatalog.length,
    packIndex: nextState.progress?.packIndex || 0,
    objectIndex: nextState.progress?.objectIndex || 0,
  });

  return {
    status: "ok",
    jobId: args.jobId,
    targetPacksetVersion: args.targetPacksetVersion,
    activeCatalog,
    progress: cloneCursor(nextState.progress),
  };
}

export async function storeLegacyCompatBatchState(args: {
  ctx: DurableObjectState;
  jobId: string;
  targetPacksetVersion: number;
  batch: LegacyCompatBackfillBatch;
  nextProgress: LegacyCompatBackfillCursor;
  logger?: Logger;
}): Promise<StoreLegacyCompatBatchResult> {
  const store = asTypedStorage<RepoStateSchema>(args.ctx.storage);
  await ensureRepoMetadataDefaults(store);

  const currentPacksetVersion = (await store.get("packsetVersion")) || 0;
  const currentState = await store.get("legacyCompatBackfill");
  const rollbackCompat = buildRollbackCompatControl(currentPacksetVersion, currentState);

  if (
    currentPacksetVersion !== args.targetPacksetVersion ||
    !currentState ||
    currentState.jobId !== args.jobId ||
    currentState.targetPacksetVersion !== args.targetPacksetVersion
  ) {
    return {
      status: "stale",
      message: "Rollback compatibility request is stale for the current pack catalog.",
      rollbackCompat,
    };
  }

  if (currentState.status !== "running") {
    return {
      status: "not_running",
      message: "Rollback compatibility request is not currently running.",
      rollbackCompat,
    };
  }

  for (const object of args.batch.objects) {
    await store.put(objKey(object.oid), object.zdata);
  }

  const db = getDb(args.ctx.storage);
  for (const entry of args.batch.packObjects) {
    await insertPackOids(db, entry.packKey, entry.oids);
  }

  await store.put("legacyCompatBackfill", {
    ...currentState,
    progress: cloneCursor(args.nextProgress),
  });
  args.logger?.debug("legacy-compat:batch", {
    jobId: args.jobId,
    targetPacksetVersion: args.targetPacksetVersion,
    objectCount: args.batch.objects.length,
    packRows: args.batch.packObjects.length,
    nextPackIndex: args.nextProgress.packIndex,
    nextObjectIndex: args.nextProgress.objectIndex,
  });

  return {
    status: "ok",
    nextProgress: cloneCursor(args.nextProgress),
  };
}

export async function completeLegacyCompatBackfillState(args: {
  ctx: DurableObjectState;
  jobId: string;
  targetPacksetVersion: number;
  logger?: Logger;
}): Promise<CompleteLegacyCompatBackfillResult> {
  const store = asTypedStorage<RepoStateSchema>(args.ctx.storage);
  await ensureRepoMetadataDefaults(store);

  const currentPacksetVersion = (await store.get("packsetVersion")) || 0;
  const currentState = await store.get("legacyCompatBackfill");
  const rollbackCompat = buildRollbackCompatControl(currentPacksetVersion, currentState);

  if (
    currentPacksetVersion !== args.targetPacksetVersion ||
    !currentState ||
    currentState.jobId !== args.jobId ||
    currentState.targetPacksetVersion !== args.targetPacksetVersion
  ) {
    return {
      status: "stale",
      message: "Rollback compatibility request is stale for the current pack catalog.",
      rollbackCompat,
    };
  }

  if (currentState.status !== "running") {
    return {
      status: "not_running",
      message: "Rollback compatibility request is not currently running.",
      rollbackCompat,
    };
  }

  const nextState: LegacyCompatBackfillState = {
    ...currentState,
    status: "ready",
    completedAt: Date.now(),
    error: undefined,
  };
  await store.put("legacyCompatBackfill", nextState);
  args.logger?.info("legacy-compat:complete", {
    jobId: args.jobId,
    targetPacksetVersion: args.targetPacksetVersion,
  });

  return {
    status: "ok",
    rollbackCompat: buildRollbackCompatControl(currentPacksetVersion, nextState),
  };
}

export async function failLegacyCompatBackfillState(args: {
  ctx: DurableObjectState;
  jobId: string;
  targetPacksetVersion: number;
  error: string;
  logger?: Logger;
}): Promise<void> {
  const store = asTypedStorage<RepoStateSchema>(args.ctx.storage);
  await ensureRepoMetadataDefaults(store);

  const currentState = await store.get("legacyCompatBackfill");
  if (
    !currentState ||
    currentState.jobId !== args.jobId ||
    currentState.targetPacksetVersion !== args.targetPacksetVersion
  ) {
    return;
  }

  await store.put("legacyCompatBackfill", {
    ...currentState,
    status: "failed",
    completedAt: Date.now(),
    error: args.error,
  });
  args.logger?.warn("legacy-compat:failed", {
    jobId: args.jobId,
    targetPacksetVersion: args.targetPacksetVersion,
    error: args.error,
  });
}
