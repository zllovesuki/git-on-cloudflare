import type {
  RepoStorageMode,
  RepoStorageModeControl,
  RepoStorageModeMutationResult,
} from "@/contracts/repoStorageMode.ts";
import type { Logger } from "@/common/logger.ts";
import type { RepoStateSchema } from "../repoState.ts";

import { asTypedStorage } from "../repoState.ts";
import { getActivePackCatalogSnapshot } from "./state.ts";
import { activeLeaseOrUndefined } from "./activity.ts";
import { getRollbackCompatControlFromStore } from "./legacyCompat.ts";
import { ensureRepoMetadataDefaults } from "./shared.ts";

const ALL_REPO_STORAGE_MODES: RepoStorageMode[] = ["legacy", "shadow-read", "streaming"];

function isRepoStorageMode(mode: string): mode is RepoStorageMode {
  return mode === "legacy" || mode === "shadow-read" || mode === "streaming";
}

function buildModeMessage(mode: RepoStorageMode): string {
  if (mode === "legacy") {
    return "Pack-first reads remain authoritative and compatibility validation is disabled.";
  }
  if (mode === "shadow-read") {
    return "Pack-first reads remain authoritative and are validated against compatibility reads.";
  }
  return "Pushes stream directly into R2 packs. Leaving this mode requires prepared rollback compatibility data.";
}

function buildRepoStorageModeBlockers(args: {
  currentMode: RepoStorageMode;
  activePackCount: number;
  receiveLease?: { createdAt: number; expiresAt: number; token: string };
  compactLease?: { createdAt: number; expiresAt: number; token: string };
  rollbackCompat: RepoStorageModeControl["rollbackCompat"];
}): string[] {
  const blockers: string[] = [];
  if (args.receiveLease) {
    blockers.push("A push is currently writing repository metadata.");
  }
  if (args.compactLease) {
    blockers.push("Pack compaction is currently updating repository metadata.");
  }

  if (args.currentMode === "legacy" && args.activePackCount === 0) {
    blockers.push("At least one active pack is required before enabling packed-read validation.");
  }
  if (args.currentMode === "shadow-read" && args.activePackCount === 0) {
    blockers.push("At least one active pack is required before enabling streaming receive.");
  }
  if (args.currentMode === "streaming" && args.rollbackCompat.status !== "ready") {
    blockers.push(
      args.rollbackCompat.message ||
        "Rollback compatibility data must be prepared before leaving streaming mode."
    );
  }

  return blockers;
}

function canTransition(args: {
  currentMode: RepoStorageMode;
  targetMode: RepoStorageMode;
  activePackCount: number;
  receiveActive: boolean;
  compactionActive: boolean;
  rollbackCompat: RepoStorageModeControl["rollbackCompat"];
}): boolean {
  if (args.currentMode === args.targetMode) return true;
  if (args.receiveActive || args.compactionActive) return false;

  if (args.currentMode === "legacy") {
    if (args.targetMode === "shadow-read") return args.activePackCount > 0;
    return false;
  }
  if (args.currentMode === "shadow-read") {
    if (args.targetMode === "legacy") return true;
    if (args.targetMode === "streaming") return args.activePackCount > 0;
    return false;
  }

  return (
    (args.targetMode === "legacy" || args.targetMode === "shadow-read") &&
    args.rollbackCompat.status === "ready"
  );
}

function buildRepoStorageModeControlSnapshot(args: {
  currentMode: RepoStorageMode;
  activePackCount: number;
  receiveLease?: { createdAt: number; expiresAt: number; token: string };
  compactLease?: { createdAt: number; expiresAt: number; token: string };
  rollbackCompat: RepoStorageModeControl["rollbackCompat"];
}): RepoStorageModeControl {
  const receiveActive = !!args.receiveLease;
  const compactionActive = !!args.compactLease;
  const blockers = buildRepoStorageModeBlockers(args);

  const canChange = ALL_REPO_STORAGE_MODES.some((targetMode) =>
    targetMode !== args.currentMode
      ? canTransition({
          currentMode: args.currentMode,
          targetMode,
          activePackCount: args.activePackCount,
          receiveActive,
          compactionActive,
          rollbackCompat: args.rollbackCompat,
        })
      : false
  );

  return {
    status: "ok",
    currentMode: args.currentMode,
    canChange,
    allowedModes: ALL_REPO_STORAGE_MODES,
    activePackCount: args.activePackCount,
    receiveActive,
    compactionActive,
    blockers,
    rollbackCompat: args.rollbackCompat,
    message: buildModeMessage(args.currentMode),
  };
}

export async function getRepoStorageModeControl(
  ctx: DurableObjectState,
  env: Env,
  prefix: string,
  logger?: Logger
): Promise<RepoStorageModeControl> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const currentMode = await ensureRepoMetadataDefaults(store);
  const activeCatalog = await getActivePackCatalogSnapshot(ctx, env, prefix, logger);
  const now = Date.now();
  const receiveLease = activeLeaseOrUndefined(await store.get("receiveLease"), now);
  const compactLease = activeLeaseOrUndefined(await store.get("compactLease"), now);
  const rollbackCompat = await getRollbackCompatControlFromStore(store);

  return buildRepoStorageModeControlSnapshot({
    currentMode,
    activePackCount: activeCatalog.length,
    receiveLease,
    compactLease,
    rollbackCompat,
  });
}

export async function setRepoStorageModeGuarded(
  ctx: DurableObjectState,
  env: Env,
  prefix: string,
  mode: string,
  logger?: Logger
): Promise<RepoStorageModeMutationResult> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const previousMode = await ensureRepoMetadataDefaults(store);
  const activeCatalog = await getActivePackCatalogSnapshot(ctx, env, prefix, logger);
  const now = Date.now();
  const receiveLease = activeLeaseOrUndefined(await store.get("receiveLease"), now);
  const compactLease = activeLeaseOrUndefined(await store.get("compactLease"), now);
  const rollbackCompat = await getRollbackCompatControlFromStore(store);
  const control = buildRepoStorageModeControlSnapshot({
    currentMode: previousMode,
    activePackCount: activeCatalog.length,
    receiveLease,
    compactLease,
    rollbackCompat,
  });

  if (!isRepoStorageMode(mode)) {
    return {
      status: "unsupported_target_mode",
      currentMode: previousMode,
      targetMode: mode,
      message: "Only legacy, shadow-read, and streaming are valid storage modes.",
      control,
    };
  }

  if (previousMode === mode) {
    return {
      status: "ok",
      changed: false,
      previousMode,
      currentMode: mode,
      message: `Repository storage mode is already ${mode}.`,
      control,
    };
  }

  if (receiveLease || compactLease) {
    return {
      status: "repo_busy",
      currentMode: previousMode,
      targetMode: mode,
      message: "Storage mode cannot change while a push or compaction task is active.",
      control,
    };
  }

  if (previousMode === "legacy" && mode === "shadow-read" && activeCatalog.length === 0) {
    return {
      status: "no_active_packs",
      currentMode: previousMode,
      targetMode: mode,
      message: "At least one active pack is required before enabling packed-read validation.",
      control,
    };
  }

  if (previousMode === "legacy" && mode === "streaming") {
    return {
      status: "unsupported_transition",
      currentMode: previousMode,
      targetMode: mode,
      message: "Enable shadow-read first before switching this repository to streaming receive.",
      control,
    };
  }

  if (previousMode === "shadow-read" && mode === "streaming" && activeCatalog.length === 0) {
    return {
      status: "no_active_packs",
      currentMode: previousMode,
      targetMode: mode,
      message: "At least one active pack is required before enabling streaming receive.",
      control,
    };
  }

  if (
    previousMode === "streaming" &&
    (mode === "legacy" || mode === "shadow-read") &&
    rollbackCompat.status !== "ready"
  ) {
    return {
      status: "rollback_backfill_required",
      currentMode: previousMode,
      targetMode: mode,
      message:
        rollbackCompat.message ||
        "Rollback compatibility data must be prepared before leaving streaming mode.",
      control,
    };
  }

  await store.put("repoStorageMode", mode);
  logger?.info("repo-storage-mode:set-guarded", { from: previousMode, to: mode });

  const nextControl = buildRepoStorageModeControlSnapshot({
    currentMode: mode,
    activePackCount: activeCatalog.length,
    receiveLease: undefined,
    compactLease: undefined,
    rollbackCompat,
  });

  return {
    status: "ok",
    changed: true,
    previousMode,
    currentMode: mode,
    message: `Repository storage mode is now ${mode}.`,
    control: nextControl,
  };
}
