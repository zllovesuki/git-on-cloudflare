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

function isRepoStorageMode(mode: string): mode is RepoStorageMode {
  return mode === "legacy" || mode === "streaming";
}

function buildModeMessage(mode: RepoStorageMode): string {
  if (mode === "legacy") {
    return "Pack-first reads are authoritative. Legacy receive/unpack/hydration paths are active.";
  }
  return "Pushes stream directly into R2 packs. Leaving this mode requires prepared rollback compatibility data.";
}

/**
 * Build the list of human-readable blockers preventing a mode change.
 *
 * Transition rules:
 * - legacy → streaming: requires activePackCount > 0, OR the repo is truly empty
 *   (no refs AND no packs AND packsetVersion === 0).
 * - streaming → legacy: requires rollback backfill ready, OR the repo is truly empty.
 *
 * The empty-repo gate requires no refs AND zero packs to avoid the unsupported
 * loose-only-with-refs case.
 */
function buildRepoStorageModeBlockers(args: {
  currentMode: RepoStorageMode;
  activePackCount: number;
  refsCount: number;
  packsetVersion: number;
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

  const isTrulyEmpty =
    args.refsCount === 0 && args.activePackCount === 0 && args.packsetVersion === 0;

  if (args.currentMode === "legacy" && args.activePackCount === 0 && !isTrulyEmpty) {
    blockers.push(
      "At least one active pack is required before enabling streaming receive (non-empty repo with no packs cannot be promoted)."
    );
  }
  if (args.currentMode === "streaming" && args.rollbackCompat.status !== "ready" && !isTrulyEmpty) {
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
  refsCount: number;
  packsetVersion: number;
  receiveActive: boolean;
  compactionActive: boolean;
  rollbackCompat: RepoStorageModeControl["rollbackCompat"];
}): boolean {
  if (args.currentMode === args.targetMode) return true;
  if (args.receiveActive || args.compactionActive) return false;

  const isTrulyEmpty =
    args.refsCount === 0 && args.activePackCount === 0 && args.packsetVersion === 0;

  if (args.currentMode === "legacy") {
    // legacy → streaming: requires active packs OR truly empty
    return args.activePackCount > 0 || isTrulyEmpty;
  }

  // streaming → legacy: requires rollback backfill ready OR truly empty
  return args.rollbackCompat.status === "ready" || isTrulyEmpty;
}

const ALLOWED_MODES: RepoStorageMode[] = ["legacy", "streaming"];

function buildRepoStorageModeControlSnapshot(args: {
  currentMode: RepoStorageMode;
  activePackCount: number;
  refsCount: number;
  packsetVersion: number;
  receiveLease?: { createdAt: number; expiresAt: number; token: string };
  compactLease?: { createdAt: number; expiresAt: number; token: string };
  rollbackCompat: RepoStorageModeControl["rollbackCompat"];
}): RepoStorageModeControl {
  const receiveActive = !!args.receiveLease;
  const compactionActive = !!args.compactLease;
  const blockers = buildRepoStorageModeBlockers(args);

  const canChange = ALLOWED_MODES.some((targetMode) =>
    targetMode !== args.currentMode
      ? canTransition({
          currentMode: args.currentMode,
          targetMode,
          activePackCount: args.activePackCount,
          refsCount: args.refsCount,
          packsetVersion: args.packsetVersion,
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
    allowedModes: ALLOWED_MODES,
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
  const [activeCatalog, refs, packsetVersion] = await Promise.all([
    getActivePackCatalogSnapshot(ctx, env, prefix, logger),
    store.get("refs"),
    store.get("packsetVersion"),
  ]);
  const now = Date.now();
  const receiveLease = activeLeaseOrUndefined(await store.get("receiveLease"), now);
  const compactLease = activeLeaseOrUndefined(await store.get("compactLease"), now);
  const rollbackCompat = await getRollbackCompatControlFromStore(store);

  return buildRepoStorageModeControlSnapshot({
    currentMode,
    activePackCount: activeCatalog.length,
    refsCount: Array.isArray(refs) ? refs.length : 0,
    packsetVersion: packsetVersion ?? 0,
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
  const [activeCatalog, refs, packsetVersion] = await Promise.all([
    getActivePackCatalogSnapshot(ctx, env, prefix, logger),
    store.get("refs"),
    store.get("packsetVersion"),
  ]);
  const now = Date.now();
  const receiveLease = activeLeaseOrUndefined(await store.get("receiveLease"), now);
  const compactLease = activeLeaseOrUndefined(await store.get("compactLease"), now);
  const rollbackCompat = await getRollbackCompatControlFromStore(store);
  const refsCount = Array.isArray(refs) ? refs.length : 0;
  const pv = packsetVersion ?? 0;

  const control = buildRepoStorageModeControlSnapshot({
    currentMode: previousMode,
    activePackCount: activeCatalog.length,
    refsCount,
    packsetVersion: pv,
    receiveLease,
    compactLease,
    rollbackCompat,
  });

  if (!isRepoStorageMode(mode)) {
    return {
      status: "unsupported_target_mode",
      currentMode: previousMode,
      targetMode: mode,
      message: "Only legacy and streaming are valid storage modes.",
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

  const isTrulyEmpty = refsCount === 0 && activeCatalog.length === 0 && pv === 0;

  if (
    previousMode === "legacy" &&
    mode === "streaming" &&
    activeCatalog.length === 0 &&
    !isTrulyEmpty
  ) {
    return {
      status: "no_active_packs",
      currentMode: previousMode,
      targetMode: mode,
      message:
        "At least one active pack is required before enabling streaming receive (non-empty repo with no packs cannot be promoted).",
      control,
    };
  }

  if (
    previousMode === "streaming" &&
    mode === "legacy" &&
    rollbackCompat.status !== "ready" &&
    !isTrulyEmpty
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
    refsCount,
    packsetVersion: pv,
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
