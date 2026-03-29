import type {
  GuardedRepoStorageMode,
  RepoStorageModeControl,
  RepoStorageModeMutationResult,
} from "@/contracts/repoStorageMode.ts";
import type { Logger } from "@/common/logger.ts";
import type { RepoStorageMode, RepoStateSchema } from "../repoState.ts";

import { asTypedStorage } from "../repoState.ts";
import { getActivePackCatalogSnapshot } from "./state.ts";
import { activeLeaseOrUndefined } from "./activity.ts";
import { ensureRepoMetadataDefaults, GUARDED_REPO_STORAGE_MODES } from "./shared.ts";

function isGuardedRepoStorageMode(mode: string): mode is GuardedRepoStorageMode {
  return mode === "legacy" || mode === "shadow-read";
}

function buildRepoStorageModeBlockers(args: {
  currentMode: GuardedRepoStorageMode;
  activePackCount: number;
  receiveLease?: { createdAt: number; expiresAt: number; token: string };
  compactLease?: { createdAt: number; expiresAt: number; token: string };
}): string[] {
  const blockers: string[] = [];
  if (args.receiveLease) {
    blockers.push("A push is currently writing repository metadata.");
  }
  if (args.compactLease) {
    blockers.push("Pack compaction is currently updating repository metadata.");
  }
  if (args.currentMode === "legacy" && args.activePackCount === 0) {
    blockers.push("Packed reads validation requires at least one active pack.");
  }
  return blockers;
}

function buildRepoStorageModeControlSnapshot(args: {
  currentMode: RepoStorageMode;
  activePackCount: number;
  receiveLease?: { createdAt: number; expiresAt: number; token: string };
  compactLease?: { createdAt: number; expiresAt: number; token: string };
}): RepoStorageModeControl {
  const receiveActive = !!args.receiveLease;
  const compactionActive = !!args.compactLease;
  if (args.currentMode === "streaming") {
    return {
      status: "unsupported_current_mode",
      currentMode: "streaming",
      canChange: false,
      allowedModes: GUARDED_REPO_STORAGE_MODES,
      activePackCount: args.activePackCount,
      receiveActive,
      compactionActive,
      blockers: ["This admin control only manages validation-only legacy and shadow-read modes."],
      message: "The current storage mode is outside the validation-only admin control.",
    };
  }

  const blockers = buildRepoStorageModeBlockers({
    currentMode: args.currentMode,
    activePackCount: args.activePackCount,
    receiveLease: args.receiveLease,
    compactLease: args.compactLease,
  });
  const canChange =
    args.currentMode === "legacy" ? blockers.length === 0 : !receiveActive && !compactionActive;

  return {
    status: "ok",
    currentMode: args.currentMode,
    canChange,
    allowedModes: GUARDED_REPO_STORAGE_MODES,
    activePackCount: args.activePackCount,
    receiveActive,
    compactionActive,
    blockers,
    message:
      args.currentMode === "shadow-read"
        ? "Packed reads stay authoritative and are validated against compatibility reads."
        : "Packed reads stay authoritative and packed-read validation is disabled.",
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
  return buildRepoStorageModeControlSnapshot({
    currentMode,
    activePackCount: activeCatalog.length,
    receiveLease,
    compactLease,
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
  const control = buildRepoStorageModeControlSnapshot({
    currentMode: previousMode,
    activePackCount: activeCatalog.length,
    receiveLease,
    compactLease,
  });

  if (control.status === "unsupported_current_mode") {
    return {
      status: "unsupported_current_mode",
      currentMode: control.currentMode,
      targetMode: mode,
      message: control.message,
      control,
    };
  }

  const currentMode = control.currentMode;

  if (!isGuardedRepoStorageMode(mode)) {
    return {
      status: "unsupported_target_mode",
      currentMode,
      targetMode: mode,
      message: "Only legacy and shadow-read can be selected from this validation control.",
      control,
    };
  }

  if (receiveLease || compactLease) {
    return {
      status: "repo_busy",
      currentMode,
      targetMode: mode,
      message: "Storage mode cannot change while a push or compaction task is active.",
      control,
    };
  }

  if (previousMode === "legacy" && mode === "shadow-read" && activeCatalog.length === 0) {
    return {
      status: "no_active_packs",
      currentMode,
      targetMode: mode,
      message: "Packed reads validation requires at least one active pack.",
      control,
    };
  }

  if (previousMode === mode) {
    return {
      status: "ok",
      changed: false,
      previousMode: currentMode,
      currentMode: mode,
      message:
        mode === "shadow-read"
          ? "Packed-read validation was already enabled."
          : "Packed-read validation was already disabled.",
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
  });

  if (nextControl.status !== "ok") {
    return {
      status: "unsupported_current_mode",
      currentMode: nextControl.currentMode,
      targetMode: mode,
      message: nextControl.message,
      control: nextControl,
    };
  }

  return {
    status: "ok",
    changed: true,
    previousMode: currentMode,
    currentMode: mode,
    message:
      mode === "shadow-read"
        ? "Packed-read validation is now enabled."
        : "Packed-read validation is now disabled.",
    control: nextControl,
  };
}
