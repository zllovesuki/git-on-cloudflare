import { RepoStorageMode } from "@/do/repo/repoState";
export { RepoStorageMode };

export type RollbackCompatStatus =
  | "not_requested"
  | "queued"
  | "running"
  | "ready"
  | "stale"
  | "failed";

export type RollbackCompatControl = {
  status: RollbackCompatStatus;
  currentPacksetVersion: number;
  targetPacksetVersion?: number;
  requestedAt?: number;
  startedAt?: number;
  completedAt?: number;
  error?: string;
  message?: string;
};

export type RepoStorageModeControl = {
  status: "ok";
  currentMode: RepoStorageMode;
  canChange: boolean;
  allowedModes: RepoStorageMode[];
  activePackCount: number;
  receiveActive: boolean;
  compactionActive: boolean;
  blockers: string[];
  rollbackCompat: RollbackCompatControl;
  message?: string;
};

export type RepoStorageModeMutationResult =
  | {
      status: "ok";
      changed: boolean;
      previousMode: RepoStorageMode;
      currentMode: RepoStorageMode;
      message: string;
      control: RepoStorageModeControl;
    }
  | {
      status:
        | "unsupported_target_mode"
        | "unsupported_transition"
        | "repo_busy"
        | "no_active_packs"
        | "rollback_backfill_required";
      currentMode: RepoStorageMode;
      targetMode: string;
      message: string;
      control: RepoStorageModeControl;
    };
