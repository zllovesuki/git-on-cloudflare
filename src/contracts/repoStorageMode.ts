export type GuardedRepoStorageMode = "legacy" | "shadow-read";

export type RepoStorageModeControl =
  | {
      status: "ok";
      currentMode: GuardedRepoStorageMode;
      canChange: boolean;
      allowedModes: GuardedRepoStorageMode[];
      activePackCount: number;
      receiveActive: boolean;
      compactionActive: boolean;
      blockers: string[];
      message?: string;
    }
  | {
      status: "unsupported_current_mode";
      currentMode: "streaming";
      canChange: false;
      allowedModes: GuardedRepoStorageMode[];
      activePackCount: number;
      receiveActive: boolean;
      compactionActive: boolean;
      blockers: string[];
      message: string;
    };

export type RepoStorageModeMutationResult =
  | {
      status: "ok";
      changed: boolean;
      previousMode: GuardedRepoStorageMode;
      currentMode: GuardedRepoStorageMode;
      message: string;
      control: Extract<RepoStorageModeControl, { status: "ok" }>;
    }
  | {
      status: "unsupported_current_mode";
      currentMode: "streaming";
      targetMode: string;
      message: string;
      control: Extract<RepoStorageModeControl, { status: "unsupported_current_mode" }>;
    }
  | {
      status: "unsupported_target_mode";
      currentMode: string;
      targetMode: string;
      message: string;
      control: RepoStorageModeControl;
    }
  | {
      status: "repo_busy" | "no_active_packs";
      currentMode: GuardedRepoStorageMode;
      targetMode: GuardedRepoStorageMode;
      message: string;
      control: Extract<RepoStorageModeControl, { status: "ok" }>;
    };
