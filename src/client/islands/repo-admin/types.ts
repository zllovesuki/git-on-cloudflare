import type {
  RepoStorageModeControl,
  RepoStorageModeMutationResult,
} from "@/contracts/repoStorageMode.ts";

export type PackStat = {
  key: string;
  kind: "receive" | "compact" | "legacy";
  state: "active" | "superseded";
  tier: number;
  seqLo: number;
  seqHi: number;
  objectCount: number;
  packSize: number;
  indexSize: number;
  hasIndex: boolean;
  createdAt: number;
  supersededBy?: string | null;
};

export type CompactionData = {
  running?: boolean;
  startedAt?: number;
  queued?: boolean;
  wantedAt?: number;
};

export type { GuardedRepoStorageMode } from "@/contracts/repoStorageMode.ts";
export type { RepoStorageModeControl, RepoStorageModeMutationResult };

export type AdminState = {
  packStats?: PackStat[];
  meta?: { doId?: string };
  repoStorageMode?: string;
  packCatalogVersion?: number;
  activePacks?: PackStat[];
  supersededPacks?: PackStat[];
  receiveLease?: {
    createdAt: number;
    expiresAt: number;
  };
  compaction?: CompactionData;
  looseR2SampleBytes?: number;
  looseR2SampleCount?: number;
  looseR2Truncated?: boolean;
  dbSizeBytes?: number;
};

export type RepoAdminProps = {
  owner: string;
  repo: string;
  refEnc: string;
  head?: { target?: string; unborn?: boolean };
  refs: Array<{ name: string; oid: string }>;
  storageSize: string;
  packCount: number;
  packList: string[];
  state: AdminState;
  storageModeControl?: RepoStorageModeControl;
  defaultBranch: string;
  compactionStatus: string;
  compactionStartedAt?: string | null;
  compactionData?: CompactionData;
  supersededPackCount: number;
  nextMaintenanceIn?: string;
  nextMaintenanceAt?: string;
};
