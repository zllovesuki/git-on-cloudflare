import type { Head, RepoLease } from "../repoState.ts";
import type { PackCatalogRow } from "../db/schema.ts";
import type { RollbackCompatControl } from "@/contracts/repoStorageMode.ts";

export type DebugPackState = {
  key: string;
  kind: PackCatalogRow["kind"];
  state: PackCatalogRow["state"];
  tier: number;
  seqLo: number;
  seqHi: number;
  objectCount: number;
  packSize: number;
  indexSize: number;
  hasIndex: boolean;
  createdAt: number;
  supersededBy: string | null;
};

export type DebugCompactionState = {
  running: boolean;
  queued: boolean;
  startedAt?: number;
  wantedAt?: number;
  lease?: RepoLease;
};

export type DebugRollbackCompatState = RollbackCompatControl;

export type DebugStateSnapshot = {
  meta: { doId: string; prefix: string };
  repoStorageMode: string;
  head?: Head;
  refsCount: number;
  refs: { name: string; oid: string }[];
  lastPackKey: string | null;
  lastPackOidsCount?: number;
  packListCount: number;
  packList: string[];
  packStats?: DebugPackState[];
  activePacks: DebugPackState[];
  supersededPacks: DebugPackState[];
  packCatalogVersion: number;
  receiveLease?: RepoLease;
  compaction: DebugCompactionState;
  rollbackCompat: DebugRollbackCompatState;
  unpackWork: null;
  unpackNext: null;
  looseSample: string[];
  hydrationPackCount: number;
  lastMaintenanceMs?: number;
  dbSizeBytes?: number;
  looseR2SampleBytes?: number;
  looseR2SampleCount?: number;
  looseR2Truncated?: boolean;
  hydration: {
    running: boolean;
    stage?: string;
    queued: number;
    startedAt?: number;
  };
};

export type DebugCommitCheck = {
  commit: { oid: string; parents: string[]; tree?: string };
  presence: { hasLooseCommit: boolean; hasLooseTree: boolean; hasR2LooseTree: boolean };
  membership: Record<string, { hasCommit: boolean; hasTree: boolean }>;
};

export type DebugOidCheck = {
  oid: string;
  presence: {
    hasLoose: boolean;
    hasR2Loose: boolean;
  };
  inPacks: string[];
};

export function toDebugPackState(row: PackCatalogRow): DebugPackState {
  return {
    key: row.packKey,
    kind: row.kind,
    state: row.state,
    tier: row.tier,
    seqLo: row.seqLo,
    seqHi: row.seqHi,
    objectCount: row.objectCount,
    packSize: row.packBytes,
    indexSize: row.idxBytes,
    hasIndex: row.idxBytes > 0,
    createdAt: row.createdAt,
    supersededBy: row.supersededBy,
  };
}
