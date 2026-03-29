// Typed schema for Repo Durable Object storage
// Provides a light wrapper to get strong typing on storage keys/values in tests and code.

export type ObjKey = `obj:${string}`;
export type PackOidsKey = `packOids:${string}`;

export type Ref = { name: string; oid: string };
export type Head = { target: string; oid?: string; unborn?: boolean };
export type RepoStorageMode = "legacy" | "shadow-read" | "streaming";
export type RepoLease = {
  token: string;
  createdAt: number;
  expiresAt: number;
};

export type UnpackWork = {
  packKey: string;
  totalCount: number; // Total objects in the pack to process (from SQLite)
  processedCount: number;
  startedAt: number;
};

export type HydrationTask = {
  reason: HydrationReason;
  createdAt: number;
  options?: { dryRun?: boolean };
};

export type HydrationReason = "post-unpack" | "post-maint" | "admin";

export type HydrationStage =
  | "plan"
  | "scan-deltas"
  | "scan-loose"
  | "build-segment"
  | "done"
  | "error";

export type HydrationWork = {
  workId: string;
  startedAt: number;
  dryRun?: boolean;
  snapshot?: {
    lastPackKey: string | null;
    packList: string[];
    window?: string[];
  };
  stage: HydrationStage;
  progress?: {
    packIndex?: number;
    objCursor?: number;
    looseCursorKey?: string;
    segmentSeq?: number;
    producedBytes?: number;
  };
  stats?: Record<string, number>;
  error?: {
    message: string;
    fatal?: boolean; // true for integrity errors, false/undefined for transient
    retryCount?: number;
    firstErrorAt?: number;
    nextRetryAt?: number;
  };
};

export type RepoStateSchema = {
  refs: Ref[];
  head: Head;
  refsVersion: number;
  packsetVersion: number;
  nextPackSeq: number;
  receiveLease: RepoLease | undefined;
  compactLease: RepoLease | undefined;
  compactionWantedAt: number | undefined;
  repoStorageMode: RepoStorageMode | undefined;
  lastPackKey: string;
  lastPackOids: string[];
  packList: string[];
  lastAccessMs: number;
  lastMaintenanceMs: number;
  unpackWork: UnpackWork | undefined; // Pending unpack work
  unpackNext: string | undefined; // One-deep next pack key awaiting promotion
  hydrationWork: HydrationWork | undefined; // Current hydration work-in-progress
  hydrationQueue: HydrationTask[] | undefined; // FIFO queue of hydration tasks
} & Record<ObjKey, Uint8Array | ArrayBuffer> &
  Record<PackOidsKey, string[]>; // Deprecated: prefer using SQLite for pack->oid membership

export type TypedStorage<S> = {
  get<K extends keyof S & string>(key: K): Promise<S[K] | undefined>;
  get<K extends keyof S & string>(keys: K[]): Promise<Map<K, S[K] | undefined>>;
  put<K extends keyof S & string>(key: K, value: S[K]): Promise<void>;
  delete<K extends keyof S & string>(key: K): Promise<boolean | void>;
};

export function asTypedStorage<S>(storage: DurableObjectStorage): TypedStorage<S> {
  async function get<K extends keyof S & string>(key: K): Promise<S[K] | undefined>;
  async function get<K extends keyof S & string>(keys: K[]): Promise<Map<K, S[K] | undefined>>;
  async function get(keyOrKeys: any): Promise<any> {
    return storage.get(keyOrKeys as any);
  }
  const put = <K extends keyof S & string>(key: K, value: S[K]) =>
    storage.put(key as string, value);
  const del = <K extends keyof S & string>(key: K) => storage.delete(key as string);
  return { get: get as any, put: put as any, delete: del as any };
}

// Key helpers for template-literal key families
export function objKey(oid: string): ObjKey {
  return `obj:${oid}` as ObjKey;
}

export function packOidsKey(key: string): PackOidsKey {
  return `packOids:${key}` as PackOidsKey;
}
