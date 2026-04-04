// Typed schema for Repo Durable Object storage
// Provides a light wrapper to get strong typing on storage keys/values in tests and code.

export type ObjKey = `obj:${string}`;

export type Ref = { name: string; oid: string };
export type Head = { target: string; oid?: string; unborn?: boolean };
export type RepoLease = {
  token: string;
  createdAt: number;
  expiresAt: number;
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
  lastAccessMs: number;
} & Record<ObjKey, Uint8Array | ArrayBuffer>;

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
