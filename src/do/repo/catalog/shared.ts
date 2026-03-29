import type {
  Head,
  RepoLease,
  RepoStateSchema,
  RepoStorageMode,
  TypedStorage,
} from "../repoState.ts";
import type { PackCatalogRow } from "../db/schema.ts";
import type { Ref } from "../repoState.ts";
import type { GuardedRepoStorageMode } from "@/contracts/repoStorageMode.ts";

export const RECEIVE_LEASE_TTL_MS = 30 * 60_000;
export const COMPACT_LEASE_TTL_MS = 20 * 60_000;
export const LEASE_RETRY_AFTER_SECONDS = 10;
export const DEFAULT_HEAD: Head = { target: "refs/heads/main", unborn: true };
export const IDX_HEADER_LEN = 8 + 256 * 4;

export type BeginReceiveResult =
  | { ok: false; retryAfter: number }
  | {
      ok: true;
      lease: RepoLease;
      refs: Ref[];
      head: Head;
      refsVersion: number;
      packsetVersion: number;
      nextPackSeq: number;
      repoStorageMode: RepoStorageMode;
      activeCatalog: PackCatalogRow[];
    };

export type BeginCompactionResult =
  | { ok: false; retryAfter: number; reason: "receive-active" | "compact-active" }
  | {
      ok: true;
      lease: RepoLease;
      packsetVersion: number;
      nextPackSeq: number;
      activeCatalog: PackCatalogRow[];
    };

export function uniq(items: Array<string | undefined>): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const item of items) {
    if (!item || seen.has(item)) continue;
    seen.add(item);
    out.push(item);
  }
  return out;
}

export const GUARDED_REPO_STORAGE_MODES: GuardedRepoStorageMode[] = ["legacy", "shadow-read"];

export async function ensureRepoMetadataDefaults(
  store: TypedStorage<RepoStateSchema>
): Promise<RepoStorageMode> {
  let mode = await store.get("repoStorageMode");
  if (!mode) {
    mode = "legacy";
    await store.put("repoStorageMode", mode);
  }
  if ((await store.get("refsVersion")) === undefined) await store.put("refsVersion", 0);
  if ((await store.get("packsetVersion")) === undefined) await store.put("packsetVersion", 0);
  if ((await store.get("nextPackSeq")) === undefined) await store.put("nextPackSeq", 1);
  return mode;
}

export async function bumpPacksetVersion(store: TypedStorage<RepoStateSchema>): Promise<number> {
  const next = ((await store.get("packsetVersion")) || 0) + 1;
  await store.put("packsetVersion", next);
  return next;
}
