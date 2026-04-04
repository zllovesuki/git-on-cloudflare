import type { Head, RepoStateSchema, RepoLease, TypedStorage } from "../repoState.ts";
import type { PackCatalogRow } from "../db/schema.ts";
import type { Ref } from "../repoState.ts";

export const RECEIVE_LEASE_TTL_MS = 30 * 60_000;
export const COMPACT_LEASE_TTL_MS = 20 * 60_000;
export const LEASE_RETRY_AFTER_SECONDS = 10;
export const COMPACTION_REARM_DELAY_MS = 60_000;
export const COMPACTION_WAKE_DELAY_MS = 5_000;
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

/**
 * Ensures all required repo metadata keys exist with sensible defaults.
 * Initializes refsVersion, packsetVersion, and nextPackSeq if not already set.
 */
export async function ensureRepoMetadataDefaults(
  store: TypedStorage<RepoStateSchema>
): Promise<void> {
  if ((await store.get("refsVersion")) === undefined) await store.put("refsVersion", 0);
  if ((await store.get("packsetVersion")) === undefined) await store.put("packsetVersion", 0);
  if ((await store.get("nextPackSeq")) === undefined) await store.put("nextPackSeq", 1);
}

export async function bumpPacksetVersion(store: TypedStorage<RepoStateSchema>): Promise<number> {
  const next = ((await store.get("packsetVersion")) || 0) + 1;
  await store.put("packsetVersion", next);
  return next;
}
