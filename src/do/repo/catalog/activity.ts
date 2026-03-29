import { asTypedStorage } from "../repoState.ts";
import type { RepoLease, RepoStateSchema } from "../repoState.ts";

export type RepoActivitySnapshot =
  | { state: "idle"; compactionWantedAt?: number }
  | { state: "receiving"; lease: RepoLease }
  | { state: "compacting"; lease: RepoLease; compactionWantedAt?: number };

export function activeLeaseOrUndefined(
  lease: RepoLease | undefined,
  now: number
): RepoLease | undefined {
  if (!lease) return undefined;
  return lease.expiresAt > now ? lease : undefined;
}

export async function getRepoActivitySnapshot(
  ctx: DurableObjectState
): Promise<RepoActivitySnapshot> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const now = Date.now();

  const receiveLease = activeLeaseOrUndefined(await store.get("receiveLease"), now);
  if (receiveLease) {
    return { state: "receiving", lease: receiveLease };
  }

  const compactLease = activeLeaseOrUndefined(await store.get("compactLease"), now);
  const compactionWantedAt = await store.get("compactionWantedAt");
  if (compactLease) {
    return {
      state: "compacting",
      lease: compactLease,
      compactionWantedAt,
    };
  }

  return {
    state: "idle",
    compactionWantedAt,
  };
}
