import type { Logger } from "@/common/logger.ts";

import { asTypedStorage } from "../repoState.ts";
import type { RepoLease, RepoStateSchema } from "../repoState.ts";
import { getActivePackCatalogSnapshot } from "./state.ts";
import {
  BeginCompactionResult,
  BeginReceiveResult,
  COMPACT_LEASE_TTL_MS,
  DEFAULT_HEAD,
  LEASE_RETRY_AFTER_SECONDS,
  ensureRepoMetadataDefaults,
  RECEIVE_LEASE_TTL_MS,
} from "./shared.ts";

export async function clearExpiredLeases(
  ctx: DurableObjectState,
  logger?: Logger,
  now: number = Date.now()
): Promise<void> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const receiveLease = await store.get("receiveLease");
  if (receiveLease && receiveLease.expiresAt <= now) {
    await store.delete("receiveLease");
    logger?.debug("lease:expired", { kind: "receive" });
  }

  const compactLease = await store.get("compactLease");
  if (compactLease && compactLease.expiresAt <= now) {
    await store.delete("compactLease");
    logger?.debug("lease:expired", { kind: "compact" });
  }
}

export async function beginReceiveLease(
  ctx: DurableObjectState,
  env: Env,
  prefix: string,
  logger?: Logger
): Promise<BeginReceiveResult> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  await clearExpiredLeases(ctx, logger);
  const existing = await store.get("receiveLease");
  if (existing) return { ok: false, retryAfter: LEASE_RETRY_AFTER_SECONDS };

  const now = Date.now();
  const lease: RepoLease = {
    token: crypto.randomUUID(),
    createdAt: now,
    expiresAt: now + RECEIVE_LEASE_TTL_MS,
  };
  await store.put("receiveLease", lease);

  const activeCatalog = await getActivePackCatalogSnapshot(ctx, env, prefix, logger);
  const repoStorageMode = await ensureRepoMetadataDefaults(store);
  const refs = (await store.get("refs")) ?? [];
  const head = (await store.get("head")) ?? DEFAULT_HEAD;

  return {
    ok: true,
    lease,
    refs,
    head,
    refsVersion: (await store.get("refsVersion")) || 0,
    packsetVersion: (await store.get("packsetVersion")) || 0,
    nextPackSeq: (await store.get("nextPackSeq")) || 1,
    repoStorageMode,
    activeCatalog,
  };
}

export async function abortReceiveLease(ctx: DurableObjectState, token: string): Promise<boolean> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const existing = await store.get("receiveLease");
  if (!existing || existing.token !== token) return false;
  await store.delete("receiveLease");
  return true;
}

export async function beginCompactionLease(
  ctx: DurableObjectState,
  env: Env,
  prefix: string,
  logger?: Logger
): Promise<BeginCompactionResult> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  await clearExpiredLeases(ctx, logger);

  const receiveLease = await store.get("receiveLease");
  if (receiveLease) {
    return { ok: false, retryAfter: LEASE_RETRY_AFTER_SECONDS, reason: "receive-active" };
  }

  const compactLease = await store.get("compactLease");
  if (compactLease) {
    return { ok: false, retryAfter: LEASE_RETRY_AFTER_SECONDS, reason: "compact-active" };
  }

  const now = Date.now();
  const lease: RepoLease = {
    token: crypto.randomUUID(),
    createdAt: now,
    expiresAt: now + COMPACT_LEASE_TTL_MS,
  };
  await store.put("compactLease", lease);

  const activeCatalog = await getActivePackCatalogSnapshot(ctx, env, prefix, logger);
  return {
    ok: true,
    lease,
    packsetVersion: (await store.get("packsetVersion")) || 0,
    nextPackSeq: (await store.get("nextPackSeq")) || 1,
    activeCatalog,
  };
}

export async function abortCompactionLease(
  ctx: DurableObjectState,
  token: string
): Promise<boolean> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const existing = await store.get("compactLease");
  if (!existing || existing.token !== token) return false;
  await store.delete("compactLease");
  return true;
}
