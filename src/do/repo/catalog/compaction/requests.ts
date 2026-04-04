/**
 * Admin-facing compaction state transitions: preview, request, and clear.
 *
 * These operations are triggered by POST/DELETE on /admin/compact (and the
 * /admin/hydrate compatibility alias). They read or mutate `compactionWantedAt`
 * in DO storage but never acquire or release compaction leases.
 */
import type { Logger } from "@/common/logger.ts";
import type { RepoStateSchema } from "../../repoState.ts";

import { asTypedStorage } from "../../repoState.ts";
import {
  loadCompactionContext,
  scheduleCompactionWake,
  type PreviewCompactionResult,
  type RequestCompactionResult,
  type ClearCompactionRequestResult,
} from "./plan.ts";

/**
 * Preview the current compaction plan without recording a request.
 *
 * Returns a plan whenever the active catalog has a tier overflow, regardless
 * of whether `compactionWantedAt` is set or whether the repo is already in
 * streaming mode. The `queued` field indicates whether a background compaction
 * request is already recorded.
 */
export async function previewCompactionState(args: {
  ctx: DurableObjectState;
  env: Env;
  prefix: string;
  logger?: Logger;
}): Promise<PreviewCompactionResult> {
  const context = await loadCompactionContext(args);
  const queued = typeof context.wantedAt === "number";

  if (!context.plan) {
    args.logger?.info("compaction:preview-no-work", {
      currentMode: context.currentMode,
      reason: "below-threshold",
      queued,
    });
    return {
      action: "preview",
      status: "no_work",
      currentMode: context.currentMode,
      queued,
      wantedAt: context.wantedAt,
      activeCatalog: context.activeCatalog,
      packCatalogVersion: context.packCatalogVersion,
      reason: "below-threshold",
      message: "The active pack catalog is already within the compaction policy.",
    };
  }

  args.logger?.info("compaction:preview", {
    currentMode: context.currentMode,
    queued,
    sourceTier: context.plan.sourceTier,
    targetTier: context.plan.targetTier,
    sourceCount: context.plan.sourcePacks.length,
  });
  return {
    action: "preview",
    status: "ok",
    currentMode: context.currentMode,
    queued,
    wantedAt: context.wantedAt,
    activeCatalog: context.activeCatalog,
    packCatalogVersion: context.packCatalogVersion,
    plan: context.plan,
    message:
      context.currentMode === "streaming"
        ? "The active pack catalog has compactable tiers."
        : "The active pack catalog has compactable tiers. Background compaction can be requested after the repository switches to streaming mode.",
  };
}

/**
 * Record a compaction request and schedule background work.
 *
 * Only succeeds when the repository is in streaming mode and the active
 * catalog actually has a tier overflow. Clears stale `compactionWantedAt`
 * when no plan exists.
 */
export async function requestCompactionState(args: {
  ctx: DurableObjectState;
  env: Env;
  prefix: string;
  logger?: Logger;
}): Promise<RequestCompactionResult> {
  const context = await loadCompactionContext(args);

  if (context.currentMode !== "streaming") {
    args.logger?.info("compaction:request-ineligible", {
      currentMode: context.currentMode,
    });
    return {
      action: "request",
      status: "ineligible",
      currentMode: context.currentMode,
      queued: typeof context.wantedAt === "number",
      shouldEnqueue: false,
      wantedAt: context.wantedAt,
      activeCatalog: context.activeCatalog,
      packCatalogVersion: context.packCatalogVersion,
      reason: "mode-mismatch",
      message:
        "Automatic pack compaction can only be requested while the repository is in streaming mode.",
    };
  }

  if (!context.plan) {
    if (typeof context.wantedAt === "number") {
      await context.store.delete("compactionWantedAt");
    }
    args.logger?.info("compaction:request-no-work", {
      currentMode: context.currentMode,
    });
    return {
      action: "request",
      status: "no_work",
      currentMode: context.currentMode,
      queued: false,
      shouldEnqueue: false,
      activeCatalog: context.activeCatalog,
      packCatalogVersion: context.packCatalogVersion,
      reason: "below-threshold",
      message: "The active pack catalog is already within the compaction policy.",
    };
  }

  const wantedAt = Date.now();
  await context.store.put("compactionWantedAt", wantedAt);
  await scheduleCompactionWake(args.ctx, args.env);

  args.logger?.info("compaction:request", {
    currentMode: context.currentMode,
    wantedAt,
    sourceTier: context.plan.sourceTier,
    targetTier: context.plan.targetTier,
    sourceCount: context.plan.sourcePacks.length,
  });
  return {
    action: "request",
    status: "queued",
    currentMode: context.currentMode,
    queued: true,
    shouldEnqueue: true,
    wantedAt,
    activeCatalog: context.activeCatalog,
    packCatalogVersion: context.packCatalogVersion,
    plan: context.plan,
    message: "Recorded a compaction request for this repository and queued background work.",
  };
}

/** Clear any recorded compaction request without affecting active leases. */
export async function clearCompactionRequestState(args: {
  ctx: DurableObjectState;
  logger?: Logger;
}): Promise<ClearCompactionRequestResult> {
  const store = asTypedStorage<RepoStateSchema>(args.ctx.storage);
  const hadQueuedWork = typeof (await store.get("compactionWantedAt")) === "number";
  await store.delete("compactionWantedAt");
  args.logger?.info("compaction:clear", {
    hadQueuedWork,
  });
  return {
    action: "cleared",
    cleared: hadQueuedWork,
    message: hadQueuedWork
      ? "Cleared the recorded compaction request."
      : "No recorded compaction request was present.",
  };
}
