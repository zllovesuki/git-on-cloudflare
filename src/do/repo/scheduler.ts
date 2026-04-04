import type { RepoStateSchema } from "./repoState.ts";
import { asTypedStorage } from "./repoState.ts";
import { getConfig } from "./repoConfig.ts";
import { createLogger } from "@/common/index.ts";
import { activeLeaseOrUndefined } from "./catalog/activity.ts";
import { COMPACTION_REARM_DELAY_MS } from "./catalog/shared.ts";

/**
 * Plan the next alarm time purely from existing DO state and repo config.
 * Priority: unpack > hydration > min(idle, maintenance).
 */
export async function planNextAlarm(
  state: DurableObjectState,
  env: Env,
  now = Date.now()
): Promise<{
  when: number;
  reason: "unpack" | "hydration" | "compaction" | "idle" | "maint";
} | null> {
  const log = createLogger(env.LOG_LEVEL, { service: "Scheduler", doId: state.id.toString() });
  const store = asTypedStorage<RepoStateSchema>(state.storage);
  const cfg = getConfig(env);
  const repoStorageMode = (await store.get("repoStorageMode")) || "legacy";

  // 1) Streaming repos re-arm compaction via alarms and ignore stale legacy
  // unpack/hydration state. Legacy repos keep the old order.
  if (repoStorageMode === "streaming") {
    try {
      const [compactionWantedAt, receiveLease, compactLease] = await Promise.all([
        store.get("compactionWantedAt"),
        store.get("receiveLease"),
        store.get("compactLease"),
      ]);

      const activeReceiveLease = activeLeaseOrUndefined(receiveLease, now);
      if (activeReceiveLease) {
        return { when: activeReceiveLease.expiresAt, reason: "compaction" };
      }

      const activeCompactLease = activeLeaseOrUndefined(compactLease, now);
      if (activeCompactLease) {
        return { when: activeCompactLease.expiresAt, reason: "compaction" };
      }

      if (typeof compactionWantedAt === "number") {
        return { when: now + COMPACTION_REARM_DELAY_MS, reason: "compaction" };
      }
    } catch (e) {
      log.warn("sched:read-compaction-state-failed", { error: String(e) });
    }
  } else {
    // 1) Unpack has highest priority
    try {
      const [unpackWork, unpackNext] = await Promise.all([
        store.get("unpackWork"),
        store.get("unpackNext"),
      ]);
      if (unpackWork || unpackNext) {
        return { when: now + cfg.unpackDelayMs, reason: "unpack" };
      }
    } catch (e) {
      log.warn("sched:read-unpack-state-failed", { error: String(e) });
      // Continue to check hydration/idle
    }

    // 2) Hydration next
    try {
      const [hydrWork, hydrQueue] = await Promise.all([
        store.get("hydrationWork"),
        store.get("hydrationQueue"),
      ]);
      const hasQueue = Array.isArray(hydrQueue) ? hydrQueue.length > 0 : !!hydrQueue;

      // Hydration scheduling:
      // - If stage is 'error': terminal, require manual intervention; do not auto-schedule hydration
      // - Else if nextRetryAt is set in error: schedule exactly at that time (fixed interval)
      // - Else: schedule soon using unpackDelayMs as slice cadence
      if (hydrWork) {
        if (hydrWork.stage === "error") {
          const fatal = !!hydrWork.error?.fatal;
          log.warn(fatal ? "sched:hydration-fatal-error" : "sched:hydration-terminal-error", {
            message: hydrWork.error?.message,
          });
          // Do not schedule hydration automatically; continue to idle/maintenance planning
        } else {
          const nextRetryAt = hydrWork.error?.nextRetryAt;
          if (typeof nextRetryAt === "number" && nextRetryAt > now) {
            return { when: nextRetryAt, reason: "hydration" };
          }
          // Work in progress or immediate retry allowed
          return { when: now + cfg.unpackDelayMs, reason: "hydration" };
        }
      } else if (hasQueue) {
        // Queue has work but no active work
        return { when: now + cfg.unpackDelayMs, reason: "hydration" };
      }
    } catch (e) {
      log.warn("sched:read-hydration-state-failed", { error: String(e) });
      // Continue to idle/maintenance fallback
    }
  }

  // 3) Idle / Maintenance planning
  try {
    const [lastAccess, lastMaint] = await Promise.all([
      store.get("lastAccessMs"),
      store.get("lastMaintenanceMs"),
    ]);
    // Guard against past deadlines (e.g., host slept). If an idle/maintenance
    // deadline is already in the past, push it forward by its interval so we
    // do not immediately re-schedule and tight-loop alarms.
    const nextIdleAt = (lastAccess ?? now) + cfg.idleMs;
    const nextMaintAt = (lastMaint ?? now) + cfg.maintMs;
    const candidateIdle = nextIdleAt <= now ? now + cfg.idleMs : nextIdleAt;
    const candidateMaint = nextMaintAt <= now ? now + cfg.maintMs : nextMaintAt;
    const when = Math.min(candidateIdle, candidateMaint);
    const reason = candidateMaint <= candidateIdle ? "maint" : "idle";
    return { when, reason };
  } catch (e) {
    log.error("sched:plan-idle-maint-failed", { error: String(e) });
    return null;
  }
}

/**
 * Set the DO alarm only if this would fire sooner than the existing one.
 */
export async function scheduleAlarmIfSooner(
  state: DurableObjectState,
  env: Env,
  when: number,
  now = Date.now()
): Promise<{ scheduled: boolean; prev: number | null; next: number }> {
  const log = createLogger(env.LOG_LEVEL, { service: "Scheduler", doId: state.id.toString() });
  let prev: number | null = null;
  try {
    prev = (await state.storage.getAlarm()) as number | null;
  } catch (e) {
    log.warn("sched:get-alarm-failed", { error: String(e) });
    prev = null;
  }

  // Avoid redundant reset to the same timestamp (even if in the past)
  if (prev !== null && prev === when) {
    return { scheduled: false, prev, next: prev };
  }

  if (!prev || prev < now || prev > when) {
    try {
      await state.storage.setAlarm(when);
      log.debug("sched:set-alarm", { when });
      return { scheduled: true, prev: prev ?? null, next: when };
    } catch (e) {
      log.error("sched:set-alarm-failed", { error: String(e), when });
      return { scheduled: false, prev: prev ?? null, next: prev ?? when };
    }
  }
  return { scheduled: false, prev: prev ?? null, next: prev };
}

/**
 * Compute and schedule in one step. No-ops if nothing to schedule.
 */
export async function ensureScheduled(
  state: DurableObjectState,
  env: Env,
  now = Date.now()
): Promise<{
  scheduled: boolean;
  when?: number;
  reason?: "unpack" | "hydration" | "compaction" | "idle" | "maint";
}> {
  const log = createLogger(env.LOG_LEVEL, { service: "Scheduler", doId: state.id.toString() });
  try {
    const plan = await planNextAlarm(state, env, now);
    if (!plan) return { scheduled: false };
    // Clamp to a near-future time to avoid repeatedly scheduling past alarms
    const targetWhen = Math.max(plan.when, now + 5);
    const res = await scheduleAlarmIfSooner(state, env, targetWhen, now);
    if (res.scheduled) {
      log.debug("sched:alarm-set", { when: res.next, reason: plan.reason });
    }
    return { scheduled: res.scheduled, when: res.next, reason: plan.reason };
  } catch (e) {
    log.error("sched:ensure-failed", { error: String(e) });
    return { scheduled: false };
  }
}
