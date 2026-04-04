import type { RepoStateSchema } from "./repoState.ts";
import { asTypedStorage } from "./repoState.ts";
import { getConfig } from "./repoConfig.ts";
import { createLogger } from "@/common/index.ts";
import { activeLeaseOrUndefined } from "./catalog/activity.ts";
import { COMPACTION_REARM_DELAY_MS } from "./catalog/shared.ts";

/**
 * Plan the next alarm time purely from existing DO state and repo config.
 * Priority: compaction wake/retry, then idle cleanup.
 */
export async function planNextAlarm(
  state: DurableObjectState,
  env: Env,
  now = Date.now()
): Promise<{
  when: number;
  reason: "compaction" | "idle";
} | null> {
  const log = createLogger(env.LOG_LEVEL, { service: "Scheduler", doId: state.id.toString() });
  const store = asTypedStorage<RepoStateSchema>(state.storage);
  const cfg = getConfig(env);

  // 1) Re-arm compaction via alarms when a compaction request or lease is active.
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

  // 2) Idle cleanup planning
  try {
    const lastAccess = await store.get("lastAccessMs");
    // Guard against past deadlines (e.g., host slept). If an idle
    // deadline is already in the past, push it forward by its interval so we
    // do not immediately re-schedule and tight-loop alarms.
    const nextIdleAt = (lastAccess ?? now) + cfg.idleMs;
    const when = nextIdleAt <= now ? now + cfg.idleMs : nextIdleAt;
    return { when, reason: "idle" };
  } catch (e) {
    log.error("sched:plan-idle-failed", { error: String(e) });
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
  reason?: "compaction" | "idle";
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
