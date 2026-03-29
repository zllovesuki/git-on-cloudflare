import type { Logger } from "@/common/logger.ts";

import { asTypedStorage } from "../repoState.ts";
import type { RepoStateSchema } from "../repoState.ts";
import { ensureScheduled } from "../scheduler.ts";

export type RepoDOAccessContext = {
  ctx: DurableObjectState;
  env: Env;
  logger: Logger;
  getLastAccessMemMs(): number | undefined;
  setLastAccessMemMs(value: number): void;
};

export async function touchAndMaybeSchedule(args: RepoDOAccessContext): Promise<void> {
  const now = Date.now();
  const store = asTypedStorage<RepoStateSchema>(args.ctx.storage);
  const lastAccessMemMs = args.getLastAccessMemMs();

  // Throttle access writes so RPC-heavy read paths do not amplify storage churn.
  try {
    if (!lastAccessMemMs || now - lastAccessMemMs >= 60_000) {
      await store.put("lastAccessMs", now);
      args.setLastAccessMemMs(now);
    }
  } catch {}
  await ensureScheduled(args.ctx, args.env, now);
}

export async function ensureAccessAndAlarm(args: RepoDOAccessContext): Promise<void> {
  try {
    await touchAndMaybeSchedule(args);
  } catch (error) {
    try {
      args.logger.warn("touch:schedule:failed", { error: String(error) });
    } catch {}
  }
}
