import type { Logger } from "@/common/logger.ts";

import type { RepoStateSchema, TypedStorage } from "../repoState.ts";
import { processHydrationSlice } from "../hydration/index.ts";
import { scheduleAlarmIfSooner } from "../scheduler.ts";

export async function handleHydrationAlarmWork(args: {
  ctx: DurableObjectState;
  env: Env;
  prefix: string;
  store: TypedStorage<RepoStateSchema>;
  logger: Logger;
}): Promise<boolean> {
  try {
    const work = await args.store.get("hydrationWork");
    const queue = await args.store.get("hydrationQueue");
    const hasQueue = Array.isArray(queue) ? queue.length > 0 : !!queue;
    if (!work && !hasQueue) return false;

    return await processHydrationSlice(args.ctx, args.env, args.prefix);
  } catch (error) {
    args.logger.error("alarm:hydration:error", { error: String(error) });
    await scheduleAlarmIfSooner(args.ctx, args.env, Date.now() + 1000);
    return true;
  }
}
