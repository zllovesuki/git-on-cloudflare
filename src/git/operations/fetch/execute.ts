import type { ServeUploadPackPlan } from "./types.ts";
import type { RewriteOptions } from "@/git/pack/rewrite/shared.ts";

import { rewritePack } from "@/git/pack/rewrite.ts";

export async function resolvePackStream(
  env: Env,
  plan: ServeUploadPackPlan,
  options?: RewriteOptions
): Promise<ReadableStream<Uint8Array> | undefined> {
  return await rewritePack(env, plan.snapshot, plan.neededOids, options);
}
