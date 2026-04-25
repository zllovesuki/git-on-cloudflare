import type { ServeUploadPackPlan } from "./types.ts";
import type { RewriteOptions } from "@/git/pack/rewrite/shared.ts";

import { rewritePack, rewritePackResult, type PackRewriteResult } from "@/git/pack/rewrite.ts";

export async function resolvePackStreamResult(
  env: Env,
  plan: ServeUploadPackPlan,
  options?: RewriteOptions
): Promise<PackRewriteResult> {
  return await rewritePackResult(env, plan.snapshot, plan.neededOids, options);
}

export async function resolvePackStream(
  env: Env,
  plan: ServeUploadPackPlan,
  options?: RewriteOptions
): Promise<ReadableStream<Uint8Array> | undefined> {
  return await rewritePack(env, plan.snapshot, plan.neededOids, options);
}
