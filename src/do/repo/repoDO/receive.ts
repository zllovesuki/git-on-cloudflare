import type { Logger } from "@/common/logger.ts";

import { asTypedStorage } from "../repoState.ts";
import type { RepoStateSchema } from "../repoState.ts";
import { receivePack } from "@/git/index.ts";

export async function handleReceiveRequest(args: {
  ctx: DurableObjectState;
  env: Env;
  prefix: string;
  request: Request;
  logger: Logger;
}): Promise<Response> {
  args.logger.info("receive:start", {});

  // The legacy receive path still supports one active unpack plus one queued pack.
  // Reject before reading the body when both slots are already occupied.
  try {
    const store = asTypedStorage<RepoStateSchema>(args.ctx.storage);
    const unpackWork = await store.get("unpackWork");
    const unpackNext = await store.get("unpackNext");
    if (unpackWork && unpackNext) {
      args.logger.warn("receive:block-busy", { retryAfter: 10 });
      return new Response("Repository is busy unpacking; please retry shortly.\n", {
        status: 503,
        headers: {
          "Retry-After": "10",
          "Content-Type": "text/plain; charset=utf-8",
        },
      });
    }
  } catch {}

  const response = await receivePack(args.ctx, args.env, args.prefix, args.request);
  args.logger.info("receive:end", { status: response.status });
  return response;
}
