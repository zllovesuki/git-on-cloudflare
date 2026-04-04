import { createExecutionContext, env as testEnv } from "cloudflare:test";
import {
  handleRepoMaintenanceQueue,
  type RepoMaintenanceQueueMessage,
} from "@/maintenance/queue.ts";

export type QueueRunResult = {
  acked: boolean;
  retried: boolean;
};

/**
 * Run a single maintenance queue message through the handler and return
 * whether it was acked or retried. Uses the real test `env` by default;
 * pass `overrideEnv` for tests that stub bindings.
 */
export async function runQueueMessage(
  body: RepoMaintenanceQueueMessage,
  overrideEnv?: Env
): Promise<QueueRunResult> {
  let acked = false;
  let retried = false;
  await handleRepoMaintenanceQueue(
    {
      queue: "git-on-cloudflare-repo-maint",
      messages: [
        {
          id: "queue-1",
          timestamp: new Date(),
          body,
          attempts: 1,
          retry() {
            retried = true;
          },
          ack() {
            acked = true;
          },
        },
      ],
      retryAll() {},
      ackAll() {},
    },
    overrideEnv ?? testEnv,
    createExecutionContext()
  );
  return { acked, retried };
}
