import { handleLegacyCompatBackfillMessage } from "./legacyCompatBackfill.ts";

export type LegacyCompatBackfillQueueMessage = {
  kind: "legacy-backfill";
  repoId: string;
  jobId: string;
  targetPacksetVersion: number;
};

export type RepoMaintenanceQueueMessage = LegacyCompatBackfillQueueMessage;

function isLegacyCompatBackfillMessage(value: unknown): value is LegacyCompatBackfillQueueMessage {
  if (!value || typeof value !== "object") return false;

  const body = value as Record<string, unknown>;
  return (
    body.kind === "legacy-backfill" &&
    typeof body.repoId === "string" &&
    typeof body.jobId === "string" &&
    typeof body.targetPacksetVersion === "number"
  );
}

export async function handleRepoMaintenanceQueue(
  batch: MessageBatch<RepoMaintenanceQueueMessage>,
  env: Env,
  ctx: ExecutionContext
): Promise<void> {
  for (const message of batch.messages) {
    if (!isLegacyCompatBackfillMessage(message.body)) {
      message.ack();
      continue;
    }

    await handleLegacyCompatBackfillMessage(message, env, ctx);
  }
}
