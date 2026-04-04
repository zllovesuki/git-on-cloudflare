import {
  type CompactionDeleteQueueMessage,
  type CompactionQueueMessage,
  handleCompactionDeleteMessage,
  handleCompactionMessage,
} from "./compaction.ts";
import {
  handleLegacyCompatBackfillMessage,
  type LegacyCompatBackfillQueueMessage,
} from "./legacyCompatBackfill.ts";

export type RepoMaintenanceQueueMessage =
  | CompactionDeleteQueueMessage
  | CompactionQueueMessage
  | LegacyCompatBackfillQueueMessage;

function isCompactionMessage(value: unknown): value is CompactionQueueMessage {
  if (!value || typeof value !== "object") return false;

  const body = value as Record<string, unknown>;
  return (
    body.kind === "compaction" &&
    typeof body.doId === "string" &&
    (body.repoId === undefined || typeof body.repoId === "string")
  );
}

function isCompactionDeleteMessage(value: unknown): value is CompactionDeleteQueueMessage {
  if (!value || typeof value !== "object") return false;

  const body = value as Record<string, unknown>;
  return (
    body.kind === "compaction-delete" &&
    typeof body.doId === "string" &&
    (body.repoId === undefined || typeof body.repoId === "string") &&
    Array.isArray(body.packKeys) &&
    body.packKeys.every((packKey) => typeof packKey === "string")
  );
}

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
    const body = message.body;

    if (isCompactionMessage(body)) {
      await handleCompactionMessage(message, body, env, ctx);
      continue;
    }

    if (isCompactionDeleteMessage(body)) {
      await handleCompactionDeleteMessage(message, body, env, ctx);
      continue;
    }

    if (!isLegacyCompatBackfillMessage(body)) {
      message.ack();
      continue;
    }

    await handleLegacyCompatBackfillMessage(message, body, env, ctx);
  }
}
