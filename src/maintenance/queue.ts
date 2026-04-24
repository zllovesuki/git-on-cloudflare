import {
  type CompactionDeleteQueueMessage,
  type CompactionQueueMessage,
  handleCompactionDeleteMessage,
  handleCompactionMessage,
} from "./compaction.ts";
import { handlePackRefBackfillMessage, type PackRefBackfillQueueMessage } from "./refBackfill.ts";

export type RepoMaintenanceQueueMessage =
  | CompactionDeleteQueueMessage
  | CompactionQueueMessage
  | PackRefBackfillQueueMessage;

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

function isPackRefBackfillMessage(value: unknown): value is PackRefBackfillQueueMessage {
  if (!value || typeof value !== "object") return false;

  const body = value as Record<string, unknown>;
  return (
    body.kind === "pack-ref-backfill" &&
    typeof body.doId === "string" &&
    (body.repoId === undefined || typeof body.repoId === "string") &&
    typeof body.packKey === "string"
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

    if (isPackRefBackfillMessage(body)) {
      await handlePackRefBackfillMessage(message, body, env, ctx);
      continue;
    }

    // Unknown message type — ack and discard
    message.ack();
  }
}
