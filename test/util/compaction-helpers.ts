import { env } from "cloudflare:test";
import { packIndexKey } from "@/keys.ts";
import { pushStreamingUpdate } from "./streaming-helpers.ts";
import { runQueueMessage, type QueueRunResult } from "./queue.ts";

export async function compactOnce(repoId: string): Promise<QueueRunResult> {
  const doId = env.REPO_DO.idFromName(repoId).toString();
  return await runQueueMessage({
    kind: "compaction",
    doId,
    repoId,
  });
}

export async function deleteSupersededOnce(
  repoId: string,
  packKeys: string[]
): Promise<QueueRunResult> {
  const doId = env.REPO_DO.idFromName(repoId).toString();
  return await runQueueMessage({
    kind: "compaction-delete",
    doId,
    repoId,
    packKeys,
  });
}

export async function collectPackObjects(
  packKeys: string[]
): Promise<Array<{ packKey: string; exists: boolean; idxExists: boolean }>> {
  const checks: Array<{ packKey: string; exists: boolean; idxExists: boolean }> = [];
  for (const packKey of packKeys) {
    checks.push({
      packKey,
      exists: (await env.REPO_BUCKET.head(packKey)) !== null,
      idxExists: (await env.REPO_BUCKET.head(packIndexKey(packKey))) !== null,
    });
  }
  return checks;
}

export async function pushOverflowingStreamingHistory(args: {
  owner: string;
  repo: string;
  repoId: string;
  startingCommitOid: string;
  updates: number;
}): Promise<{ currentCommitOid: string; objectOids: string[] }> {
  let currentCommitOid = args.startingCommitOid;
  const objectOids: string[] = [];

  for (let index = 0; index < args.updates; index++) {
    const pushed = await pushStreamingUpdate(
      args.owner,
      args.repo,
      currentCommitOid,
      `streaming update ${index}\n`
    );
    currentCommitOid = pushed.commitOid;
    objectOids.push(pushed.blob.oid, pushed.tree.oid, pushed.commit.oid);
  }

  return { currentCommitOid, objectOids };
}
