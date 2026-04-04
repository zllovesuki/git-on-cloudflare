import { describe, expect, it, vi } from "vitest";
import { createExecutionContext, env, SELF } from "cloudflare:test";

import { handleRepoMaintenanceQueue } from "@/maintenance/queue.ts";
import { deleteLooseObjectCopies, uniqueRepoId } from "./util/test-helpers.ts";
import { seedPackFirstRepo } from "./util/pack-first.ts";
import { getRepoStub } from "@/common/index.ts";
import { promoteToStreaming, pushStreamingUpdate } from "./util/streaming-helpers.ts";
import { runQueueMessage } from "./util/queue.ts";

type BackfillQueuePayload = {
  status?: string;
  jobId?: string;
  targetPacksetVersion?: number;
  shouldEnqueue?: boolean;
};

async function queueLegacyCompatBackfill(repoId: string, payload: BackfillQueuePayload) {
  if (payload.status !== "queued" || !payload.jobId || !payload.targetPacksetVersion) {
    throw new Error("expected queued rollback backfill payload");
  }

  const result = await runQueueMessage({
    kind: "legacy-backfill",
    repoId,
    jobId: payload.jobId,
    targetPacksetVersion: payload.targetPacksetVersion,
  });
  expect(result.acked).toBe(true);
}

describe("streaming receive rollback preparation", () => {
  it("keeps repeated rollback-prep triggers idempotent for the current pack catalog", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stream-roll-idempotent");
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    await deleteLooseObjectCopies(env, seeded.getStub, seeded.objectOids);
    await promoteToStreaming(owner, repo);

    const firstResponse = await SELF.fetch(
      `https://example.com/${owner}/${repo}/admin/storage-mode/backfill`,
      { method: "POST" }
    );
    expect(firstResponse.status).toBe(202);
    const firstJson = (await firstResponse.json()) as BackfillQueuePayload;

    const secondResponse = await SELF.fetch(
      `https://example.com/${owner}/${repo}/admin/storage-mode/backfill`,
      { method: "POST" }
    );
    expect(secondResponse.status).toBe(202);
    const secondJson = (await secondResponse.json()) as BackfillQueuePayload;

    expect(secondJson.jobId).toBe(firstJson.jobId);
    expect(secondJson.targetPacksetVersion).toBe(firstJson.targetPacksetVersion);
  });

  it("re-enqueues an already-queued rollback backfill job on a later admin retry", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stream-roll-reenqueue");
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    await deleteLooseObjectCopies(env, seeded.getStub, seeded.objectOids);
    await promoteToStreaming(owner, repo);

    const stub = getRepoStub(env, repoId);
    const queued = await stub.requestLegacyCompatBackfill();
    if (queued.status !== "queued") {
      throw new Error("expected rollback backfill to be queued");
    }

    const sendSpy = vi.spyOn(env.REPO_MAINT_QUEUE, "send").mockImplementation(async () => {});
    try {
      const response = await SELF.fetch(
        `https://example.com/${owner}/${repo}/admin/storage-mode/backfill`,
        { method: "POST" }
      );
      expect(response.status).toBe(202);
      const payload = (await response.json()) as BackfillQueuePayload;
      expect(payload.status).toBe("queued");
      expect(payload.jobId).toBe(queued.jobId);
      expect(payload.targetPacksetVersion).toBe(queued.targetPacksetVersion);
      expect(payload.shouldEnqueue).toBe(true);
      expect(sendSpy).toHaveBeenCalledWith({
        kind: "legacy-backfill",
        repoId,
        jobId: queued.jobId,
        targetPacksetVersion: queued.targetPacksetVersion,
      });
    } finally {
      sendSpy.mockRestore();
    }
  });

  it("blocks leaving streaming mode until rollback data is prepared, then allows it", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stream-roll-backfill");
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    await deleteLooseObjectCopies(env, seeded.getStub, seeded.objectOids);
    await promoteToStreaming(owner, repo);

    const blockedResponse = await SELF.fetch(
      `https://example.com/${owner}/${repo}/admin/storage-mode`,
      {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ mode: "legacy" }),
      }
    );
    expect(blockedResponse.status).toBe(409);
    const blockedJson = (await blockedResponse.json()) as { status?: string };
    expect(blockedJson.status).toBe("rollback_backfill_required");

    const queueResponse = await SELF.fetch(
      `https://example.com/${owner}/${repo}/admin/storage-mode/backfill`,
      { method: "POST" }
    );
    expect(queueResponse.status).toBe(202);
    const queueJson = (await queueResponse.json()) as BackfillQueuePayload;

    const duplicateQueueResponse = await SELF.fetch(
      `https://example.com/${owner}/${repo}/admin/storage-mode/backfill`,
      { method: "POST" }
    );
    expect(duplicateQueueResponse.status).toBe(202);
    const duplicateQueueJson = (await duplicateQueueResponse.json()) as BackfillQueuePayload;
    expect(duplicateQueueJson.jobId).toBe(queueJson.jobId);

    await queueLegacyCompatBackfill(repoId, queueJson);

    const controlResponse = await SELF.fetch(
      `https://example.com/${owner}/${repo}/admin/storage-mode`
    );
    expect(controlResponse.status).toBe(200);
    const controlJson = (await controlResponse.json()) as {
      rollbackCompat?: { status?: string };
    };
    expect(controlJson.rollbackCompat?.status).toBe("ready");

    const oidResponse = await SELF.fetch(
      `https://example.com/${owner}/${repo}/admin/debug-oid/${seeded.nextBlob.oid}`
    );
    const oidJson = (await oidResponse.json()) as {
      presence?: { hasLoose?: boolean; hasR2Loose?: boolean };
    };
    expect(oidJson.presence?.hasLoose).toBe(true);
    expect(oidJson.presence?.hasR2Loose).toBe(true);

    const rollbackResponse = await SELF.fetch(
      `https://example.com/${owner}/${repo}/admin/storage-mode`,
      {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ mode: "shadow-read" }),
      }
    );
    expect(rollbackResponse.status).toBe(200);
    const rollbackJson = (await rollbackResponse.json()) as { currentMode?: string };
    expect(rollbackJson.currentMode).toBe("shadow-read");
  });

  it("marks rollback data stale after a later streaming push changes the pack catalog", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stream-roll-stale");
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    await deleteLooseObjectCopies(env, seeded.getStub, seeded.objectOids);
    await promoteToStreaming(owner, repo);

    const queueResponse = await SELF.fetch(
      `https://example.com/${owner}/${repo}/admin/storage-mode/backfill`,
      { method: "POST" }
    );
    expect(queueResponse.status).toBe(202);
    const queueJson = (await queueResponse.json()) as BackfillQueuePayload;
    await queueLegacyCompatBackfill(repoId, queueJson);

    await pushStreamingUpdate(owner, repo, seeded.nextCommit.oid, "after backfill\n");

    const controlResponse = await SELF.fetch(
      `https://example.com/${owner}/${repo}/admin/storage-mode`
    );
    expect(controlResponse.status).toBe(200);
    const controlJson = (await controlResponse.json()) as {
      rollbackCompat?: { status?: string };
    };
    expect(controlJson.rollbackCompat?.status).toBe("stale");

    const rollbackResponse = await SELF.fetch(
      `https://example.com/${owner}/${repo}/admin/storage-mode`,
      {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ mode: "legacy" }),
      }
    );
    expect(rollbackResponse.status).toBe(409);
    const rollbackJson = (await rollbackResponse.json()) as { status?: string };
    expect(rollbackJson.status).toBe("rollback_backfill_required");
  });

  it("acks stale queue messages at completion without marking the backfill as failed", async () => {
    let acked = false;
    const fakeStub = {
      beginLegacyCompatBackfill: vi.fn().mockResolvedValue({
        status: "ok",
        jobId: "job-1",
        targetPacksetVersion: 1,
        activeCatalog: [],
        progress: { packIndex: 0, objectIndex: 0 },
      }),
      storeLegacyCompatBatch: vi.fn(),
      completeLegacyCompatBackfill: vi.fn().mockResolvedValue({
        status: "stale",
        message: "Rollback compatibility request is stale for the current pack catalog.",
      }),
      failLegacyCompatBackfill: vi.fn(),
    };
    const fakeEnv = {
      LOG_LEVEL: "warn",
      REPO_DO: {
        idFromName() {
          return { toString: () => "fake-do-id" };
        },
        get() {
          return fakeStub;
        },
      },
      REPO_BUCKET: {},
      REPO_MAINT_QUEUE: {
        send: vi.fn(),
      },
    } as unknown as Env;

    await handleRepoMaintenanceQueue(
      {
        queue: "git-on-cloudflare-repo-maint",
        messages: [
          {
            id: "backfill-stale",
            timestamp: new Date(),
            body: {
              kind: "legacy-backfill",
              repoId: "o/repo",
              jobId: "job-1",
              targetPacksetVersion: 1,
            },
            attempts: 1,
            retry() {},
            ack() {
              acked = true;
            },
          },
        ],
        retryAll() {},
        ackAll() {},
      },
      fakeEnv,
      createExecutionContext()
    );

    expect(acked).toBe(true);
    expect(fakeStub.completeLegacyCompatBackfill).toHaveBeenCalledOnce();
    expect(fakeStub.failLegacyCompatBackfill).not.toHaveBeenCalled();
  });
});
