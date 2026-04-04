import type { RepoDurableObject } from "@/index";

import { describe, expect, it, vi } from "vitest";
import { env, SELF } from "cloudflare:test";

import { getRepoStub } from "@/common/index.ts";
import { buildFetchBody } from "./util/fetch-protocol.ts";
import { deleteLooseObjectCopies, uniqueRepoId, runDOWithRetry } from "./util/test-helpers.ts";
import { seedPackFirstRepo } from "./util/pack-first.ts";
import { decodeReportStatus, promoteToStreaming } from "./util/streaming-helpers.ts";
import { asTypedStorage, type RepoStateSchema } from "@/do/repo/repoState.ts";
import {
  compactOnce,
  deleteSupersededOnce,
  collectPackObjects,
  pushOverflowingStreamingHistory,
} from "./util/compaction-helpers.ts";

type DebugState = {
  activePacks?: Array<{ key: string; tier: number; kind: string }>;
  supersededPacks?: Array<{ key: string; tier: number; kind: string }>;
  compaction?: { queued?: boolean };
};

async function getDebugState(owner: string, repo: string): Promise<DebugState> {
  const response = await SELF.fetch(`https://example.com/${owner}/${repo}/admin/debug-state`);
  expect(response.status).toBe(200);
  return (await response.json()) as DebugState;
}

describe("streaming compaction", () => {
  it("previews and requests real compaction work only after streaming overflow exists", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stream-compaction-admin");
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    await promoteToStreaming(owner, repo);

    const sendSpy = vi.spyOn(env.REPO_MAINT_QUEUE, "send").mockImplementation(async () => {});
    try {
      await pushOverflowingStreamingHistory({
        owner,
        repo,
        repoId,
        startingCommitOid: seeded.nextCommit.oid,
        updates: 4,
      });
      sendSpy.mockClear();

      const previewResponse = await SELF.fetch(
        `https://example.com/${owner}/${repo}/admin/compact`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({}),
        }
      );
      expect(previewResponse.status).toBe(200);
      const previewJson = (await previewResponse.json()) as {
        action?: string;
        status?: string;
        plan?: {
          sourcePacks?: Array<{ packKey?: string }>;
          sourceTier?: number;
          targetTier?: number;
        };
      };
      expect(previewJson.action).toBe("preview");
      expect(previewJson.status).toBe("ok");
      expect(previewJson.plan?.sourcePacks?.length).toBe(4);
      expect(previewJson.plan?.sourceTier).toBe(0);
      expect(previewJson.plan?.targetTier).toBe(1);

      const requestResponse = await SELF.fetch(
        `https://example.com/${owner}/${repo}/admin/compact`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ dryRun: false }),
        }
      );
      expect(requestResponse.status).toBe(202);
      const requestJson = (await requestResponse.json()) as {
        action?: string;
        status?: string;
        shouldEnqueue?: boolean;
      };
      expect(requestJson.action).toBe("request");
      expect(requestJson.status).toBe("queued");
      expect(requestJson.shouldEnqueue).toBe(true);
      expect(sendSpy).toHaveBeenCalledWith({
        kind: "compaction",
        doId: env.REPO_DO.idFromName(repoId).toString(),
        repoId,
      });
    } finally {
      sendSpy.mockRestore();
    }
  });

  it("keeps the admin request queued when queue enqueue fails after DO state is recorded", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stream-compaction-admin-enqueue-failure");
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    await promoteToStreaming(owner, repo);

    const sendSpy = vi.spyOn(env.REPO_MAINT_QUEUE, "send").mockImplementation(async () => {});
    try {
      await pushOverflowingStreamingHistory({
        owner,
        repo,
        repoId,
        startingCommitOid: seeded.nextCommit.oid,
        updates: 4,
      });
      sendSpy.mockClear();
      sendSpy.mockRejectedValue(new Error("queue unavailable"));

      const requestResponse = await SELF.fetch(
        `https://example.com/${owner}/${repo}/admin/compact`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ dryRun: false }),
        }
      );
      expect(requestResponse.status).toBe(202);
      const requestJson = (await requestResponse.json()) as {
        action?: string;
        status?: string;
        shouldEnqueue?: boolean;
      };
      expect(requestJson.action).toBe("request");
      expect(requestJson.status).toBe("queued");
      expect(requestJson.shouldEnqueue).toBe(true);

      const stateAfterRequest = await getDebugState(owner, repo);
      expect(stateAfterRequest.compaction?.queued).toBe(true);
      expect(sendSpy).toHaveBeenCalledTimes(1);
    } finally {
      sendSpy.mockRestore();
    }
  });

  it("previews compaction plan even after clearing compactionWantedAt", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stream-compaction-preview-cleared");
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    await promoteToStreaming(owner, repo);

    const sendSpy = vi.spyOn(env.REPO_MAINT_QUEUE, "send").mockImplementation(async () => {});
    try {
      await pushOverflowingStreamingHistory({
        owner,
        repo,
        repoId,
        startingCommitOid: seeded.nextCommit.oid,
        updates: 4,
      });
      sendSpy.mockClear();

      // Request compaction so compactionWantedAt is set.
      const requestResponse = await SELF.fetch(
        `https://example.com/${owner}/${repo}/admin/compact`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ dryRun: false }),
        }
      );
      expect(requestResponse.status).toBe(202);

      // Clear the recorded request.
      const clearResponse = await SELF.fetch(`https://example.com/${owner}/${repo}/admin/compact`, {
        method: "DELETE",
      });
      expect(clearResponse.status).toBe(200);
      const clearJson = (await clearResponse.json()) as { cleared?: boolean };
      expect(clearJson.cleared).toBe(true);

      // Preview should still show the plan with queued: false.
      const previewResponse = await SELF.fetch(
        `https://example.com/${owner}/${repo}/admin/compact`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({}),
        }
      );
      expect(previewResponse.status).toBe(200);
      const previewJson = (await previewResponse.json()) as {
        action?: string;
        status?: string;
        queued?: boolean;
        plan?: { sourcePacks?: unknown[] };
      };
      expect(previewJson.action).toBe("preview");
      expect(previewJson.status).toBe("ok");
      expect(previewJson.queued).toBe(false);
      expect(previewJson.plan?.sourcePacks?.length).toBe(4);
    } finally {
      sendSpy.mockRestore();
    }
  });

  it("compacts superseded packs and keeps fetch and raw reads correct without loose objects", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stream-compaction-run");
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    const stub = getRepoStub(env, repoId) as DurableObjectStub<RepoDurableObject>;
    await promoteToStreaming(owner, repo);

    const pushed = await pushOverflowingStreamingHistory({
      owner,
      repo,
      repoId,
      startingCommitOid: seeded.nextCommit.oid,
      updates: 4,
    });

    await deleteLooseObjectCopies(env, seeded.getStub, [
      ...seeded.objectOids,
      ...pushed.objectOids,
    ]);

    const compacted = await compactOnce(repoId);
    expect(compacted.acked).toBe(true);
    expect(compacted.retried).toBe(false);

    const stateAfterCompaction = await getDebugState(owner, repo);
    expect(stateAfterCompaction.compaction?.queued).toBe(false);
    expect(stateAfterCompaction.activePacks?.some((pack) => pack.kind === "compact")).toBe(true);
    expect(stateAfterCompaction.supersededPacks?.length).toBe(4);

    const supersededPackKeys = (stateAfterCompaction.supersededPacks || []).map((pack) => pack.key);
    const beforeDelete = await collectPackObjects(supersededPackKeys);
    expect(beforeDelete.every((entry) => entry.exists && entry.idxExists)).toBe(true);

    const rawResponse = await SELF.fetch(
      `https://example.com/${owner}/${repo}/raw?oid=${pushed.objectOids.at(-3)}&name=README.md`
    );
    expect(rawResponse.status).toBe(200);
    expect(await rawResponse.text()).toBe("streaming update 3\n");

    const fetchResponse = await SELF.fetch(`https://example.com/${owner}/${repo}/git-upload-pack`, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-git-upload-pack-request",
        "Git-Protocol": "version=2",
      },
      body: buildFetchBody({
        wants: [pushed.currentCommitOid],
        haves: [seeded.nextCommit.oid],
        done: true,
      }),
    } as any);
    expect(fetchResponse.status).toBe(200);
    expect(
      decodeReportStatus(new Uint8Array(await fetchResponse.arrayBuffer())).length
    ).toBeGreaterThan(0);

    const deleted = await deleteSupersededOnce(repoId, supersededPackKeys);
    expect(deleted.acked).toBe(true);
    expect(deleted.retried).toBe(false);

    const afterDelete = await collectPackObjects(supersededPackKeys);
    expect(afterDelete.every((entry) => !entry.exists && !entry.idxExists)).toBe(true);

    // The superseded catalog rows remain visible for admin/debug until explicit cleanup.
    const finalState = await getDebugState(owner, repo);
    expect(finalState.supersededPacks?.length).toBe(4);

    void stub;
  });

  it("returns retry when a receive lease appears before compaction commit", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stream-compaction-receive-priority");
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    const getStub = () =>
      env.REPO_DO.get(env.REPO_DO.idFromName(repoId)) as DurableObjectStub<RepoDurableObject>;
    await promoteToStreaming(owner, repo);

    await pushOverflowingStreamingHistory({
      owner,
      repo,
      repoId,
      startingCommitOid: seeded.nextCommit.oid,
      updates: 4,
    });

    const stub = getStub();
    const begin = await stub.beginCompaction();
    expect(begin.ok).toBe(true);
    if (!begin.ok) {
      throw new Error("expected compaction to begin");
    }

    await runDOWithRetry(getStub, async (_instance, state) => {
      const store = asTypedStorage<RepoStateSchema>(state.storage);
      const now = Date.now();
      await store.put("receiveLease", {
        token: "receive-priority",
        createdAt: now,
        expiresAt: now + 60_000,
      });
    });

    const result = await stub.commitCompaction({
      token: begin.lease.token,
      sourcePacks: begin.sourcePacks,
      targetTier: begin.targetTier,
      packsetVersion: begin.packsetVersion,
      stagedPack: {
        packKey: `${begin.sourcePacks[0]!.packKey}.fake-compaction`,
        packBytes: begin.sourcePacks[0]!.packBytes,
        idxBytes: begin.sourcePacks[0]!.idxBytes,
        objectCount: begin.sourcePacks[0]!.objectCount,
      },
    });
    expect(result.status).toBe("retry");
    if (result.status === "retry") {
      expect(result.reason).toBe("receive-active");
    }

    const state = await getDebugState(owner, repo);
    expect(state.activePacks?.every((pack) => pack.kind !== "compact")).toBe(true);
  });

  it("returns retry when packsetVersion changes before compaction commit", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stream-compaction-packset-changed");
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    const getStub = () =>
      env.REPO_DO.get(env.REPO_DO.idFromName(repoId)) as DurableObjectStub<RepoDurableObject>;
    await promoteToStreaming(owner, repo);

    await pushOverflowingStreamingHistory({
      owner,
      repo,
      repoId,
      startingCommitOid: seeded.nextCommit.oid,
      updates: 4,
    });

    const stub = getStub();
    const begin = await stub.beginCompaction();
    expect(begin.ok).toBe(true);
    if (!begin.ok) {
      throw new Error("expected compaction to begin");
    }

    // Bump the packset version behind the compaction lease's back.
    await runDOWithRetry(getStub, async (_instance, state) => {
      const store = asTypedStorage<RepoStateSchema>(state.storage);
      const current = (await store.get("packsetVersion")) || 0;
      await store.put("packsetVersion", current + 1);
    });

    const result = await stub.commitCompaction({
      token: begin.lease.token,
      sourcePacks: begin.sourcePacks,
      targetTier: begin.targetTier,
      packsetVersion: begin.packsetVersion,
      stagedPack: {
        packKey: `${begin.sourcePacks[0]!.packKey}.fake-compaction`,
        packBytes: begin.sourcePacks[0]!.packBytes,
        idxBytes: begin.sourcePacks[0]!.idxBytes,
        objectCount: begin.sourcePacks[0]!.objectCount,
      },
    });
    expect(result.status).toBe("retry");
    if (result.status === "retry") {
      expect(result.reason).toBe("packset-changed");
    }

    const state = await getDebugState(owner, repo);
    expect(state.activePacks?.every((pack) => pack.kind !== "compact")).toBe(true);
  });

  it("keeps active pack counts bounded after repeated pushes and compaction drains", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stream-compaction-bounded");
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    await promoteToStreaming(owner, repo);

    await pushOverflowingStreamingHistory({
      owner,
      repo,
      repoId,
      startingCommitOid: seeded.nextCommit.oid,
      updates: 8,
    });

    for (let attempt = 0; attempt < 6; attempt++) {
      const queuedState = await getDebugState(owner, repo);
      if (!queuedState.compaction?.queued) break;
      const result = await compactOnce(repoId);
      expect(result.acked || result.retried).toBe(true);
    }

    const finalState = await getDebugState(owner, repo);
    const counts = new Map<number, number>();
    for (const pack of finalState.activePacks || []) {
      counts.set(pack.tier, (counts.get(pack.tier) || 0) + 1);
    }
    for (const count of counts.values()) {
      expect(count).toBeLessThanOrEqual(4);
    }
  });
});
