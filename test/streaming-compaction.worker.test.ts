import type { RepoDurableObject } from "@/index";

import { describe, expect, it, vi } from "vitest";
import { env, SELF } from "cloudflare:test";

import { getRepoStub } from "@/common/index.ts";
import { bytesToHex } from "@/common/hex.ts";
import { encodeGitObject } from "@/git/core/objects.ts";
import { concatChunks, decodePktLines } from "@/git";
import { buildFetchBody } from "./util/fetch-protocol.ts";
import {
  deleteLooseObjectCopies,
  uniqueRepoId,
  runDOWithRetry,
  seedPackedRepoState,
  buildTreePayload,
  buildPack,
  buildAppendOnlyDelta,
} from "./util/test-helpers.ts";
import { seedPackFirstRepo } from "./util/pack-first.ts";
import { indexTestPack } from "./util/test-indexer.ts";
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

  it("compacts successfully when a non-source pack contains a duplicate identity REF_DELTA", async () => {
    // Regression test for the compaction self-referential delta bug.
    //
    // When the active catalog snapshot is newest-first and a newer non-source
    // pack contains a REF_DELTA whose resolved OID equals its baseOid (an
    // identity delta), resolveOrderedEntryByOid picks that entry for a needed
    // OID and the base chase loops back to the same entry, creating a cycle
    // that the topology sort cannot order.
    //
    // The fix reorders the compaction snapshot so source packs are searched
    // first, ensuring the authoritative full-object entry is selected.

    const owner = "o";
    const repo = uniqueRepoId("stream-compaction-self-ref");
    const repoId = `${owner}/${repo}`;
    const id = env.REPO_DO.idFromName(repoId);
    const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;

    const author = "You <you@example.com> 0 +0000";

    // Shared blob that will appear in both a source pack and the newer
    // non-source pack as an identity REF_DELTA.
    const sharedBlobPayload = new TextEncoder().encode("shared content\n");
    const sharedBlob = await encodeGitObject("blob", sharedBlobPayload);

    // Build 4 source packs (oldest) with distinct commits, one containing the
    // shared blob. Each pack needs at least one unique object so the OID sets
    // differ and compaction has real work to do.
    const sourcePacks: Array<{ name: string; packBytes: Uint8Array }> = [];
    let parentOid: string | undefined;
    const allObjectOids: string[] = [];

    for (let i = 0; i < 4; i++) {
      const blobPayload = new TextEncoder().encode(`source content ${i}\n`);
      const blob = await encodeGitObject("blob", blobPayload);

      // Include the shared blob in the first source pack so it is a needed OID.
      const treeEntries = [{ mode: "100644" as const, name: `file-${i}.txt`, oid: blob.oid }];
      if (i === 0) {
        treeEntries.push({ mode: "100644" as const, name: "shared.txt", oid: sharedBlob.oid });
      }
      const treePayload = buildTreePayload(treeEntries);
      const tree = await encodeGitObject("tree", treePayload);

      const commitText =
        `tree ${tree.oid}\n` +
        (parentOid ? `parent ${parentOid}\n` : "") +
        `author ${author}\ncommitter ${author}\n\nsource ${i}\n`;
      const commit = await encodeGitObject("commit", new TextEncoder().encode(commitText));
      parentOid = commit.oid;

      const objects: Array<{ type: "blob" | "tree" | "commit"; payload: Uint8Array }> = [
        { type: "blob", payload: blobPayload },
        { type: "tree", payload: treePayload },
        { type: "commit", payload: new TextEncoder().encode(commitText) },
      ];
      if (i === 0) {
        objects.unshift({ type: "blob", payload: sharedBlobPayload });
      }

      sourcePacks.push({ name: `pack-source-${i}.pack`, packBytes: await buildPack(objects) });
      allObjectOids.push(blob.oid, tree.oid, commit.oid);
      if (i === 0) allObjectOids.push(sharedBlob.oid);
    }

    // Build the newest pack with an identity REF_DELTA for the shared blob.
    // The delta copies the entire base content, so the resolved OID equals
    // the base OID — exactly the scenario that triggers the self-loop.
    const identityDelta = buildAppendOnlyDelta(sharedBlobPayload, new Uint8Array(0));
    const newestPackBytes = await buildPack([
      { type: "ref-delta", baseOid: sharedBlob.oid, delta: identityDelta },
    ]);
    const newestPack = { name: "pack-newest-dup.pack", packBytes: newestPackBytes };

    // seedPackedRepoState expects packs newest-first; it indexes oldest-first
    // internally so the REF_DELTA base in the source packs is available.
    const lastCommitOid = parentOid!;
    await seedPackedRepoState({
      env,
      repoId,
      getStub,
      packs: [newestPack, ...sourcePacks],
      refs: [{ name: "refs/heads/main", oid: lastCommitOid }],
      head: { target: "refs/heads/main", oid: lastCommitOid },
    });

    // Verify compaction plan selects the 4 source packs (oldest tier-0).
    const preState = await getDebugState(owner, repo);
    expect(preState.activePacks?.length).toBe(5);
    expect(preState.activePacks?.filter((p) => p.tier === 0).length).toBe(5);

    // Request and run compaction.
    const stub = getStub();
    const request = await stub.requestCompaction();
    expect(request.status).toBe("queued");

    const result = await compactOnce(repoId);
    expect(result.acked).toBe(true);
    expect(result.retried).toBe(false);

    // Verify post-compaction state: one compacted pack, 4 source packs superseded.
    const postState = await getDebugState(owner, repo);
    expect(postState.activePacks?.some((p) => p.kind === "compact")).toBe(true);
    expect(postState.supersededPacks?.length).toBe(4);
    expect(postState.compaction?.queued).toBe(false);
  });

  it("fetch after compaction does not produce duplicate REF_DELTA entries", async () => {
    // Regression test for broken `git pull` after compaction.
    //
    // After compaction, the active snapshot is newest-first:
    //   [compacted pack (newest seqHi), non-source pack (older)]
    // If the non-source pack has an OFS_DELTA whose in-pack base chain
    // reaches an entry with the same OID as a full object in the compacted
    // pack, the OFS_DELTA base chase adds that entry by pack-local offset —
    // bypassing OID-level dedup. The output pack then has two entries for
    // the same OID, causing git's index-pack to reject the pack with
    // "REF_DELTA at offset X already resolved (duplicate base Y)".
    //
    // The fix canonicalizes OFS_DELTA bases via resolveOrderedEntryByOid so
    // the position-based dedup in addEntry catches cross-pack duplicates.

    const owner = "o";
    const repo = uniqueRepoId("stream-compaction-fetch-dup");
    const repoId = `${owner}/${repo}`;
    const id = env.REPO_DO.idFromName(repoId);
    const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;

    const author = "You <you@example.com> 0 +0000";

    // Shared blob: appears as a full object in a source pack, and as an
    // identity REF_DELTA in the non-source pack. After compaction, both
    // the compacted pack and the non-source pack contain this OID.
    const sharedBlobPayload = new TextEncoder().encode("shared content for fetch test\n");
    const sharedBlob = await encodeGitObject("blob", sharedBlobPayload);

    // Child blob: stored as OFS_DELTA based on the shared blob in the
    // non-source pack. Its base chase is the path that triggers the
    // cross-pack duplicate. Must be referenced by a tree to be needed.
    const childBlobFullPayload = new Uint8Array([
      ...sharedBlobPayload,
      ...new TextEncoder().encode("extra\n"),
    ]);
    const childBlob = await encodeGitObject("blob", childBlobFullPayload);

    // Build 4 source packs with distinct commits. Source pack 0 includes
    // the shared blob as a full object.
    const sourcePacks: Array<{ name: string; packBytes: Uint8Array }> = [];
    let parentOid: string | undefined;

    for (let i = 0; i < 4; i++) {
      const blobPayload = new TextEncoder().encode(`source content fetch ${i}\n`);
      const blob = await encodeGitObject("blob", blobPayload);

      const treeEntries = [{ mode: "100644" as const, name: `file-${i}.txt`, oid: blob.oid }];
      if (i === 0) {
        treeEntries.push({ mode: "100644" as const, name: "shared.txt", oid: sharedBlob.oid });
      }
      const treePayload = buildTreePayload(treeEntries);
      const tree = await encodeGitObject("tree", treePayload);

      const commitText =
        `tree ${tree.oid}\n` +
        (parentOid ? `parent ${parentOid}\n` : "") +
        `author ${author}\ncommitter ${author}\n\nsource fetch ${i}\n`;
      const commit = await encodeGitObject("commit", new TextEncoder().encode(commitText));
      parentOid = commit.oid;

      const objects: Array<{ type: "blob" | "tree" | "commit"; payload: Uint8Array }> = [
        { type: "blob", payload: blobPayload },
        { type: "tree", payload: treePayload },
        { type: "commit", payload: new TextEncoder().encode(commitText) },
      ];
      if (i === 0) {
        objects.unshift({ type: "blob", payload: sharedBlobPayload });
      }

      sourcePacks.push({ name: `pack-src-${i}.pack`, packBytes: await buildPack(objects) });
    }

    // Build a newest pack that contributes a real commit to the graph.
    // Layout (by entry index):
    //   0: identity REF_DELTA for sharedBlob (resolves to sharedBlob.oid)
    //   1: OFS_DELTA child blob based on entry 0 (resolves to childBlob.oid)
    //   2: tree referencing the child blob
    //   3: commit (child of last source commit) referencing tree 2
    //
    // During fetch, the child blob (entry 1) is needed because the tree
    // references it. Its OFS_DELTA base chase reaches entry 0 (shared blob
    // OID), which was already selected from the compacted pack — the
    // cross-pack duplicate that this test guards against.
    const identityDelta = buildAppendOnlyDelta(sharedBlobPayload, new Uint8Array(0));
    const childDelta = buildAppendOnlyDelta(sharedBlobPayload, new TextEncoder().encode("extra\n"));

    const newestTreePayload = buildTreePayload([
      { mode: "100644" as const, name: "child.txt", oid: childBlob.oid },
      { mode: "100644" as const, name: "shared.txt", oid: sharedBlob.oid },
    ]);
    const newestTree = await encodeGitObject("tree", newestTreePayload);

    const newestCommitText =
      `tree ${newestTree.oid}\n` +
      `parent ${parentOid}\n` +
      `author ${author}\ncommitter ${author}\n\nnewest with child blob\n`;
    const newestCommit = await encodeGitObject(
      "commit",
      new TextEncoder().encode(newestCommitText)
    );

    const newestPackBytes = await buildPack([
      { type: "ref-delta", baseOid: sharedBlob.oid, delta: identityDelta },
      { type: "ofs-delta", baseIndex: 0, delta: childDelta },
      { type: "tree", payload: newestTreePayload },
      { type: "commit", payload: new TextEncoder().encode(newestCommitText) },
    ]);
    const newestPack = { name: "pack-newest-fetch-dup.pack", packBytes: newestPackBytes };

    // HEAD = newest commit so the fetch graph reaches objects in every pack.
    await seedPackedRepoState({
      env,
      repoId,
      getStub,
      packs: [newestPack, ...sourcePacks],
      refs: [{ name: "refs/heads/main", oid: newestCommit.oid }],
      head: { target: "refs/heads/main", oid: newestCommit.oid },
    });

    // Run compaction — merges the 4 source packs into one compacted pack.
    // The non-source pack (newest) stays active.
    const stub = getStub();
    await stub.requestCompaction();
    const compactResult = await compactOnce(repoId);
    expect(compactResult.acked).toBe(true);
    expect(compactResult.retried).toBe(false);

    const postState = await getDebugState(owner, repo);
    expect(postState.activePacks?.some((p) => p.kind === "compact")).toBe(true);

    // Fetch all objects (clone scenario) — this is the path that was broken.
    const fetchResponse = await SELF.fetch(`https://example.com/${owner}/${repo}/git-upload-pack`, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-git-upload-pack-request",
        "Git-Protocol": "version=2",
      },
      body: buildFetchBody({ wants: [newestCommit.oid], done: true }),
    } as any);
    expect(fetchResponse.status).toBe(200);

    // Extract sideband-encoded pack bytes from the response.
    const bytes = new Uint8Array(await fetchResponse.arrayBuffer());
    const lines = decodePktLines(bytes);
    const packChunks: Uint8Array[] = [];
    let inPackfile = false;
    for (const line of lines) {
      if (line.type === "line" && line.text === "packfile\n") {
        inPackfile = true;
        continue;
      }
      if (inPackfile && line.type === "line" && line.raw && line.raw[0] === 0x01) {
        packChunks.push(line.raw.subarray(1));
      }
    }
    const packOut = concatChunks(packChunks);

    // Basic pack header sanity check.
    expect(new TextDecoder().decode(packOut.subarray(0, 4))).toBe("PACK");

    // Index the returned pack and verify no duplicate OIDs.
    const verifyKey = `verify/compaction-fetch-dup-${Date.now()}.pack`;
    await env.REPO_BUCKET.put(verifyKey, packOut);
    const verifyResult = await indexTestPack(env, verifyKey, packOut.byteLength);

    const oidSet = new Set<string>();
    for (let i = 0; i < verifyResult.idxView.count; i++) {
      const oidBytes = verifyResult.idxView.rawNames.subarray(i * 20, (i + 1) * 20);
      oidSet.add(bytesToHex(oidBytes));
    }
    // Intentionally stricter than git index-pack (which tolerates duplicate
    // full objects and OFS identity deltas, but rejects duplicate REF_DELTAs).
    // Our rewrite should never produce ANY duplicate OIDs in the output pack.
    expect(oidSet.size).toBe(verifyResult.idxView.count);

    // Also verify after superseded pack deletion — compacted pack is sole
    // source for the merged objects, no duplicates possible.
    const supersededKeys =
      postState.supersededPacks?.map((p) => p.key).filter((k): k is string => !!k) ?? [];
    if (supersededKeys.length > 0) {
      await deleteSupersededOnce(repoId, supersededKeys);
    }

    const fetchResponse2 = await SELF.fetch(
      `https://example.com/${owner}/${repo}/git-upload-pack`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/x-git-upload-pack-request",
          "Git-Protocol": "version=2",
        },
        body: buildFetchBody({ wants: [newestCommit.oid], done: true }),
      } as any
    );
    expect(fetchResponse2.status).toBe(200);

    const bytes2 = new Uint8Array(await fetchResponse2.arrayBuffer());
    const lines2 = decodePktLines(bytes2);
    const packChunks2: Uint8Array[] = [];
    let inPackfile2 = false;
    for (const line of lines2) {
      if (line.type === "line" && line.text === "packfile\n") {
        inPackfile2 = true;
        continue;
      }
      if (inPackfile2 && line.type === "line" && line.raw && line.raw[0] === 0x01) {
        packChunks2.push(line.raw.subarray(1));
      }
    }
    const packOut2 = concatChunks(packChunks2);
    expect(new TextDecoder().decode(packOut2.subarray(0, 4))).toBe("PACK");

    const verifyKey2 = `verify/compaction-fetch-post-delete-${Date.now()}.pack`;
    await env.REPO_BUCKET.put(verifyKey2, packOut2);
    const verifyResult2 = await indexTestPack(env, verifyKey2, packOut2.byteLength);

    const oidSet2 = new Set<string>();
    for (let i = 0; i < verifyResult2.idxView.count; i++) {
      const oidBytes = verifyResult2.idxView.rawNames.subarray(i * 20, (i + 1) * 20);
      oidSet2.add(bytesToHex(oidBytes));
    }
    expect(oidSet2.size).toBe(verifyResult2.idxView.count);
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
