import type { RepoDurableObject } from "@/index";
import type { CacheContext } from "@/cache";

import { describe, expect, it } from "vitest";
import { env, SELF } from "cloudflare:test";

import { getDb, upsertPackCatalogRow } from "@/do/repo/db/index.ts";
import { asTypedStorage, type RepoStateSchema } from "@/do/repo/repoState.ts";
import { readLooseObjectRaw } from "@/git/operations/read/objects.ts";
import { callStubWithRetry, runDOWithRetry, uniqueRepoId } from "./util/test-helpers.ts";
import { createTestCacheContext, seedPackFirstRepo } from "./util/pack-first.ts";

async function seedCompatCompactionOverflow(repoId: string): Promise<void> {
  const getStub = () =>
    env.REPO_DO.get(env.REPO_DO.idFromName(repoId)) as DurableObjectStub<RepoDurableObject>;

  await runDOWithRetry(getStub, async (_instance, state) => {
    const db = getDb(state.storage);
    const now = Date.now();

    for (let index = 0; index < 5; index++) {
      const seq = index + 2;
      await upsertPackCatalogRow(db, {
        packKey: `do/${state.id.toString()}/objects/pack/pack-compat-preview-${seq}.pack`,
        kind: "legacy",
        state: "active",
        tier: 0,
        seqLo: seq,
        seqHi: seq,
        objectCount: 1,
        packBytes: 1,
        idxBytes: 1,
        createdAt: now + index,
        supersededBy: null,
      });
    }
  });
}

describe("pack-first read path storage mode", () => {
  it("treats compact and hydrate admin routes as preview and queue aliases", async () => {
    const owner = "o";
    const repo = uniqueRepoId("compaction-admin");
    const repoId = `${owner}/${repo}`;
    await seedPackFirstRepo(repoId);
    await seedCompatCompactionOverflow(repoId);

    const previewResponse = await SELF.fetch(`https://example.com/${owner}/${repo}/admin/hydrate`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });
    expect(previewResponse.status).toBe(200);
    const previewJson = (await previewResponse.json()) as {
      action?: string;
      status?: string;
      plan?: { sourcePacks?: unknown[] };
      message?: string;
    };
    expect(previewJson.action).toBe("preview");
    expect(previewJson.status).toBe("ok");
    expect(previewJson.plan?.sourcePacks?.length).toBe(4);
    expect(previewJson.message).toContain("switches to streaming mode");

    const previewStateResponse = await SELF.fetch(
      `https://example.com/${owner}/${repo}/admin/debug-state`
    );
    expect(previewStateResponse.status).toBe(200);
    const previewState = (await previewStateResponse.json()) as {
      compaction?: { queued?: boolean };
    };
    expect(previewState.compaction?.queued).toBe(false);

    const queueResponse = await SELF.fetch(`https://example.com/${owner}/${repo}/admin/compact`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ dryRun: false }),
    });
    expect(queueResponse.status).toBe(200);
    const queueJson = (await queueResponse.json()) as {
      status?: string;
      shouldEnqueue?: boolean;
      message?: string;
    };
    expect(queueJson.status).toBe("ineligible");
    expect(queueJson.shouldEnqueue).toBe(false);
    expect(queueJson.message).toContain("only be requested");

    const queuedStateResponse = await SELF.fetch(
      `https://example.com/${owner}/${repo}/admin/debug-state`
    );
    expect(queuedStateResponse.status).toBe(200);
    const queuedState = (await queuedStateResponse.json()) as {
      compaction?: { queued?: boolean; wantedAt?: number };
    };
    expect(queuedState.compaction?.queued).toBe(false);

    const getStub = () =>
      env.REPO_DO.get(env.REPO_DO.idFromName(repoId)) as DurableObjectStub<RepoDurableObject>;
    await runDOWithRetry(getStub, async (_instance, state) => {
      const store = asTypedStorage<RepoStateSchema>(state.storage);
      await store.put("compactionWantedAt", Date.now());
    });

    const clearResponse = await SELF.fetch(`https://example.com/${owner}/${repo}/admin/hydrate`, {
      method: "DELETE",
    });
    expect(clearResponse.status).toBe(200);
    const clearJson = (await clearResponse.json()) as {
      ok?: boolean;
      action?: string;
      cleared?: boolean;
      message?: string;
    };
    expect(clearJson.ok).toBe(true);
    expect(clearJson.action).toBe("cleared");
    expect(clearJson.cleared).toBe(true);
    expect(clearJson.message).toContain("recorded compaction request");

    const clearedStateResponse = await SELF.fetch(
      `https://example.com/${owner}/${repo}/admin/debug-state`
    );
    expect(clearedStateResponse.status).toBe(200);
    const clearedState = (await clearedStateResponse.json()) as {
      compaction?: { queued?: boolean };
    };
    expect(clearedState.compaction?.queued).toBe(false);
  });

  it("reads and updates storage mode for packed repos (legacy ↔ streaming)", async () => {
    const owner = "o";
    const repo = uniqueRepoId("storage-mode-packed");
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);

    // Initially legacy (seedPackFirstRepo sets refs and packs, so cold-repo default is legacy)
    const getResponse = await SELF.fetch(`https://example.com/${owner}/${repo}/admin/storage-mode`);
    expect(getResponse.status).toBe(200);
    const getJson = (await getResponse.json()) as {
      status?: string;
      currentMode?: string;
      activePackCount?: number;
      canChange?: boolean;
    };
    expect(getJson.status).toBe("ok");
    expect(getJson.currentMode).toBe("legacy");
    expect(getJson.activePackCount).toBe(1);
    expect(getJson.canChange).toBe(true);

    // Direct transition: legacy → streaming (no shadow-read intermediate)
    const setResponse = await SELF.fetch(
      `https://example.com/${owner}/${repo}/admin/storage-mode`,
      {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ mode: "streaming" }),
      }
    );
    expect(setResponse.status).toBe(200);
    const setJson = (await setResponse.json()) as {
      status?: string;
      changed?: boolean;
      currentMode?: string;
      message?: string;
    };
    expect(setJson.status).toBe("ok");
    expect(setJson.changed).toBe(true);
    expect(setJson.currentMode).toBe("streaming");
    expect(setJson.message).toContain("now streaming");

    expect(await callStubWithRetry(seeded.getStub, (stub) => stub.getRepoStorageMode())).toBe(
      "streaming"
    );
  });

  it("pack-first reads work in both legacy and streaming modes", async () => {
    const owner = "o";
    const repo = uniqueRepoId("storage-mode-reads-stable");
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);

    // Read in legacy mode
    const legacyCacheCtx: CacheContext = createTestCacheContext(
      `https://example.com/${owner}/${repo}/raw`
    );
    const legacyRead = await readLooseObjectRaw(env, repoId, seeded.nextCommit.oid, legacyCacheCtx);
    expect(legacyRead?.type).toBe("commit");

    const legacyRawResponse = await SELF.fetch(
      `https://example.com/${owner}/${repo}/raw?oid=${seeded.nextBlob.oid}&name=README.md`
    );
    expect(legacyRawResponse.status).toBe(200);
    const legacyRawText = await legacyRawResponse.text();

    // Switch to streaming
    const switchRes = await SELF.fetch(`https://example.com/${owner}/${repo}/admin/storage-mode`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mode: "streaming" }),
    });
    expect(switchRes.status).toBe(200);

    // Read in streaming mode — same results
    const streamingCacheCtx: CacheContext = createTestCacheContext(
      `https://example.com/${owner}/${repo}/raw`
    );
    const streamingRead = await readLooseObjectRaw(
      env,
      repoId,
      seeded.nextCommit.oid,
      streamingCacheCtx
    );
    expect(streamingRead?.type).toBe(legacyRead?.type);
    expect(streamingRead?.payload).toEqual(legacyRead?.payload);

    const streamingRawResponse = await SELF.fetch(
      `https://example.com/${owner}/${repo}/raw?oid=${seeded.nextBlob.oid}&name=README.md`
    );
    expect(streamingRawResponse.status).toBe(200);
    expect(await streamingRawResponse.text()).toBe(legacyRawText);
  });

  it("blocks promoting non-empty zero-pack repo to streaming", async () => {
    const owner = "o";
    const repo = uniqueRepoId("storage-mode-no-pack");
    const repoId = `${owner}/${repo}`;
    const getStub = () =>
      env.REPO_DO.get(env.REPO_DO.idFromName(repoId)) as DurableObjectStub<RepoDurableObject>;

    // Seed a repo with refs but no packs (the unsupported loose-only case)
    await runDOWithRetry(getStub, async (instance) => {
      await instance.seedMinimalRepo(false);
    });

    const getResponse = await SELF.fetch(`https://example.com/${owner}/${repo}/admin/storage-mode`);
    expect(getResponse.status).toBe(200);
    const getJson = (await getResponse.json()) as {
      status?: string;
      currentMode?: string;
      activePackCount?: number;
      canChange?: boolean;
      blockers?: string[];
    };
    expect(getJson.status).toBe("ok");
    expect(getJson.currentMode).toBe("legacy");
    expect(getJson.activePackCount).toBe(0);
    expect(getJson.canChange).toBe(false);
    expect(getJson.blockers?.[0]).toContain("At least one active pack");

    const setResponse = await SELF.fetch(
      `https://example.com/${owner}/${repo}/admin/storage-mode`,
      {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ mode: "streaming" }),
      }
    );
    expect(setResponse.status).toBe(409);
    const setJson = (await setResponse.json()) as {
      status?: string;
      message?: string;
    };
    expect(setJson.status).toBe("no_active_packs");
    expect(setJson.message).toContain("At least one active pack");

    const adminResponse = await SELF.fetch(`https://example.com/${owner}/${repo}/admin`);
    expect(adminResponse.status).toBe(200);
    const adminHtml = await adminResponse.text();
    expect(adminHtml).toContain("Storage Mode");
    expect(adminHtml).toContain("At least one active pack");
  });

  it("blocks storage mode changes while a receive lease is active", async () => {
    const owner = "o";
    const repo = uniqueRepoId("storage-mode-receive-busy");
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);

    await runDOWithRetry(seeded.getStub, async (_instance, state) => {
      const store = asTypedStorage<RepoStateSchema>(state.storage);
      const now = Date.now();
      await store.put("receiveLease", {
        token: "busy-receive",
        createdAt: now,
        expiresAt: now + 60_000,
      });
    });

    const response = await SELF.fetch(`https://example.com/${owner}/${repo}/admin/storage-mode`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mode: "streaming" }),
    });
    expect(response.status).toBe(409);
    const payload = (await response.json()) as { status?: string; message?: string };
    expect(payload.status).toBe("repo_busy");
    expect(payload.message).toContain("cannot change");
  });

  it("blocks storage mode changes while a compaction lease is active", async () => {
    const owner = "o";
    const repo = uniqueRepoId("storage-mode-compaction-busy");
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);

    await runDOWithRetry(seeded.getStub, async (_instance, state) => {
      const store = asTypedStorage<RepoStateSchema>(state.storage);
      const now = Date.now();
      await store.put("compactLease", {
        token: "busy-compact",
        createdAt: now,
        expiresAt: now + 60_000,
      });
    });

    const response = await SELF.fetch(`https://example.com/${owner}/${repo}/admin/storage-mode`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mode: "streaming" }),
    });
    expect(response.status).toBe(409);
    const payload = (await response.json()) as { status?: string; message?: string };
    expect(payload.status).toBe("repo_busy");
    expect(payload.message).toContain("cannot change");
  });

  it("rejects invalid mode names", async () => {
    const owner = "o";
    const repo = uniqueRepoId("storage-mode-invalid");
    const repoId = `${owner}/${repo}`;
    await seedPackFirstRepo(repoId);

    // shadow-read is no longer a valid mode
    const response = await SELF.fetch(`https://example.com/${owner}/${repo}/admin/storage-mode`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mode: "shadow-read" }),
    });
    expect(response.status).toBe(400);
    const payload = (await response.json()) as {
      status?: string;
      message?: string;
    };
    expect(payload.status).toBe("unsupported_target_mode");
    expect(payload.message).toContain("Only legacy and streaming");
  });
});
