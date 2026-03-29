import type { RepoDurableObject } from "@/index";
import type { CacheContext } from "@/cache";

import { describe, expect, it } from "vitest";
import { env, SELF } from "cloudflare:test";

import { asTypedStorage, type RepoStateSchema } from "@/do/repo/repoState.ts";
import { readLooseObjectRaw } from "@/git/operations/read/objects.ts";
import { callStubWithRetry, runDOWithRetry, uniqueRepoId } from "./util/test-helpers.ts";
import { createTestCacheContext, seedPackFirstRepo } from "./util/pack-first.ts";

describe("pack-first read path storage mode", () => {
  it("treats compact and hydrate admin routes as preview and queue aliases", async () => {
    const owner = "o";
    const repo = uniqueRepoId("compaction-admin");
    const repoId = `${owner}/${repo}`;
    await seedPackFirstRepo(repoId);

    const previewResponse = await SELF.fetch(`https://example.com/${owner}/${repo}/admin/compact`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });
    expect(previewResponse.status).toBe(200);
    const previewJson = (await previewResponse.json()) as {
      action?: string;
      queued?: boolean;
      message?: string;
    };
    expect(previewJson.action).toBe("preview");
    expect(previewJson.queued).toBe(false);
    expect(previewJson.message).toContain("No new request was recorded");

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
    expect(queueResponse.status).toBe(202);
    const queueJson = (await queueResponse.json()) as {
      action?: string;
      queued?: boolean;
      wantedAt?: number;
      message?: string;
    };
    expect(queueJson.action).toBe("queued");
    expect(queueJson.queued).toBe(true);
    expect(typeof queueJson.wantedAt).toBe("number");
    expect(queueJson.message).toContain("Recorded");
    expect(queueJson.message).toContain("Background compaction");

    const queuedStateResponse = await SELF.fetch(
      `https://example.com/${owner}/${repo}/admin/debug-state`
    );
    expect(queuedStateResponse.status).toBe(200);
    const queuedState = (await queuedStateResponse.json()) as {
      compaction?: { queued?: boolean; wantedAt?: number };
    };
    expect(queuedState.compaction?.queued).toBe(true);
    expect(typeof queuedState.compaction?.wantedAt).toBe("number");

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

  it("reads and updates storage mode for packed repos", async () => {
    const owner = "o";
    const repo = uniqueRepoId("storage-mode-packed");
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);

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

    const setResponse = await SELF.fetch(
      `https://example.com/${owner}/${repo}/admin/storage-mode`,
      {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ mode: "shadow-read" }),
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
    expect(setJson.currentMode).toBe("shadow-read");
    expect(setJson.message).toContain("validation is now enabled");

    expect(await callStubWithRetry(seeded.getStub, (stub) => stub.getRepoStorageMode())).toBe(
      "shadow-read"
    );

    const resetResponse = await SELF.fetch(
      `https://example.com/${owner}/${repo}/admin/storage-mode`,
      {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ mode: "legacy" }),
      }
    );
    expect(resetResponse.status).toBe(200);
    const resetJson = (await resetResponse.json()) as {
      status?: string;
      currentMode?: string;
      message?: string;
    };
    expect(resetJson.status).toBe("ok");
    expect(resetJson.currentMode).toBe("legacy");
    expect(resetJson.message).toContain("validation is now disabled");
  });

  it("keeps pack-first reads stable while storage mode only toggles validation", async () => {
    const owner = "o";
    const repo = uniqueRepoId("storage-mode-validation-only");
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);

    const legacyCacheCtx: CacheContext = createTestCacheContext(
      `https://example.com/${owner}/${repo}/raw`
    );
    const legacyRead = await readLooseObjectRaw(env, repoId, seeded.nextCommit.oid, legacyCacheCtx);
    expect(legacyRead?.type).toBe("commit");
    expect(legacyCacheCtx.memo?.repoStorageMode).toBe("legacy");
    expect(legacyCacheCtx.memo?.loaderCalls ?? 0).toBe(0);

    const legacyRawResponse = await SELF.fetch(
      `https://example.com/${owner}/${repo}/raw?oid=${seeded.nextBlob.oid}&name=README.md`
    );
    expect(legacyRawResponse.status).toBe(200);
    const legacyRawText = await legacyRawResponse.text();

    await callStubWithRetry(seeded.getStub, (stub) => stub.setRepoStorageMode("shadow-read"));

    const shadowCacheCtx: CacheContext = createTestCacheContext(
      `https://example.com/${owner}/${repo}/raw`
    );
    const shadowRead = await readLooseObjectRaw(env, repoId, seeded.nextCommit.oid, shadowCacheCtx);
    expect(shadowRead?.type).toBe(legacyRead?.type);
    expect(shadowRead?.payload).toEqual(legacyRead?.payload);
    expect(shadowCacheCtx.memo?.repoStorageMode).toBe("shadow-read");
    expect(shadowCacheCtx.memo?.loaderCalls ?? 0).toBeGreaterThan(0);

    const shadowRawResponse = await SELF.fetch(
      `https://example.com/${owner}/${repo}/raw?oid=${seeded.nextBlob.oid}&name=README.md`
    );
    expect(shadowRawResponse.status).toBe(200);
    expect(await shadowRawResponse.text()).toBe(legacyRawText);
  });

  it("blocks enabling packed reads validation when no active pack exists", async () => {
    const owner = "o";
    const repo = uniqueRepoId("storage-mode-no-pack");
    const repoId = `${owner}/${repo}`;
    const getStub = () =>
      env.REPO_DO.get(env.REPO_DO.idFromName(repoId)) as DurableObjectStub<RepoDurableObject>;

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
    expect(getJson.blockers).toContain(
      "Packed reads validation requires at least one active pack."
    );

    const setResponse = await SELF.fetch(
      `https://example.com/${owner}/${repo}/admin/storage-mode`,
      {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ mode: "shadow-read" }),
      }
    );
    expect(setResponse.status).toBe(409);
    const setJson = (await setResponse.json()) as {
      status?: string;
      message?: string;
    };
    expect(setJson.status).toBe("no_active_packs");
    expect(setJson.message).toContain("at least one active pack");

    const adminResponse = await SELF.fetch(`https://example.com/${owner}/${repo}/admin`);
    expect(adminResponse.status).toBe(200);
    const adminHtml = await adminResponse.text();
    expect(adminHtml).toContain("Packed Read Validation");
    expect(adminHtml).toContain("Packed reads validation requires at least one active pack.");
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
      body: JSON.stringify({ mode: "shadow-read" }),
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
      body: JSON.stringify({ mode: "shadow-read" }),
    });
    expect(response.status).toBe(409);
    const payload = (await response.json()) as { status?: string; message?: string };
    expect(payload.status).toBe("repo_busy");
    expect(payload.message).toContain("cannot change");
  });

  it("rejects selecting an unsupported storage mode from the admin route", async () => {
    const owner = "o";
    const repo = uniqueRepoId("storage-mode-unsupported-target");
    const repoId = `${owner}/${repo}`;
    await seedPackFirstRepo(repoId);

    const response = await SELF.fetch(`https://example.com/${owner}/${repo}/admin/storage-mode`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mode: "streaming" }),
    });
    expect(response.status).toBe(400);
    const payload = (await response.json()) as {
      status?: string;
      message?: string;
      targetMode?: string;
    };
    expect(payload.status).toBe("unsupported_target_mode");
    expect(payload.targetMode).toBe("streaming");
    expect(payload.message).toContain("legacy and shadow-read");
  });
});
