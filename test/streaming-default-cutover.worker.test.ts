import type { RepoDurableObject } from "@/index";
import type { RepoStorageModeMutationResult } from "@/contracts/repoStorageMode.ts";

import { describe, expect, it } from "vitest";
import { env, SELF } from "cloudflare:test";

import { asTypedStorage, type RepoStateSchema } from "@/do/repo/repoState.ts";
import { callStubWithRetry, runDOWithRetry, uniqueRepoId } from "./util/test-helpers.ts";
import { seedPackFirstRepo } from "./util/pack-first.ts";

describe("streaming-by-default cutover", () => {
  it("new empty repo defaults to streaming mode", async () => {
    const owner = "o";
    const repo = uniqueRepoId("empty-default");
    const repoId = `${owner}/${repo}`;
    const getStub = () =>
      env.REPO_DO.get(env.REPO_DO.idFromName(repoId)) as DurableObjectStub<RepoDurableObject>;

    // Touch the DO to trigger ensureRepoMetadataDefaults (via ensureAccessAndAlarm)
    const mode = await callStubWithRetry(getStub, (stub) => stub.getRepoStorageMode());
    expect(mode).toBe("streaming");
  });

  it("sanitizeRawStorageMode normalizes shadow-read to streaming", async () => {
    const owner = "o";
    const repo = uniqueRepoId("shadow-normalize");
    const repoId = `${owner}/${repo}`;
    const getStub = () =>
      env.REPO_DO.get(env.REPO_DO.idFromName(repoId)) as DurableObjectStub<RepoDurableObject>;

    // Test the sanitization function directly inside the DO context.
    // Write shadow-read to raw storage, then run sanitizeRawStorageMode
    // to confirm it normalizes to streaming.
    const { sanitizeRawStorageMode } = await import("@/do/repo/catalog/shared.ts");
    const normalizedMode = await runDOWithRetry(getStub, async (_instance, state) => {
      await state.storage.put("repoStorageMode", "shadow-read");
      const noopLogger = { info: () => {} };
      return await sanitizeRawStorageMode(state.storage, noopLogger);
    });
    expect(normalizedMode).toBe("streaming");

    // Verify storage was actually updated
    const storedMode = await runDOWithRetry(getStub, async (_instance, state) => {
      return (await state.storage.get("repoStorageMode")) as string;
    });
    expect(storedMode).toBe("streaming");
  });

  it("truly empty repo can transition between legacy and streaming without backfill", async () => {
    const owner = "o";
    const repo = uniqueRepoId("empty-escape-hatch");
    const repoId = `${owner}/${repo}`;
    const getStub = () =>
      env.REPO_DO.get(env.REPO_DO.idFromName(repoId)) as DurableObjectStub<RepoDurableObject>;

    // Start as streaming (empty default)
    const mode = await callStubWithRetry(getStub, (stub) => stub.getRepoStorageMode());
    expect(mode).toBe("streaming");

    // streaming → legacy: should work for truly empty repo (no backfill needed)
    const toLegacyResult = await SELF.fetch(
      `https://example.com/${owner}/${repo}/admin/storage-mode`,
      {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ mode: "legacy" }),
      }
    );
    expect(toLegacyResult.status).toBe(200);
    const toLegacy = (await toLegacyResult.json()) as RepoStorageModeMutationResult;
    expect(toLegacy.status).toBe("ok");
    if (toLegacy.status === "ok") {
      expect(toLegacy.changed).toBe(true);
      expect(toLegacy.currentMode).toBe("legacy");
    }

    // legacy → streaming: should work for truly empty repo (no packs needed)
    const toStreamingResult = await SELF.fetch(
      `https://example.com/${owner}/${repo}/admin/storage-mode`,
      {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ mode: "streaming" }),
      }
    );
    expect(toStreamingResult.status).toBe(200);
    const toStreaming = (await toStreamingResult.json()) as RepoStorageModeMutationResult;
    expect(toStreaming.status).toBe("ok");
    if (toStreaming.status === "ok") {
      expect(toStreaming.changed).toBe(true);
      expect(toStreaming.currentMode).toBe("streaming");
    }
  });

  it("blocks promoting non-empty zero-pack repo to streaming", async () => {
    const owner = "o";
    const repo = uniqueRepoId("nonempty-nopack");
    const repoId = `${owner}/${repo}`;
    const getStub = () =>
      env.REPO_DO.get(env.REPO_DO.idFromName(repoId)) as DurableObjectStub<RepoDurableObject>;

    // Seed a repo with refs but no packs (loose-only, the unsupported case)
    await runDOWithRetry(getStub, async (instance) => {
      await instance.seedMinimalRepo(false);
    });

    // Should be on legacy (has data signals)
    const mode = await callStubWithRetry(getStub, (stub) => stub.getRepoStorageMode());
    expect(mode).toBe("legacy");

    // Try to promote via admin API — should fail
    const response = await SELF.fetch(`https://example.com/${owner}/${repo}/admin/storage-mode`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mode: "streaming" }),
    });
    expect(response.status).toBe(409);
    const result = (await response.json()) as RepoStorageModeMutationResult;
    expect(result.status).toBe("no_active_packs");
  });

  it("rollback: legacy mode is functional for repos with pack data", async () => {
    const owner = "o";
    const repo = uniqueRepoId("rollback-compat");
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);

    // Promote to streaming via admin API
    const promoteRes = await SELF.fetch(`https://example.com/${owner}/${repo}/admin/storage-mode`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mode: "streaming" }),
    });
    expect(promoteRes.status).toBe(200);

    // Verify fetch still works in streaming mode
    const fetchRes1 = await SELF.fetch(
      `https://example.com/${owner}/${repo}/raw?oid=${seeded.nextBlob.oid}&name=README.md`
    );
    expect(fetchRes1.status).toBe(200);
    const text1 = await fetchRes1.text();
    expect(text1).toBe("version two\n");

    // Prepare rollback backfill, then revert to legacy
    const backfillRes = await SELF.fetch(
      `https://example.com/${owner}/${repo}/admin/storage-mode/backfill`,
      { method: "POST" }
    );
    expect([200, 202]).toContain(backfillRes.status);

    // Advance the backfill to ready (in real usage the maintenance worker does this)
    await runDOWithRetry(seeded.getStub, async (_instance, state) => {
      const store = asTypedStorage<RepoStateSchema>(state.storage);
      const backfill = await store.get("legacyCompatBackfill");
      if (backfill) {
        await store.put("legacyCompatBackfill", {
          ...backfill,
          status: "ready",
          completedAt: Date.now(),
        });
      }
    });

    const revertRes = await SELF.fetch(`https://example.com/${owner}/${repo}/admin/storage-mode`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mode: "legacy" }),
    });
    expect(revertRes.status).toBe(200);
    const reverted = (await revertRes.json()) as RepoStorageModeMutationResult;
    expect(reverted.status).toBe("ok");
    if (reverted.status === "ok") {
      expect(reverted.currentMode).toBe("legacy");
    }

    // Verify fetch still works in legacy mode (pack-first reads are the same)
    const fetchRes2 = await SELF.fetch(
      `https://example.com/${owner}/${repo}/raw?oid=${seeded.nextBlob.oid}&name=README.md`
    );
    expect(fetchRes2.status).toBe(200);
    expect(await fetchRes2.text()).toBe("version two\n");
  });
});
