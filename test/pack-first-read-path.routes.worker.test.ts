import { describe, expect, it } from "vitest";
import { env, SELF } from "cloudflare:test";

import { asTypedStorage, type RepoStateSchema } from "@/do/repo/repoState.ts";
import { computeNeededFast } from "@/git/operations/fetch/neededFast.ts";
import { packRefsKey } from "@/keys.ts";
import {
  deleteLooseObjectCopies,
  runDOWithRetry,
  toRequestBody,
  uniqueRepoId,
} from "./util/test-helpers.ts";
import { buildFetchBody, decodePktTextLines } from "./util/fetch-protocol.ts";
import { seedPackFirstRepo } from "./util/pack-first.ts";

describe("pack-first read path routes", () => {
  it("serves fetch and UI routes after deleting all loose object copies", async () => {
    const owner = "o";
    const repo = uniqueRepoId("pack-read-path");
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);

    await deleteLooseObjectCopies(env, seeded.getStub, seeded.objectOids);

    const needed = await computeNeededFast(
      env,
      repoId,
      [seeded.nextCommit.oid],
      [seeded.baseCommit.oid]
    );
    expect(new Set(needed)).toEqual(
      new Set([seeded.nextCommit.oid, seeded.nextTree.oid, seeded.nextBlob.oid])
    );

    const ackResponse = await SELF.fetch(`https://example.com/${owner}/${repo}/git-upload-pack`, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-git-upload-pack-request",
        "Git-Protocol": "version=2",
      },
      body: toRequestBody(
        buildFetchBody({
          wants: [seeded.nextCommit.oid],
          haves: [seeded.baseCommit.oid],
        })
      ),
    });
    expect(ackResponse.status).toBe(200);
    const ackLines = decodePktTextLines(new Uint8Array(await ackResponse.arrayBuffer()));
    expect(ackLines).toContain(`ACK ${seeded.baseCommit.oid} ready`);

    const fetchResponse = await SELF.fetch(`https://example.com/${owner}/${repo}/git-upload-pack`, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-git-upload-pack-request",
        "Git-Protocol": "version=2",
      },
      body: toRequestBody(
        buildFetchBody({
          wants: [seeded.nextCommit.oid],
          haves: [seeded.baseCommit.oid],
          done: true,
        })
      ),
    });
    expect(fetchResponse.status).toBe(200);
    const fetchBytes = new Uint8Array(await fetchResponse.arrayBuffer());
    expect(new TextDecoder().decode(fetchBytes.subarray(4, 13))).toBe("packfile\n");

    const treeResponse = await SELF.fetch(`https://example.com/${owner}/${repo}/tree?ref=main`);
    expect(treeResponse.status).toBe(200);
    expect(await treeResponse.text()).toContain("README.md");

    const blobResponse = await SELF.fetch(
      `https://example.com/${owner}/${repo}/blob?ref=main&path=${encodeURIComponent("README.md")}`
    );
    expect(blobResponse.status).toBe(200);
    expect(await blobResponse.text()).toContain("version two");

    const rawResponse = await SELF.fetch(
      `https://example.com/${owner}/${repo}/raw?oid=${seeded.nextBlob.oid}&name=README.md`
    );
    expect(rawResponse.status).toBe(200);
    expect(await rawResponse.text()).toBe("version two\n");

    const commitResponse = await SELF.fetch(
      `https://example.com/${owner}/${repo}/commit/${seeded.nextCommit.oid}`
    );
    expect(commitResponse.status).toBe(200);
    const commitHtml = await commitResponse.text();
    expect(commitHtml).toContain("second commit");
    expect(commitHtml).toContain("README.md");

    const diffResponse = await SELF.fetch(
      `https://example.com/${owner}/${repo}/commit/${seeded.nextCommit.oid}/diff?path=${encodeURIComponent("README.md")}`
    );
    expect(diffResponse.status).toBe(200);
    const diffJson = (await diffResponse.json()) as { patch?: string; skipReason?: string };
    expect(diffJson.skipReason).toBeUndefined();
    expect(diffJson.patch).toContain("-version one");
    expect(diffJson.patch).toContain("+version two");
  });

  it("keeps admin debug endpoints on the shared DO contract after loose copies are deleted", async () => {
    const owner = "o";
    const repo = uniqueRepoId("pack-debug-contract");
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    await deleteLooseObjectCopies(env, seeded.getStub, seeded.objectOids);

    const commitResponse = await SELF.fetch(
      `https://example.com/${owner}/${repo}/admin/debug-commit/${seeded.nextCommit.oid}`
    );
    expect(commitResponse.status).toBe(200);
    const commitJson = (await commitResponse.json()) as {
      commit?: { oid?: string; tree?: string; parents?: string[] };
      presence?: {
        hasLooseCommit?: boolean;
        hasLooseTree?: boolean;
        hasR2LooseTree?: boolean;
      };
      membership?: Record<string, { hasCommit?: boolean; hasTree?: boolean }>;
      inPacks?: unknown;
    };
    expect(commitJson.commit?.oid).toBe(seeded.nextCommit.oid);
    expect(commitJson.commit?.tree).toBe(seeded.nextTree.oid);
    expect(commitJson.presence?.hasLooseCommit).toBe(false);
    expect(commitJson.presence?.hasLooseTree).toBe(false);
    expect(commitJson.presence?.hasR2LooseTree).toBe(false);
    expect(commitJson.membership?.[seeded.packKeys[0]]).toEqual({
      hasCommit: true,
      hasTree: true,
    });
    expect(commitJson.inPacks).toBeUndefined();

    const oidResponse = await SELF.fetch(
      `https://example.com/${owner}/${repo}/admin/debug-oid/${seeded.nextBlob.oid}`
    );
    expect(oidResponse.status).toBe(200);
    const oidJson = (await oidResponse.json()) as {
      oid?: string;
      presence?: { hasLoose?: boolean; hasR2Loose?: boolean; hasPacked?: boolean };
      inPacks?: string[];
    };
    expect(oidJson.oid).toBe(seeded.nextBlob.oid);
    expect(oidJson.presence?.hasLoose).toBe(false);
    expect(oidJson.presence?.hasR2Loose).toBe(false);
    expect(oidJson.presence?.hasPacked).toBeUndefined();
    expect(oidJson.inPacks).toEqual([seeded.packKeys[0]]);
  });

  it("renders pack .refs sidecar status on the admin page", async () => {
    const owner = "o";
    const repo = uniqueRepoId("pack-admin-refs-sidecar");
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    const packKey = seeded.packKeys[0];
    if (!packKey) throw new Error("missing seeded pack key");

    const presentResponse = await SELF.fetch(`https://example.com/${owner}/${repo}/admin`);
    expect(presentResponse.status).toBe(200);
    const presentHtml = await presentResponse.text();
    expect(presentHtml).toContain("Reference sidecar is present in R2");

    await env.REPO_BUCKET.delete(packRefsKey(packKey));

    const missingResponse = await SELF.fetch(`https://example.com/${owner}/${repo}/admin`);
    expect(missingResponse.status).toBe(200);
    const missingHtml = await missingResponse.text();
    expect(missingHtml).toContain("Reference sidecar is missing from R2");
  });

  it("rejects deleting an active pack through the admin route", async () => {
    const owner = "o";
    const repo = uniqueRepoId("pack-delete-guard");
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    const activePackName = seeded.packKeys[0]?.split("/").pop();
    if (!activePackName) throw new Error("missing active pack name");

    const stateResponse = await SELF.fetch(
      `https://example.com/${owner}/${repo}/admin/debug-state`
    );
    expect(stateResponse.status).toBe(200);
    const stateJson = (await stateResponse.json()) as {
      activePacks?: Array<{ key: string }>;
      packCatalogVersion?: number;
    };
    expect(stateJson.activePacks?.[0]?.key).toBe(seeded.packKeys[0]);
    expect(typeof stateJson.packCatalogVersion).toBe("number");

    const deleteResponse = await SELF.fetch(
      `https://example.com/${owner}/${repo}/admin/pack/${encodeURIComponent(activePackName)}`,
      { method: "DELETE" }
    );
    expect(deleteResponse.status).toBe(409);
    const deleteJson = (await deleteResponse.json()) as { error?: string; rejected?: string };
    expect(deleteJson.rejected).toBe("active-pack");
    expect(deleteJson.error).toContain("Active packs");
  });

  it("rejects deleting a pack that is not superseded", async () => {
    const owner = "o";
    const repo = uniqueRepoId("pack-delete-non-superseded");
    const repoId = `${owner}/${repo}`;
    await seedPackFirstRepo(repoId);

    const deleteResponse = await SELF.fetch(
      `https://example.com/${owner}/${repo}/admin/pack/${encodeURIComponent("pack-missing.pack")}`,
      { method: "DELETE" }
    );
    expect(deleteResponse.status).toBe(409);
    const deleteJson = (await deleteResponse.json()) as {
      error?: string;
      rejected?: string;
      packState?: string;
    };
    expect(deleteJson.rejected).toBe("non-superseded-pack");
    expect(deleteJson.packState).toBe("unknown");
    expect(deleteJson.error).toContain("Only superseded packs");
  });

  it("renders receiving state on the admin page when a receive lease is active", async () => {
    const owner = "o";
    const repo = uniqueRepoId("pack-admin-receiving");
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);

    await runDOWithRetry(seeded.getStub, async (_instance, state) => {
      const store = asTypedStorage<RepoStateSchema>(state.storage);
      const now = Date.now();
      await store.put("receiveLease", {
        token: "test-receive-lease",
        createdAt: now,
        expiresAt: now + 60_000,
      });
    });

    const response = await SELF.fetch(`https://example.com/${owner}/${repo}/admin`);
    expect(response.status).toBe(200);
    const html = await response.text();
    expect(html).toContain("Receiving push...");
    expect(html).toContain("receive lease is active");
  });
});
