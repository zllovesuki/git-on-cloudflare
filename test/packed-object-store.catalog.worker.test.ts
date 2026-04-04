import { describe, expect, it } from "vitest";
import { env } from "cloudflare:test";

import { getDb, supersedePackCatalogRows } from "@/do/repo/db/index.ts";
import type { RepoDurableObject } from "@/index";
import {
  buildPack,
  callStubWithRetry,
  readRepoCatalogState,
  runDOWithRetry,
  seedLegacyPackedRepo,
  seedPackedRepo,
  uniqueRepoId,
} from "./util/test-helpers.ts";
import { encodeGitObject } from "@/git/core/index.ts";

describe("packed object store catalog", () => {
  it("seeds active receive rows for packed repos", async () => {
    const repoId = `o/${uniqueRepoId("pack-catalog")}`;
    const id = env.REPO_DO.idFromName(repoId);
    const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;
    const { getStub: seededStub } = await seedPackedRepo({ env, repoId, getStub });

    const catalog = await callStubWithRetry(seededStub, (stub) => stub.getActivePackCatalog());
    expect(catalog.length).toBe(1);
    expect(catalog[0]?.kind).toBe("receive");
    expect(catalog[0]?.state).toBe("active");
    expect(catalog[0]?.objectCount).toBe(4);
    expect(catalog[0]?.packBytes).toBeGreaterThan(0);
    expect(catalog[0]?.idxBytes).toBeGreaterThan(0);
  });

  it("repeated catalog snapshots stay read-only after initial seed", async () => {
    const repoId = `o/${uniqueRepoId("pack-catalog-snapshot")}`;
    const id = env.REPO_DO.idFromName(repoId);
    const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;
    const seeded = await seedPackedRepo({ env, repoId, getStub });

    const first = await callStubWithRetry(seeded.getStub, (stub) => stub.getActivePackCatalog());
    expect(first).toHaveLength(1);

    const before = await readRepoCatalogState(seeded.getStub);
    const second = await callStubWithRetry(seeded.getStub, (stub) => stub.getActivePackCatalog());
    const after = await readRepoCatalogState(seeded.getStub);

    expect(second).toEqual(first);
    expect(after.packsetVersion).toBe(before.packsetVersion);
    expect(after.nextPackSeq).toBe(before.nextPackSeq);
    expect(after.activeCatalog).toEqual(before.activeCatalog);
    expect(after.catalog).toEqual(before.catalog);
  });

  it("catalog snapshots do not reactivate superseded packs from legacy mirrors", async () => {
    const repoId = `o/${uniqueRepoId("pack-superseded")}`;
    const olderPayload = new TextEncoder().encode("older pack blob\n");
    const newerPayload = new TextEncoder().encode("newer pack blob\n");
    const olderBlob = await encodeGitObject("blob", olderPayload);
    const newerBlob = await encodeGitObject("blob", newerPayload);
    const id = env.REPO_DO.idFromName(repoId);
    const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;

    const { packKeys, getStub: seededStub } = await seedLegacyPackedRepo({
      env,
      repoId,
      getStub,
      packs: [
        {
          name: "pack-newer.pack",
          packBytes: await buildPack([{ type: "blob", payload: newerPayload }]),
        },
        {
          name: "pack-older.pack",
          packBytes: await buildPack([{ type: "blob", payload: olderPayload }]),
        },
      ],
      looseObjects: [olderBlob, newerBlob],
    });

    const seeded = await callStubWithRetry(seededStub, (stub) => stub.getActivePackCatalog());
    expect(seeded.map((row) => row.packKey)).toEqual(packKeys);

    await runDOWithRetry(
      seededStub,
      async (_instance: RepoDurableObject, state: DurableObjectState) => {
        const db = getDb(state.storage);
        await supersedePackCatalogRows(db, [packKeys[1]], packKeys[0] || null);
      }
    );

    const beforeSnapshot = await readRepoCatalogState(seededStub);
    expect(beforeSnapshot.activeCatalog.map((row) => row.packKey)).toEqual([packKeys[0]]);
    expect(beforeSnapshot.catalog.find((row) => row.packKey === packKeys[1])?.state).toBe(
      "superseded"
    );

    const snapshot = await callStubWithRetry(seededStub, (stub) => stub.getActivePackCatalog());
    const afterSnapshot = await readRepoCatalogState(seededStub);

    expect(snapshot.map((row) => row.packKey)).toEqual([packKeys[0]]);
    expect(afterSnapshot.packsetVersion).toBe(beforeSnapshot.packsetVersion);
    expect(afterSnapshot.nextPackSeq).toBe(beforeSnapshot.nextPackSeq);
    expect(afterSnapshot.catalog.find((row) => row.packKey === packKeys[1])?.state).toBe(
      "superseded"
    );
  });
});
