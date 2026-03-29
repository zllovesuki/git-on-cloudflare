import { describe, expect, it } from "vitest";
import { createExecutionContext, env } from "cloudflare:test";

import type { CacheContext } from "@/cache/index.ts";
import { readLooseObjectRaw } from "@/git/operations/read/objects.ts";
import {
  callStubWithRetry,
  readRepoCatalogState,
  seedLegacyPackedRepo,
  uniqueRepoId,
} from "./util/test-helpers.ts";
import { buildPack } from "./util/test-helpers.ts";
import { encodeGitObject } from "@/git/core/index.ts";
import type { RepoDurableObject } from "@/index";

describe("packed object store shadow validation", () => {
  it("shadow-read packed validation does not mutate seeded catalog state", async () => {
    const repoId = `o/${uniqueRepoId("pack-shadow-state")}`;
    const olderPayload = new TextEncoder().encode("older shadow blob\n");
    const newerPayload = new TextEncoder().encode("newer shadow blob\n");
    const olderBlob = await encodeGitObject("blob", olderPayload);
    const newerBlob = await encodeGitObject("blob", newerPayload);
    const id = env.REPO_DO.idFromName(repoId);
    const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;

    const { getStub: seededStub, packKeys } = await seedLegacyPackedRepo({
      env,
      repoId,
      getStub,
      packs: [
        {
          name: "pack-shadow-newer.pack",
          packBytes: await buildPack([{ type: "blob", payload: newerPayload }]),
        },
        {
          name: "pack-shadow-older.pack",
          packBytes: await buildPack([{ type: "blob", payload: olderPayload }]),
        },
      ],
      looseObjects: [olderBlob, newerBlob],
    });

    await callStubWithRetry(seededStub, (stub) => stub.getActivePackCatalog());
    await callStubWithRetry(seededStub, (stub) => stub.setRepoStorageMode("shadow-read"));

    const before = await readRepoCatalogState(seededStub);
    const cacheCtx: CacheContext = {
      req: new Request("https://example.com/shadow"),
      ctx: createExecutionContext(),
      memo: {},
    };

    const legacy = await readLooseObjectRaw(env, repoId, olderBlob.oid, cacheCtx);
    const after = await readRepoCatalogState(seededStub);

    expect(legacy?.type).toBe("blob");
    expect(cacheCtx.memo?.packCatalog?.map((row) => row.packKey)).toEqual(packKeys);
    expect(cacheCtx.memo?.packedObjects?.get(olderBlob.oid)?.oid).toBe(olderBlob.oid);
    expect(after.packsetVersion).toBe(before.packsetVersion);
    expect(after.nextPackSeq).toBe(before.nextPackSeq);
    expect(after.activeCatalog).toEqual(before.activeCatalog);
  });
});
