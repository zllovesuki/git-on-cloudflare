import { describe, expect, it } from "vitest";
import { env } from "cloudflare:test";

import { concatChunks, encodeGitObject } from "@/git/core/index.ts";
import { readObject } from "@/git/object-store/index.ts";
import {
  buildAppendOnlyDelta,
  buildPack,
  callStubWithRetry,
  seedLegacyPackedRepo,
  uniqueRepoId,
} from "./util/test-helpers.ts";
import type { RepoDurableObject } from "@/index";

describe("packed object store deltas", () => {
  it("resolves OFS_DELTA blobs from pack-only storage", async () => {
    const repoId = `o/${uniqueRepoId("pack-ofs-delta")}`;
    const basePayload = new TextEncoder().encode("delta base\n");
    const suffix = new TextEncoder().encode("plus ofs delta\n");
    const resultPayload = concatChunks([basePayload, suffix]);
    const baseBlob = await encodeGitObject("blob", basePayload);
    const deltaBlob = await encodeGitObject("blob", resultPayload);
    const id = env.REPO_DO.idFromName(repoId);
    const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;

    const packBytes = await buildPack([
      { type: "blob", payload: basePayload },
      {
        type: "ofs-delta",
        baseIndex: 0,
        delta: buildAppendOnlyDelta(basePayload, suffix),
      },
    ]);

    const { getStub: seededStub } = await seedLegacyPackedRepo({
      env,
      repoId,
      getStub,
      packs: [{ name: "pack-ofs-delta.pack", packBytes }],
    });

    const catalog = await callStubWithRetry(seededStub, (stub) => stub.getActivePackCatalog());
    expect(catalog).toHaveLength(1);

    const loadedBase = await readObject(env, repoId, baseBlob.oid);
    const loadedDelta = await readObject(env, repoId, deltaBlob.oid);
    expect(loadedBase?.type).toBe("blob");
    expect(loadedBase?.payload).toEqual(basePayload);
    expect(loadedDelta?.type).toBe("blob");
    expect(loadedDelta?.payload).toEqual(resultPayload);
  });

  it("resolves REF_DELTA blobs across active catalog packs", async () => {
    const repoId = `o/${uniqueRepoId("pack-ref-delta")}`;
    const basePayload = new TextEncoder().encode("older pack blob\n");
    const suffix = new TextEncoder().encode("newer ref delta\n");
    const resultPayload = concatChunks([basePayload, suffix]);
    const baseBlob = await encodeGitObject("blob", basePayload);
    const deltaBlob = await encodeGitObject("blob", resultPayload);
    const id = env.REPO_DO.idFromName(repoId);
    const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;

    const basePackBytes = await buildPack([{ type: "blob", payload: basePayload }]);
    const refPackBytes = await buildPack([
      {
        type: "ref-delta",
        baseOid: baseBlob.oid,
        delta: buildAppendOnlyDelta(basePayload, suffix),
      },
    ]);

    const { getStub: seededStub, packKeys } = await seedLegacyPackedRepo({
      env,
      repoId,
      getStub,
      packs: [
        { name: "pack-ref-delta.pack", packBytes: refPackBytes },
        { name: "pack-base.pack", packBytes: basePackBytes },
      ],
      looseObjects: [baseBlob],
    });

    const catalog = await callStubWithRetry(seededStub, (stub) => stub.getActivePackCatalog());
    expect(catalog.map((row) => row.packKey)).toEqual(packKeys);

    const loadedBase = await readObject(env, repoId, baseBlob.oid);
    const loadedDelta = await readObject(env, repoId, deltaBlob.oid);
    expect(loadedBase?.type).toBe("blob");
    expect(loadedBase?.payload).toEqual(basePayload);
    expect(loadedDelta?.type).toBe("blob");
    expect(loadedDelta?.payload).toEqual(resultPayload);
  });
});
