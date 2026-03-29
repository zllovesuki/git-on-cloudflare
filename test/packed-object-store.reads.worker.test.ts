import { describe, expect, it } from "vitest";
import { env, SELF } from "cloudflare:test";

import { objKey } from "@/do/repo/repoState.ts";
import { readLooseObjectRaw } from "@/git/operations/read/objects.ts";
import { readObject, validatePackedObjectShadowRead } from "@/git/object-store/index.ts";
import {
  callStubWithRetry,
  runDOWithRetry,
  seedPackedRepo,
  uniqueRepoId,
} from "./util/test-helpers.ts";
import type { RepoDurableObject } from "@/index";

describe("packed object store reads", () => {
  it("matches legacy reads and still works after loose objects are deleted", async () => {
    const repoId = `o/${uniqueRepoId("pack-object-store")}`;
    const id = env.REPO_DO.idFromName(repoId);
    const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;
    const {
      getStub: seededStub,
      blob,
      tree,
      commit,
      tag,
      objectOids,
    } = await seedPackedRepo({
      env,
      repoId,
      getStub,
    });

    for (const obj of [blob, tree, commit, tag]) {
      const legacy = await readLooseObjectRaw(env, repoId, obj.oid);
      const packed = await readObject(env, repoId, obj.oid);
      expect(legacy?.type).toBe(packed?.type);
      expect(legacy?.payload).toEqual(packed?.payload);
    }

    await callStubWithRetry(seededStub, (stub) => stub.setRepoStorageMode("shadow-read"));
    const shadowLegacy = await readLooseObjectRaw(env, repoId, blob.oid);
    await validatePackedObjectShadowRead(env, repoId, blob.oid, shadowLegacy);

    await runDOWithRetry(seededStub, async (_instance, state) => {
      for (const oid of objectOids) await state.storage.delete(objKey(oid));
    });

    for (const obj of [blob, tree, commit, tag]) {
      const packed = await readObject(env, repoId, obj.oid);
      expect(packed?.type).toBe(
        obj === blob ? "blob" : obj === tree ? "tree" : obj === commit ? "commit" : "tag"
      );
      expect(packed?.payload).toEqual(obj.raw.subarray(obj.raw.indexOf(0) + 1));
    }
  });

  it("preserves legacy raw responses in shadow-read mode", async () => {
    const owner = "o";
    const repo = uniqueRepoId("pack-shadow-raw");
    const repoId = `${owner}/${repo}`;
    const id = env.REPO_DO.idFromName(repoId);
    const getStub = () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>;
    const { getStub: seededStub, blob } = await seedPackedRepo({ env, repoId, getStub });

    await callStubWithRetry(seededStub, (stub) => stub.setRepoStorageMode("shadow-read"));

    const res = await SELF.fetch(
      `https://example.com/${owner}/${repo}/raw?oid=${encodeURIComponent(blob.oid)}&name=hello.txt`
    );
    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toContain("text/plain");
    expect(await res.text()).toBe("hello from packed storage\n");

    const packed = await readObject(env, repoId, blob.oid);
    expect(packed?.type).toBe("blob");
    expect(packed?.payload).toEqual(blob.raw.subarray(blob.raw.indexOf(0) + 1));
  });
});
