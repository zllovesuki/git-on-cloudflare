import { expect, it } from "vitest";
import { createExecutionContext, env } from "cloudflare:test";
import type { RepoDurableObject } from "@/index";
import type { CacheContext } from "@/cache/index.ts";

import { asTypedStorage, objKey, type RepoStateSchema } from "@/do/repo/repoState.ts";
import { getDb, getPackCatalogCount } from "@/do/repo/db/index.ts";
import { readLooseObjectRaw } from "@/git";
import { readObject, hasObjectsBatch, readObjectRefsBatch } from "@/git/object-store";
import { doPrefix, r2LooseKey } from "@/keys.ts";
import { callStubWithRetry, runDOWithRetry, uniqueRepoId } from "./util/test-helpers.ts";

it("pack catalog backfills lazily from seeded legacy pack metadata", async () => {
  const owner = "o";
  const repo = uniqueRepoId("pack-catalog");
  const repoId = `${owner}/${repo}`;
  const id = env.REPO_DO.idFromName(repoId);
  const getStub: () => DurableObjectStub<RepoDurableObject> = () => env.REPO_DO.get(id);

  await runDOWithRetry(getStub, async (instance: RepoDurableObject, state: DurableObjectState) => {
    await instance.seedMinimalRepo();
    const db = getDb(state.storage);
    expect(await getPackCatalogCount(db)).toBe(0);
  });

  const rows = await callStubWithRetry(getStub, (stub) => stub.getActivePackCatalog());
  expect(rows.length).toBe(1);
  expect(rows[0].kind).toBe("legacy");
  expect(rows[0].state).toBe("active");
  expect(rows[0].packKey.endsWith(".pack")).toBe(true);

  await runDOWithRetry(getStub, async (_instance: RepoDurableObject, state: DurableObjectState) => {
    const db = getDb(state.storage);
    expect(await getPackCatalogCount(db)).toBe(1);
  });
});

it("worker object store reads packed objects after loose data is deleted", async () => {
  const owner = "o";
  const repo = uniqueRepoId("pack-only");
  const repoId = `${owner}/${repo}`;
  const id = env.REPO_DO.idFromName(repoId);
  const getStub: () => DurableObjectStub<RepoDurableObject> = () => env.REPO_DO.get(id);

  const { commitOid, treeOid } = await runDOWithRetry(
    getStub,
    async (instance: RepoDurableObject) => await instance.seedMinimalRepo()
  );

  await callStubWithRetry(getStub, (stub) => stub.getActivePackCatalog());

  await runDOWithRetry(getStub, async (_instance: RepoDurableObject, state: DurableObjectState) => {
    const store = asTypedStorage<RepoStateSchema>(state.storage);
    await store.delete(objKey(commitOid));
    await store.delete(objKey(treeOid));
  });

  const prefix = doPrefix(id.toString());
  await env.REPO_BUCKET.delete(r2LooseKey(prefix, commitOid));
  await env.REPO_BUCKET.delete(r2LooseKey(prefix, treeOid));

  const commit = await readObject(env, repoId, commitOid);
  const tree = await readObject(env, repoId, treeOid);
  expect(commit?.type).toBe("commit");
  expect(tree?.type).toBe("tree");

  const present = await hasObjectsBatch(env, repoId, [commitOid, treeOid, "0".repeat(40)]);
  expect(present).toEqual([true, true, false]);

  const refs = await readObjectRefsBatch(env, repoId, [commitOid, treeOid]);
  expect(refs.get(commitOid)).toEqual([treeOid]);
  expect(refs.get(treeOid)).toEqual([]);
});

it("shadow-read validates packed reads while preserving legacy results", async () => {
  const owner = "o";
  const repo = uniqueRepoId("shadow-read");
  const repoId = `${owner}/${repo}`;
  const id = env.REPO_DO.idFromName(repoId);
  const getStub: () => DurableObjectStub<RepoDurableObject> = () => env.REPO_DO.get(id);

  const { commitOid } = await runDOWithRetry(getStub, async (instance: RepoDurableObject) => {
    const seeded = await instance.seedMinimalRepo();
    await instance.setRepoStorageMode("shadow-read");
    return seeded;
  });

  const cacheCtx: CacheContext = {
    req: new Request("https://example.com/shadow"),
    ctx: createExecutionContext(),
    memo: {},
  };

  const legacy = await readLooseObjectRaw(env, repoId, commitOid, cacheCtx);
  expect(legacy?.type).toBe("commit");
  expect(cacheCtx.memo?.repoStorageMode).toBe("shadow-read");
  expect(cacheCtx.memo?.packedObjects?.get(commitOid)?.oid).toBe(commitOid);
});
