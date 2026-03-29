import type { Head, Ref, RepoStateSchema } from "@/do/repo/repoState.ts";
import type { TreeEntry } from "@/git/operations/read/types.ts";
import type { RepoDurableObject } from "@/index";

import { concatChunks, encodeGitObject } from "@/git/core/index.ts";
import { hexToBytes } from "@/common/index.ts";
import { asTypedStorage, objKey } from "@/do/repo/repoState.ts";
import { doPrefix, r2PackKey } from "@/keys.ts";
import { getDb, listActivePackCatalog, listPackCatalog } from "@/do/repo/db/index.ts";
import { indexPackOnly } from "@/git/pack/index.ts";
import { buildPack } from "./git-pack.ts";
import { runDOWithRetry } from "./do-retry.ts";

export type EncodedGitObject = Awaited<ReturnType<typeof encodeGitObject>>;

export type SeedLegacyPackedRepoArgs = {
  env: Env;
  repoId: string;
  getStub: () => DurableObjectStub<RepoDurableObject>;
  packs: Array<{ name: string; packBytes: Uint8Array }>;
  refs?: Ref[];
  head?: Head;
  looseObjects?: EncodedGitObject[];
};

export type SeedPackedRepoResult = {
  getStub: () => DurableObjectStub<RepoDurableObject>;
  packKeys: string[];
  blob: EncodedGitObject;
  tree: EncodedGitObject;
  commit: EncodedGitObject;
  tag: EncodedGitObject;
  objectOids: string[];
};

export type RepoCatalogStateSnapshot = {
  packsetVersion: number;
  nextPackSeq: number;
  activeCatalog: Awaited<ReturnType<typeof listActivePackCatalog>>;
  catalog: Awaited<ReturnType<typeof listPackCatalog>>;
};

export function buildTreePayload(entries: TreeEntry[]): Uint8Array {
  const encoder = new TextEncoder();
  const parts: Uint8Array[] = [];
  for (const entry of entries) {
    parts.push(
      encoder.encode(`${entry.mode} ${entry.name}`),
      Uint8Array.from([0]),
      hexToBytes(entry.oid)
    );
  }
  return concatChunks(parts);
}

export async function seedLegacyPackedRepo(
  args: SeedLegacyPackedRepoArgs
): Promise<{ getStub: () => DurableObjectStub<RepoDurableObject>; packKeys: string[] }> {
  const packKeys: string[] = [];

  await runDOWithRetry(args.getStub, async (_instance, state) => {
    const prefix = doPrefix(state.id.toString());
    const store = asTypedStorage<RepoStateSchema>(state.storage);
    let lastPackOids: string[] = [];

    for (const obj of args.looseObjects ?? []) {
      await store.put(objKey(obj.oid), obj.zdata);
    }

    for (let i = 0; i < args.packs.length; i++) {
      const pack = args.packs[i];
      const packKey = r2PackKey(prefix, pack.name);
      packKeys.push(packKey);
      await args.env.REPO_BUCKET.put(packKey, pack.packBytes);
      const oids = await indexPackOnly(pack.packBytes, args.env, packKey, state, prefix);
      if (i === 0) lastPackOids = oids;
    }

    if (packKeys.length === 0) throw new Error("seedLegacyPackedRepo requires at least one pack");

    const newestPackKey = packKeys[0];
    if (!newestPackKey) throw new Error("missing newest pack key");
    await store.put("lastPackKey", newestPackKey);
    await store.put("lastPackOids", lastPackOids);
    await store.put("packList", packKeys);
    if (args.refs) await store.put("refs", args.refs);
    if (args.head) await store.put("head", args.head);
  });

  return { getStub: args.getStub, packKeys };
}

export async function seedPackedRepo(args: {
  env: Env;
  repoId: string;
  getStub: () => DurableObjectStub<RepoDurableObject>;
}): Promise<SeedPackedRepoResult> {
  const author = "You <you@example.com> 0 +0000";
  const blobPayload = new TextEncoder().encode("hello from packed storage\n");
  const blob = await encodeGitObject("blob", blobPayload);
  const treePayload = buildTreePayload([{ mode: "100644", name: "hello.txt", oid: blob.oid }]);
  const tree = await encodeGitObject("tree", treePayload);
  const commitPayload = new TextEncoder().encode(
    `tree ${tree.oid}\n` + `author ${author}\n` + `committer ${author}\n\npacked commit\n`
  );
  const commit = await encodeGitObject("commit", commitPayload);
  const tagPayload = new TextEncoder().encode(
    `object ${commit.oid}\n` +
      `type commit\n` +
      `tag v1\n` +
      `tagger ${author}\n\n` +
      `packed tag\n`
  );
  const tag = await encodeGitObject("tag", tagPayload);

  const packBytes = await buildPack([
    { type: "blob", payload: blobPayload },
    { type: "tree", payload: treePayload },
    { type: "commit", payload: commitPayload },
    { type: "tag", payload: tagPayload },
  ]);

  const seeded = await seedLegacyPackedRepo({
    env: args.env,
    repoId: args.repoId,
    getStub: args.getStub,
    packs: [{ name: "pack-shadow-read.pack", packBytes }],
    refs: [
      { name: "refs/heads/main", oid: commit.oid },
      { name: "refs/tags/v1", oid: tag.oid },
    ],
    head: { target: "refs/heads/main", oid: commit.oid },
    looseObjects: [blob, tree, commit, tag],
  });

  return {
    ...seeded,
    blob,
    tree,
    commit,
    tag,
    objectOids: [blob.oid, tree.oid, commit.oid, tag.oid],
  };
}

export async function readRepoCatalogState(
  getStub: () => DurableObjectStub<RepoDurableObject>
): Promise<RepoCatalogStateSnapshot> {
  return await runDOWithRetry(
    getStub,
    async (_instance: RepoDurableObject, state: DurableObjectState) => {
      const store = asTypedStorage<RepoStateSchema>(state.storage);
      const db = getDb(state.storage);
      return {
        packsetVersion: (await store.get("packsetVersion")) || 0,
        nextPackSeq: (await store.get("nextPackSeq")) || 0,
        activeCatalog: await listActivePackCatalog(db),
        catalog: await listPackCatalog(db),
      };
    }
  );
}
