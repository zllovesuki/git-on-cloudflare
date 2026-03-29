import type { Logger } from "@/common/logger.ts";
import type { PackCatalogRow } from "../db/schema.ts";
import type { RepoStateSchema, TypedStorage } from "../repoState.ts";

import { getDb, listActivePackCatalog, upsertPackCatalogRow } from "../db/index.ts";
import { isPackKey, packIndexKey, r2PackDirPrefix } from "@/keys.ts";
import { bumpPacksetVersion, IDX_HEADER_LEN, uniq } from "./shared.ts";

type RepoCatalogPackInfo = {
  packKey: string;
  packBytes: number;
  idxBytes: number;
  objectCount: number;
  createdAt: number;
};

type ListedPackObject = Pick<R2Object, "key" | "size"> & {
  uploadedAt: number;
};

async function readIdxObjectCount(env: Env, idxKey: string): Promise<number | null> {
  const obj = await env.REPO_BUCKET.get(idxKey, { range: { offset: 0, length: IDX_HEADER_LEN } });
  if (!obj) return null;
  const buf = new Uint8Array(await obj.arrayBuffer());
  if (buf.byteLength < IDX_HEADER_LEN) return null;
  if (!(buf[0] === 0xff && buf[1] === 0x74 && buf[2] === 0x4f && buf[3] === 0x63)) return null;
  const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  const version = dv.getUint32(4, false);
  if (version !== 2 && version !== 3) return null;
  return dv.getUint32(8 + 255 * 4, false);
}

async function listAllPackObjects(env: Env, prefix: string): Promise<ListedPackObject[]> {
  const out: ListedPackObject[] = [];
  let cursor: string | undefined;
  const packPrefix = r2PackDirPrefix(prefix);
  do {
    const res = await env.REPO_BUCKET.list({ prefix: packPrefix, cursor });
    for (const obj of res.objects) {
      if (!isPackKey(obj.key)) continue;
      out.push({
        key: obj.key,
        size: obj.size,
        uploadedAt: obj.uploaded.getTime(),
      });
    }
    cursor = res.truncated ? res.cursor : undefined;
  } while (cursor);
  out.sort((a, b) => b.uploadedAt - a.uploadedAt || a.key.localeCompare(b.key));
  return out;
}

async function loadPackInfo(
  env: Env,
  packKey: string,
  seed?: Pick<ListedPackObject, "size" | "uploadedAt">
): Promise<RepoCatalogPackInfo | null> {
  const idxKey = packIndexKey(packKey);
  const [packHead, idxHead, objectCount] = await Promise.all([
    seed ? Promise.resolve<R2Object | null>(null) : env.REPO_BUCKET.head(packKey),
    env.REPO_BUCKET.head(idxKey),
    readIdxObjectCount(env, idxKey),
  ]);
  if ((!seed && !packHead) || !idxHead || objectCount === null) return null;

  const packBytes = seed?.size ?? packHead?.size;
  if (packBytes === undefined) return null;

  return {
    packKey,
    packBytes,
    idxBytes: idxHead.size,
    objectCount,
    createdAt: seed?.uploadedAt ?? packHead?.uploaded.getTime() ?? Date.now(),
  };
}

export async function hydrateLegacyCatalog(
  ctx: DurableObjectState,
  store: TypedStorage<RepoStateSchema>,
  env: Env,
  prefix: string,
  logger?: Logger
): Promise<PackCatalogRow[]> {
  const db = getDb(ctx.storage);
  const lastPackKey = await store.get("lastPackKey");
  const packList = (await store.get("packList")) ?? [];
  const listed = await listAllPackObjects(env, prefix);
  const listedByKey = new Map(listed.map((item) => [item.key, item]));

  // Preserve legacy ordering first, then append any R2-only packs so the automatic
  // backfill never forgets a pack that was already present in the repository.
  const orderedKnown = uniq([lastPackKey, ...packList]).filter((key) => listedByKey.has(key));
  const knownSet = new Set(orderedKnown);
  const ordered = [
    ...orderedKnown.map((key) => listedByKey.get(key)!),
    ...listed.filter((item) => !knownSet.has(item.key)),
  ];

  logger?.info("catalog:legacy-backfill:start", {
    knownPacks: orderedKnown.length,
    listedPacks: listed.length,
  });

  // Synthetic sequence numbers keep the newest legacy pack at the highest seq value
  // so worker reads can search the backfilled catalog in the same order as before.
  let seq = ordered.length;
  for (const item of ordered) {
    const info = await loadPackInfo(env, item.key, item);
    if (!info) {
      logger?.warn("catalog:legacy-backfill-skip", { packKey: item.key });
      continue;
    }
    await upsertPackCatalogRow(db, {
      packKey: info.packKey,
      kind: "legacy",
      state: "active",
      tier: 0,
      seqLo: seq,
      seqHi: seq,
      objectCount: info.objectCount,
      packBytes: info.packBytes,
      idxBytes: info.idxBytes,
      createdAt: info.createdAt,
      supersededBy: null,
    });
    seq--;
  }

  await store.put("nextPackSeq", Math.max(1, ordered.length + 1));
  const packsetVersion =
    ordered.length > 0 ? await bumpPacksetVersion(store) : (await store.get("packsetVersion")) || 0;
  const activeCatalog = await listActivePackCatalog(db);
  logger?.info("catalog:legacy-backfill:complete", {
    activePacks: activeCatalog.length,
    nextPackSeq: (await store.get("nextPackSeq")) || 1,
    packsetVersion,
  });
  return activeCatalog;
}
