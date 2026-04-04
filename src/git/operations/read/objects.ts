import type { CacheContext } from "@/cache/index.ts";
import type { TreeEntry } from "./types.ts";

import { buildObjectCacheKey, cacheOrLoadObject } from "@/cache/index.ts";
import { createBlobFromBytes, createLogger } from "@/common/index.ts";
import { readObject } from "@/git/object-store/index.ts";

type LooseObjectRead = {
  type: string;
  payload: Uint8Array;
};

function ensureMemo(cacheCtx: CacheContext | undefined, repoId: string) {
  if (!cacheCtx) return;
  if (!cacheCtx.memo || (cacheCtx.memo.repoId && cacheCtx.memo.repoId !== repoId)) {
    cacheCtx.memo = { repoId };
    return;
  }
  if (!cacheCtx.memo.repoId) cacheCtx.memo.repoId = repoId;
}

export function parseTree(buf: Uint8Array): TreeEntry[] {
  const td = new TextDecoder();
  const out: TreeEntry[] = [];
  let i = 0;
  while (i < buf.length) {
    let sp = i;
    while (sp < buf.length && buf[sp] !== 0x20) sp++;
    if (sp >= buf.length) break;
    const mode = td.decode(buf.subarray(i, sp));
    let nul = sp + 1;
    while (nul < buf.length && buf[nul] !== 0x00) nul++;
    if (nul + 20 > buf.length) break;
    const name = td.decode(buf.subarray(sp + 1, nul));
    const oidBytes = buf.subarray(nul + 1, nul + 21);
    const oid = [...oidBytes].map((b) => b.toString(16).padStart(2, "0")).join("");
    out.push({ mode, name, oid });
    i = nul + 21;
  }
  return out;
}

/**
 * Pack-first object reader. Reads git objects from the active pack catalog
 * via the worker-local object store.
 */
export async function readLooseObjectRaw(
  env: Env,
  repoId: string,
  oid: string,
  cacheCtx?: CacheContext
): Promise<{ type: string; payload: Uint8Array } | undefined> {
  const oidLc = oid.toLowerCase();
  ensureMemo(cacheCtx, repoId);

  if (cacheCtx?.memo?.objects?.has(oidLc)) {
    return cacheCtx.memo.objects.get(oidLc);
  }

  const logger = createLogger(env.LOG_LEVEL, {
    service: "readObjectRaw",
    repoId,
  });
  const bypassCacheRead = cacheCtx?.memo?.flags?.has("no-cache-read") === true;

  const loadFromPacks = async (): Promise<LooseObjectRead | undefined> => {
    const packed = await readObject(env, repoId, oidLc, cacheCtx);
    if (packed) {
      logger.debug("object-read", {
        source: "pack-catalog",
        oid: oidLc,
        type: packed.type,
        packKey: packed.packKey,
      });
      return { type: packed.type, payload: packed.payload };
    }
    return undefined;
  };

  const storeMemoized = (value: LooseObjectRead | undefined) => {
    if (!cacheCtx?.memo) return;
    cacheCtx.memo.objects = cacheCtx.memo.objects || new Map();
    cacheCtx.memo.objects.set(oidLc, value);
  };

  if (cacheCtx) {
    const cacheKey = buildObjectCacheKey(cacheCtx.req, repoId, oidLc);
    const loaded = bypassCacheRead
      ? await loadFromPacks()
      : await cacheOrLoadObject(cacheKey, loadFromPacks, cacheCtx.ctx);

    storeMemoized(loaded);
    return loaded;
  }

  const loaded = await loadFromPacks();
  return loaded;
}

export async function readBlob(
  env: Env,
  repoId: string,
  oid: string,
  cacheCtx?: CacheContext
): Promise<{ content: Uint8Array | null; type: string | null }> {
  const obj = await readLooseObjectRaw(env, repoId, oid, cacheCtx);
  if (!obj) return { content: null, type: null };
  return { content: obj.payload, type: obj.type };
}

export async function readBlobStream(
  env: Env,
  repoId: string,
  oid: string,
  cacheCtx?: CacheContext
): Promise<Response | null> {
  const obj = await readLooseObjectRaw(env, repoId, oid, cacheCtx);
  if (!obj || obj.type !== "blob") return null;
  return new Response(createBlobFromBytes(obj.payload).stream(), {
    headers: {
      "Content-Type": "application/octet-stream",
      "Cache-Control": "public, max-age=31536000, immutable",
      ETag: `"${oid.toLowerCase()}"`,
    },
  });
}
