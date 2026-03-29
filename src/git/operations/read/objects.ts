import type { CacheContext } from "@/cache/index.ts";
import type { TreeEntry } from "./types.ts";

import { buildObjectCacheKey, cacheOrLoadObject } from "@/cache/index.ts";
import { createBlobFromBytes, createLogger, getRepoStub } from "@/common/index.ts";
import {
  loadRepoStorageMode,
  readObject,
  validatePackedObjectShadowRead,
} from "@/git/object-store/index.ts";
import { inflateAndParseHeader } from "@/git/core/index.ts";
import { countSubrequest, getLimiter } from "../limits.ts";

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

function logOnce(cacheCtx: CacheContext | undefined, flag: string, fn: () => void) {
  if (!cacheCtx) {
    fn();
    return;
  }
  cacheCtx.memo = cacheCtx.memo || {};
  cacheCtx.memo.flags = cacheCtx.memo.flags || new Set<string>();
  if (cacheCtx.memo.flags.has(flag)) return;
  fn();
  cacheCtx.memo.flags.add(flag);
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

async function readCompatibilityLooseObject(
  env: Env,
  repoId: string,
  oid: string,
  cacheCtx?: CacheContext
): Promise<LooseObjectRead | undefined> {
  ensureMemo(cacheCtx, repoId);
  const oidLc = oid.toLowerCase();
  const stub = getRepoStub(env, repoId);
  const logger = createLogger(env.LOG_LEVEL, {
    service: "readLooseObjectCompat",
    repoId,
    doId: stub.id.toString(),
  });
  const limiter = getLimiter(cacheCtx);

  if (cacheCtx?.memo) {
    const nextCalls = (cacheCtx.memo.loaderCalls ?? 0) + 1;
    cacheCtx.memo.loaderCalls = nextCalls;
    const cap = cacheCtx.memo.loaderCap;
    if (typeof cap === "number" && nextCalls > cap) {
      cacheCtx.memo.flags = cacheCtx.memo.flags || new Set<string>();
      cacheCtx.memo.flags.add("loader-capped");
      logOnce(cacheCtx, "compat-loader-capped-warned", () => {
        logger.warn("compat:loader-capped", { oid: oidLc, cap });
      });
      return undefined;
    }
  }

  try {
    const zdata = await limiter.run("do:get-object-compat", async () => {
      if (!countSubrequest(cacheCtx)) {
        logOnce(cacheCtx, "compat-soft-budget-warned", () => {
          logger.warn("soft-budget-exhausted", {
            op: "do:get-object-compat",
            oid: oidLc,
          });
        });
      }
      return await stub.getObject(oidLc);
    });
    if (!zdata) {
      logger.debug("compat:loose-miss", { oid: oidLc });
      return undefined;
    }
    const parsed = await inflateAndParseHeader(
      zdata instanceof Uint8Array ? zdata : new Uint8Array(zdata)
    );
    if (!parsed) {
      logger.debug("compat:loose-parse-miss", { oid: oidLc });
      return undefined;
    }
    logger.debug("compat:loose-hit", { oid: oidLc, type: parsed.type });
    return { type: parsed.type, payload: parsed.payload };
  } catch (error) {
    logger.debug("compat:loose-error", { oid: oidLc, error: String(error) });
    return undefined;
  }
}

async function maybeValidateShadowRead(
  env: Env,
  repoId: string,
  oid: string,
  cacheCtx: CacheContext | undefined,
  legacy: LooseObjectRead | null | undefined = undefined
): Promise<void> {
  try {
    const mode = await loadRepoStorageMode(env, repoId, cacheCtx);
    if (mode !== "shadow-read") return;
    const legacyObject =
      legacy === undefined
        ? await readCompatibilityLooseObject(env, repoId, oid, cacheCtx)
        : legacy || undefined;
    if (!legacyObject && cacheCtx?.memo?.flags?.has("loader-capped")) {
      return;
    }
    await validatePackedObjectShadowRead(env, repoId, oid, legacyObject, cacheCtx);
  } catch {
    // Shadow validation is best-effort and must never affect correctness.
  }
}

/**
 * Despite the historical name, this is now the shared pack-first object reader.
 * Loose object RPCs remain only as a compatibility fallback for repos that have
 * not been migrated onto the active pack-catalog read path yet.
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
  let compatLegacy: LooseObjectRead | null | undefined;

  const loadPackedFirst = async (): Promise<LooseObjectRead | undefined> => {
    const packed = await readObject(env, repoId, oidLc, cacheCtx);
    if (packed) {
      compatLegacy = undefined;
      logger.debug("object-read", {
        source: "pack-catalog",
        oid: oidLc,
        type: packed.type,
        packKey: packed.packKey,
      });
      return { type: packed.type, payload: packed.payload };
    }

    const compat = await readCompatibilityLooseObject(env, repoId, oidLc, cacheCtx);
    compatLegacy = compat || null;
    if (compat) {
      logger.debug("object-read", {
        source: "compat-loose",
        oid: oidLc,
        type: compat.type,
      });
    }
    return compat;
  };

  const storeMemoized = (value: LooseObjectRead | undefined) => {
    if (!cacheCtx?.memo) return;
    cacheCtx.memo.objects = cacheCtx.memo.objects || new Map();
    cacheCtx.memo.objects.set(oidLc, value);
  };

  if (cacheCtx) {
    const cacheKey = buildObjectCacheKey(cacheCtx.req, repoId, oidLc);
    const loaded = bypassCacheRead
      ? await loadPackedFirst()
      : await cacheOrLoadObject(cacheKey, loadPackedFirst, cacheCtx.ctx);

    storeMemoized(loaded);

    await maybeValidateShadowRead(env, repoId, oidLc, cacheCtx, compatLegacy);
    return loaded;
  }

  const loaded = await loadPackedFirst();
  await maybeValidateShadowRead(env, repoId, oidLc, cacheCtx, compatLegacy);
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
