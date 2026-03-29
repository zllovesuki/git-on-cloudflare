import type { CacheContext } from "@/cache/index.ts";
import type { TreeEntry } from "./types.ts";
import { packIndexKey } from "@/keys.ts";
import { loadRepoStorageMode, validatePackedObjectShadowRead } from "@/git/object-store/index.ts";
import { getPackCandidates } from "../packDiscovery.ts";
import { getLimiter, countSubrequest } from "../limits.ts";
import { createMemPackFs, createStubLooseLoader } from "@/git/pack/index.ts";
import { buildObjectCacheKey, cacheOrLoadObject, cachePutObject } from "@/cache/index.ts";
import { createLogger, createInflateStream, getRepoStub } from "@/common/index.ts";
import * as git from "isomorphic-git";
import { inflateAndParseHeader } from "@/git/core/index.ts";

const LOADER_CAP = 400; // cap DO loose-loader calls per request in heavy mode

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
  oid: string
): Promise<Response | null> {
  const mode = await loadRepoStorageMode(env, repoId);
  if (mode === "shadow-read") {
    try {
      // Raw/blob routes still stream from the legacy object source in phase 1.
      // Force one legacy read first so shadow mode validates the packed resolver
      // without changing the user-visible response path yet.
      await readLooseObjectRaw(env, repoId, oid);
    } catch {}
  }

  const stub = getRepoStub(env, repoId);
  const objStream = await stub.getObjectStream(oid);
  if (!objStream) return null;

  let headerParsed = false;
  let buffer = new Uint8Array(0);

  const { readable, writable } = new TransformStream<Uint8Array, Uint8Array>({
    transform(chunk: Uint8Array, controller) {
      if (!headerParsed) {
        const combined = new Uint8Array(buffer.length + chunk.length);
        combined.set(buffer);
        combined.set(chunk, buffer.length);

        const nullIndex = combined.indexOf(0);
        if (nullIndex !== -1) {
          headerParsed = true;
          if (nullIndex + 1 < combined.length) {
            controller.enqueue(combined.slice(nullIndex + 1));
          }
        } else {
          buffer = combined;
        }
      } else {
        controller.enqueue(chunk);
      }
    },
  });

  const decompressed = objStream
    .pipeThrough(createInflateStream())
    .pipeThrough({ readable, writable });

  return new Response(decompressed, {
    headers: {
      "Content-Type": "application/octet-stream",
      "Cache-Control": "public, max-age=31536000, immutable",
      ETag: `"${oid}"`,
    },
  });
}

export async function readLooseObjectRaw(
  env: Env,
  repoId: string,
  oid: string,
  cacheCtx?: CacheContext
): Promise<{ type: string; payload: Uint8Array } | undefined> {
  const oidLc = oid.toLowerCase();
  const stub = getRepoStub(env, repoId);
  const doId = stub.id.toString();
  const logger = createLogger(env.LOG_LEVEL, {
    service: "readLooseObjectRaw",
    repoId,
    doId,
  });

  if (cacheCtx) {
    if (!cacheCtx.memo || (cacheCtx.memo.repoId && cacheCtx.memo.repoId !== repoId)) {
      cacheCtx.memo = { repoId };
    } else if (!cacheCtx.memo.repoId) {
      cacheCtx.memo.repoId = repoId;
    }
  }

  if (cacheCtx?.memo?.objects?.has(oidLc)) {
    return cacheCtx.memo.objects.get(oidLc);
  }

  const heavyNoCache = cacheCtx?.memo?.flags?.has("no-cache-read") === true;
  const limiter = getLimiter(cacheCtx);
  const withShadowValidation = async (value: { type: string; payload: Uint8Array } | undefined) => {
    try {
      await validatePackedObjectShadowRead(env, repoId, oidLc, value, cacheCtx);
    } catch {}
    return value;
  };

  async function addPackToFiles(
    env: Env,
    packKey: string,
    files: Map<string, Uint8Array>
  ): Promise<boolean> {
    const [p, i] = await Promise.all([
      limiter.run("r2:get-pack", async () => {
        if (!countSubrequest(cacheCtx)) {
          logger.warn("soft-budget-exhausted", { op: "r2:get-pack", key: packKey });
          return null;
        }
        return await env.REPO_BUCKET.get(packKey);
      }),
      limiter.run("r2:get-idx", async () => {
        if (!countSubrequest(cacheCtx)) {
          logger.warn("soft-budget-exhausted", { op: "r2:get-idx", key: packKey });
          return null;
        }
        return await env.REPO_BUCKET.get(packIndexKey(packKey));
      }),
    ]);
    if (!p || !i) return false;

    const [packArrayBuf, idxArrayBuf] = await Promise.all([p.arrayBuffer(), i.arrayBuffer()]);
    const packBuf = new Uint8Array(packArrayBuf);
    const idxBuf = new Uint8Array(idxArrayBuf);
    const base = packKey.split("/").pop()!;
    const idxBase = base.replace(/\.pack$/i, ".idx");
    files.set(`/git/objects/pack/${base}`, packBuf);
    files.set(`/git/objects/pack/${idxBase}`, idxBuf);
    return true;
  }

  const loadFromPacks = async () => {
    try {
      const packListRaw = await getPackCandidates(env, stub, doId, heavyNoCache, cacheCtx);
      let packList: string[] = packListRaw;
      const PROBE_MAX = heavyNoCache ? 10 : packList.length;
      if (packList.length > PROBE_MAX) packList = packList.slice(0, PROBE_MAX);
      if (cacheCtx?.memo) {
        cacheCtx.memo.flags = cacheCtx.memo.flags || new Set();
        if (!cacheCtx.memo.flags.has("pack-list-candidates-logged")) {
          logger.debug("pack-list-candidates", { count: packList.length });
          cacheCtx.memo.flags.add("pack-list-candidates-logged");
        }
      } else {
        logger.debug("pack-list-candidates", { count: packList.length });
      }
      if (packList.length === 0) {
        const alreadyWarned = cacheCtx?.memo?.flags?.has("pack-list-empty");
        if (!alreadyWarned) {
          logger.warn("pack-list-empty", { oid: oidLc, afterFallbacks: true });
          if (cacheCtx?.memo) {
            cacheCtx.memo.flags = cacheCtx.memo.flags || new Set();
            cacheCtx.memo.flags.add("pack-list-empty");
          }
        }
        return undefined;
      }

      let chosenPackKey: string | undefined;
      const contains: Record<string, boolean> = {};
      for (const key of packList) {
        try {
          let set: Set<string>;
          if (cacheCtx?.memo?.packOids?.has(key)) {
            set = cacheCtx.memo.packOids.get(key)!;
          } else {
            const dataOids = await limiter.run("do:getPackOids", async () => {
              if (!countSubrequest(cacheCtx)) {
                logger.warn("soft-budget-exhausted", { op: "do:getPackOids", key });
                return [] as string[];
              }
              return await stub.getPackOids(key);
            });
            set = new Set((dataOids || []).map((x: string) => x.toLowerCase()));
            if (cacheCtx?.memo) {
              cacheCtx.memo.packOids = cacheCtx.memo.packOids || new Map();
              cacheCtx.memo.packOids.set(key, set);
            }
          }
          const has = set.has(oidLc);
          contains[key] = has;
          if (!chosenPackKey && has) chosenPackKey = key;
        } catch {}
      }
      if (!chosenPackKey) chosenPackKey = packList[0];
      if (cacheCtx?.memo) {
        cacheCtx.memo.flags = cacheCtx.memo.flags || new Set();
        if (!cacheCtx.memo.flags.has("chosen-pack-logged")) {
          logger.debug("chosen-pack", { chosenPackKey, hasDirectHit: !!contains[chosenPackKey] });
          cacheCtx.memo.flags.add("chosen-pack-logged");
        }
      } else {
        logger.debug("chosen-pack", { chosenPackKey, hasDirectHit: !!contains[chosenPackKey] });
      }

      const order: string[] = (() => {
        const arr = packList.slice(0);
        if (chosenPackKey) {
          const i = arr.indexOf(chosenPackKey);
          if (i > 0) {
            arr.splice(i, 1);
            arr.unshift(chosenPackKey);
          } else if (i < 0) {
            arr.unshift(chosenPackKey);
          }
        }
        const LOAD_MAX = heavyNoCache ? 12 : 20;
        if (arr.length > LOAD_MAX) arr.length = LOAD_MAX;
        return arr;
      })();

      let files: Map<string, Uint8Array>;
      if (cacheCtx?.memo?.packFiles) {
        files = cacheCtx.memo.packFiles;
      } else {
        files = new Map<string, Uint8Array>();
        if (cacheCtx?.memo) cacheCtx.memo.packFiles = files;
      }
      const loaded = new Set<string>();
      const BATCH = 5;
      const dir = "/git";
      const baseLoader = createStubLooseLoader(stub);
      const looseLoader = async (oid: string) => {
        if (cacheCtx?.memo) {
          const next = (cacheCtx.memo.loaderCalls ?? 0) + 1;
          cacheCtx.memo.loaderCalls = next;
          const cap = cacheCtx.memo.loaderCap ?? LOADER_CAP;
          if (heavyNoCache && next > cap) {
            cacheCtx.memo.flags = cacheCtx.memo.flags || new Set();
            if (!cacheCtx.memo.flags.has("loader-capped")) {
              logger.warn("read:loader-calls-capped", { cap });
              cacheCtx.memo.flags.add("loader-capped");
              cacheCtx.memo.flags.add("closure-timeout");
            }
            return undefined;
          }
        }
        return await limiter.run("do:getObject", async () => {
          countSubrequest(cacheCtx);
          return await baseLoader(oid);
        });
      };
      const fs = createMemPackFs(files, { looseLoader });

      for (let idx = 0; idx < order.length; idx += BATCH) {
        const batch = order.slice(idx, idx + BATCH).filter((k) => !loaded.has(k));
        await Promise.all(
          batch.map(async (key) => {
            try {
              const base = key.split("/").pop()!;
              const idxBase = base.replace(/\.pack$/i, ".idx");
              if (
                files.has(`/git/objects/pack/${base}`) &&
                files.has(`/git/objects/pack/${idxBase}`)
              ) {
                loaded.add(key);
                return;
              }
              const ok = await addPackToFiles(env, key, files);
              if (ok) loaded.add(key);
            } catch {}
          })
        );
        if (files.size === 0) continue;
        try {
          const result = (await git.readObject({ fs, dir, oid: oidLc, format: "content" })) as {
            object: Uint8Array;
            type: "blob" | "tree" | "commit" | "tag";
          };
          if (cacheCtx?.memo) {
            cacheCtx.memo.flags = cacheCtx.memo.flags || new Set();
            if (!cacheCtx.memo.flags.has("object-read-logged")) {
              logger.debug("object-read", {
                source: "r2-packs",
                chosenPackKey,
                packsLoaded: files.size,
                type: result.type,
              });
              cacheCtx.memo.flags.add("object-read-logged");
            }
          } else {
            logger.debug("object-read", {
              source: "r2-packs",
              chosenPackKey,
              packsLoaded: files.size,
              type: result.type,
            });
          }
          if (cacheCtx?.memo) {
            cacheCtx.memo.objects = cacheCtx.memo.objects || new Map();
            cacheCtx.memo.objects.set(oidLc, { type: result.type, payload: result.object });
          }
          return { type: result.type, payload: result.object };
        } catch (e) {
          logger.debug("git-readObject-miss", {
            error: String(e),
            oid: oidLc,
            packsTried: files.size,
          });
        }
      }
      return undefined;
    } catch (e) {
      logger.debug("loadFromPacks:error", { error: String(e) });
      return undefined;
    }
  };

  const loadFromState = async (): Promise<{ type: string; payload: Uint8Array } | undefined> => {
    try {
      const z = await limiter.run("do:getObject", async () => {
        if (!countSubrequest(cacheCtx)) {
          logger.warn("soft-budget-exhausted", { op: "do:getObject", oid: oidLc });
          return null;
        }
        return await stub.getObject(oidLc);
      });
      if (z) {
        const parsed = await inflateAndParseHeader(z instanceof Uint8Array ? z : new Uint8Array(z));
        if (parsed) {
          logger.debug("object-read", { source: "do-state", type: parsed.type });
          return { type: parsed.type, payload: parsed.payload };
        }
      } else {
        logger.debug("do-state-miss", { oid: oidLc });
      }
    } catch (e) {
      logger.debug("do:getObject:error", { error: String(e), oid: oidLc });
      return undefined;
    }
  };

  if (cacheCtx) {
    const cacheKey = buildObjectCacheKey(cacheCtx.req, repoId, oidLc);
    const bypassCacheRead = cacheCtx.memo?.flags?.has("no-cache-read") === true;
    const doLoad = async (): Promise<{ type: string; payload: Uint8Array } | undefined> => {
      if (heavyNoCache) {
        const res = await loadFromPacks();
        if (res && cacheCtx?.memo) {
          cacheCtx.memo.objects = cacheCtx.memo.objects || new Map();
          cacheCtx.memo.objects.set(oidLc, res);
        }
        return res;
      }

      const stateResult = await loadFromState();
      if (stateResult) {
        if (cacheCtx?.memo) {
          cacheCtx.memo.objects = cacheCtx.memo.objects || new Map();
          cacheCtx.memo.objects.set(oidLc, stateResult);
        }
        return stateResult;
      }

      const res = await loadFromPacks();
      if (res && cacheCtx?.memo) {
        cacheCtx.memo.objects = cacheCtx.memo.objects || new Map();
        cacheCtx.memo.objects.set(oidLc, res);
      }
      return res;
    };

    if (!bypassCacheRead) {
      const loaded = await cacheOrLoadObject(cacheKey, doLoad, cacheCtx.ctx);
      if (loaded && cacheCtx?.memo) {
        cacheCtx.memo.objects = cacheCtx.memo.objects || new Map();
        cacheCtx.memo.objects.set(oidLc, loaded);
      }
      return await withShadowValidation(loaded);
    }

    const loaded = await doLoad();
    if (loaded && !heavyNoCache) {
      try {
        const savePromise = cachePutObject(cacheKey, loaded.type, loaded.payload);
        cacheCtx.ctx?.waitUntil?.(savePromise);
      } catch {}
    }
    return await withShadowValidation(loaded);
  }

  {
    const stateResult = await loadFromState();
    if (stateResult) return await withShadowValidation(stateResult);
    return await withShadowValidation(await loadFromPacks());
  }
}
