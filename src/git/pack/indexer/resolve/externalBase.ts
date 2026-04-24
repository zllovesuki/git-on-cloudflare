import type { PackedObjectResult, PackCatalogRow } from "@/git/object-store/types.ts";
import type { IndexedPackSource, PackedObjectCandidate } from "@/git/object-store/candidates.ts";

import { bytesToHex } from "@/common/index.ts";
import { collectPackedObjectCandidates, loadIdxView } from "@/git/object-store/index.ts";
import { materializePackedObjectCandidate } from "@/git/object-store/materialize.ts";
import { readObject } from "@/git/object-store/store.ts";

import type { ResolveOptions } from "../types.ts";

import { throwIfAborted } from "./errors.ts";

function normalizeOidHex(oid: string | Uint8Array): string {
  return typeof oid === "string" ? oid.toLowerCase() : bytesToHex(oid);
}

function getExternalBaseCatalog(opts: ResolveOptions): PackCatalogRow[] | undefined {
  return opts.activeCatalog ?? opts.cacheCtx?.memo?.packCatalog;
}

async function collectExternalBaseSources(
  opts: ResolveOptions,
  catalog: PackCatalogRow[]
): Promise<IndexedPackSource[]> {
  const sources: IndexedPackSource[] = [];

  for (const pack of catalog) {
    const idx = await loadIdxView(opts.env, pack.packKey, opts.cacheCtx, pack.packBytes);
    if (!idx) continue;
    sources.push({
      packKey: pack.packKey,
      packBytes: pack.packBytes,
      idx,
    });
  }

  return sources;
}

async function materializeExternalBaseCandidate(
  opts: ResolveOptions,
  candidate: PackedObjectCandidate,
  visited: Set<string>
): Promise<PackedObjectResult | undefined> {
  return await materializePackedObjectCandidate({
    env: opts.env,
    candidate,
    limiter: opts.limiter,
    countSubrequest: opts.countSubrequest,
    log: opts.log,
    cyclePolicy: "miss",
    resolveRefBase: async (baseOid, nextVisited) => {
      return await readExternalBaseObject(opts, baseOid, nextVisited);
    },
    visited,
    signal: opts.signal,
    checkAborted: (stage) => throwIfAborted(opts.signal, opts.log, stage),
  });
}

/**
 * Resolve a thin-pack base from the caller-supplied catalog snapshot.
 *
 * `readObject()` deliberately returns the first catalog hit for an OID. Backfill
 * needs a weaker contract: when duplicate OIDs exist, a newer duplicate may be
 * an unusable REF_DELTA whose base lives in the target pack being backfilled.
 * Try every duplicate candidate in snapshot order and accept the first one
 * whose delta chain materializes without crossing back into the excluded target.
 */
export async function readExternalBaseObject(
  opts: ResolveOptions,
  oid: string | Uint8Array,
  visited = new Set<string>()
): Promise<PackedObjectResult | undefined> {
  const oidHex = normalizeOidHex(oid);
  const cached = opts.cacheCtx?.memo?.packedObjects?.get(oidHex);
  if (cached) return cached;

  const catalog = getExternalBaseCatalog(opts);
  if (!catalog) {
    return await readObject(opts.env, opts.repoId, oidHex, opts.cacheCtx, visited);
  }

  const sources = await collectExternalBaseSources(opts, catalog);
  const candidates = collectPackedObjectCandidates(sources, oid, {
    excludePackKey: opts.packKey,
  });
  opts.log.debug("resolve:external-base-candidates", {
    oid: oidHex,
    candidates: candidates.length,
    packs: sources.length,
    excludedPackKey: opts.packKey,
  });

  for (const candidate of candidates) {
    const object = await materializeExternalBaseCandidate(opts, candidate, visited);
    if (!object) continue;

    if (opts.cacheCtx?.memo) {
      opts.cacheCtx.memo.packedObjects = opts.cacheCtx.memo.packedObjects || new Map();
      opts.cacheCtx.memo.packedObjects.set(oidHex, object);
    }
    return object;
  }

  opts.log.debug("resolve:external-base-miss", {
    oid: oidHex,
    candidates: candidates.length,
  });
  return undefined;
}
