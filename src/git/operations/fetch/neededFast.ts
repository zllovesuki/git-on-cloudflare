import type { CacheContext } from "@/cache/index.ts";
import { parseCommitRefs, parseTagTarget, parseTreeChildOids } from "@/git/core/index.ts";
import { createLogger } from "@/common/index.ts";
import { readObjectRefsBatch } from "@/git/object-store/index.ts";
import { findCommonHaves } from "../closure.ts";
import { readLooseObjectRaw } from "../read/index.ts";

export async function computeNeededFast(
  env: Env,
  repoId: string,
  wants: string[],
  haves: string[],
  cacheCtx?: CacheContext
): Promise<string[]> {
  const log = createLogger(env.LOG_LEVEL, { service: "NeededFast", repoId });
  const startTime = Date.now();

  log.debug("fast:building-stop-set", { haves: haves.length });
  const stopSet = new Set<string>();
  const timeout = 49000;

  let ackOids: string[] = [];
  if (haves.length > 0) {
    ackOids = await findCommonHaves(env, repoId, haves.slice(0, 128), cacheCtx);
    for (const oid of ackOids) {
      stopSet.add(oid.toLowerCase());
    }

    if (ackOids.length === 0) {
      log.debug("fast:no-common-base", { haves: haves.length });
    }
  }

  if (ackOids.length > 0 && ackOids.length < 10) {
    const MAINLINE_BUDGET = 20;
    const mainlineQueue = [...ackOids];
    let mainlineCount = 0;

    while (mainlineQueue.length > 0 && mainlineCount < MAINLINE_BUDGET) {
      if (Date.now() - startTime > 2000) break;

      const oid = mainlineQueue.shift()!;
      try {
        const obj = await readLooseObjectRaw(env, repoId, oid, cacheCtx);
        if (obj?.type === "commit") {
          const refs = parseCommitRefs(obj.payload);
          if (refs.parents && refs.parents.length > 0) {
            const parent = refs.parents[0];
            if (!stopSet.has(parent)) {
              stopSet.add(parent);
              mainlineQueue.push(parent);
              mainlineCount++;
            }
          }
        }
      } catch {}
    }

    log.debug("fast:mainline-enriched", { stopSize: stopSet.size, walked: mainlineCount });
  }

  const seen = new Set<string>();
  const needed = new Set<string>();
  const queue = [...wants];

  if (cacheCtx) {
    cacheCtx.memo = cacheCtx.memo || {};
    cacheCtx.memo.refs = cacheCtx.memo.refs || new Map<string, string[]>();
    cacheCtx.memo.flags = cacheCtx.memo.flags || new Set<string>();
  }

  let refsBatchCalls = 0;
  let memoRefsHits = 0;
  let compatFallbackReads = 0;

  log.info("fast:starting-closure", { wants: wants.length, stopSet: stopSet.size });

  while (queue.length > 0) {
    if (Date.now() - startTime > timeout) {
      log.warn("fast:timeout", { seen: seen.size, needed: needed.size });
      if (cacheCtx) {
        cacheCtx.memo = cacheCtx.memo || {};
        cacheCtx.memo.flags = cacheCtx.memo.flags || new Set<string>();
        cacheCtx.memo.flags.add("closure-timeout");
      }
      break;
    }

    if (cacheCtx?.memo?.flags?.has("loader-capped")) {
      log.warn("fast:loader-capped", { seen: seen.size, needed: needed.size });
      if (cacheCtx) {
        cacheCtx.memo = cacheCtx.memo || {};
        cacheCtx.memo.flags = cacheCtx.memo.flags || new Set<string>();
        cacheCtx.memo.flags.add("closure-timeout");
      }
      break;
    }

    const batchSize = Math.min(128, queue.length);
    const batch = queue.splice(0, batchSize);
    const unseenBatch = batch.filter((oid) => !seen.has(oid));

    if (unseenBatch.length === 0) continue;

    const toProcess: string[] = [];
    for (const oid of unseenBatch) {
      seen.add(oid);
      const lc = oid.toLowerCase();

      if (stopSet.has(lc)) {
        log.debug("fast:hit-stop", { oid });
        continue;
      }

      needed.add(oid);
      toProcess.push(oid);
    }

    if (toProcess.length === 0) continue;

    let refsMap: Map<string, string[]> = new Map();

    if (cacheCtx?.memo?.refs) {
      for (const oid of toProcess) {
        const lc = oid.toLowerCase();
        const cached = cacheCtx.memo.refs.get(lc);
        if (cached !== undefined) {
          refsMap.set(oid, cached);
          memoRefsHits++;
        }
      }
    }

    const toBatch = toProcess.filter((oid) => !refsMap.has(oid));
    if (toBatch.length > 0) {
      try {
        const batchMap = await readObjectRefsBatch(env, repoId, toBatch, cacheCtx);
        refsBatchCalls++;

        for (const oid of toBatch) {
          const refs = batchMap.get(oid);
          if (refs === undefined) continue;
          const lc = oid.toLowerCase();
          // An empty refs array is still a resolved answer for leaf objects.
          // Only omitted keys fall through to the compatibility shim below.
          refsMap.set(oid, refs);
          if (cacheCtx?.memo) {
            cacheCtx.memo.refs = cacheCtx.memo.refs || new Map<string, string[]>();
            cacheCtx.memo.refs.set(lc, refs);
          }
        }
      } catch (e) {
        log.debug("fast:batch-error", { error: String(e) });
      }
    }

    const stillMissing = toProcess.filter((oid) => !refsMap.has(oid));
    if (stillMissing.length > 0) {
      log.debug("fast:compat-ref-fallback", { count: stillMissing.length });
      for (const oid of stillMissing) {
        try {
          const obj = await readLooseObjectRaw(env, repoId, oid, cacheCtx);
          compatFallbackReads++;
          const refs: string[] = [];
          if (obj?.type === "commit") {
            const commitRefs = parseCommitRefs(obj.payload);
            if (commitRefs.tree) refs.push(commitRefs.tree);
            refs.push(...commitRefs.parents);
          } else if (obj?.type === "tree") {
            refs.push(...parseTreeChildOids(obj.payload));
          } else if (obj?.type === "tag") {
            const tag = parseTagTarget(obj.payload);
            if (tag?.targetOid) refs.push(tag.targetOid);
          }
          refsMap.set(oid, refs);
          if (cacheCtx?.memo) {
            cacheCtx.memo.refs = cacheCtx.memo.refs || new Map<string, string[]>();
            cacheCtx.memo.refs.set(oid.toLowerCase(), refs);
          }
        } catch (error) {
          log.debug("fast:compat-read-error", { oid, error: String(error) });
        }
      }
    }

    for (const refs of refsMap.values()) {
      for (const ref of refs) {
        if (!seen.has(ref)) {
          queue.push(ref);
        }
      }
    }
  }

  const elapsed = Date.now() - startTime;
  log.info("fast:completed", {
    needed: needed.size,
    seen: seen.size,
    stopSet: stopSet.size,
    memoHits: memoRefsHits,
    refsBatches: refsBatchCalls,
    compatFallbacks: compatFallbackReads,
    timeMs: elapsed,
  });

  return Array.from(needed);
}
