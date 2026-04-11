import type { CacheContext } from "@/cache/index.ts";

import { createLogger } from "@/common/index.ts";
import { parseCommitRefs } from "@/git/core/index.ts";
import { readObject, readObjectRefsBatch } from "@/git/object-store/index.ts";
import { findCommonHaves } from "../closure.ts";

export async function computeNeededFast(
  env: Env,
  repoId: string,
  wants: string[],
  haves: string[],
  cacheCtx?: CacheContext,
  onProgress?: (message: string) => void
): Promise<string[]> {
  const log = createLogger(env.LOG_LEVEL, { service: "NeededFast", repoId });
  const startTime = Date.now();
  const timeoutMs = 49_000;

  log.debug("fast:building-stop-set", { haves: haves.length });
  const stopSet = new Set<string>();

  let ackOids: string[] = [];
  if (haves.length > 0) {
    onProgress?.("Finding common commits...\n");
    ackOids = await findCommonHaves(env, repoId, haves, cacheCtx);
    for (const oid of ackOids) {
      stopSet.add(oid.toLowerCase());
    }

    if (ackOids.length === 0) {
      log.debug("fast:no-common-base", { haves: haves.length });
    }
  }

  onProgress?.("Selecting objects to send...\n");

  if (ackOids.length > 0 && ackOids.length < 10) {
    const mainlineBudget = 20;
    const mainlineQueue = [...ackOids];
    let walked = 0;

    while (mainlineQueue.length > 0 && walked < mainlineBudget) {
      if (Date.now() - startTime > 2_000) break;

      const oid = mainlineQueue.shift()!;
      const object = await readObject(env, repoId, oid, cacheCtx);
      if (object?.type !== "commit") continue;

      const refs = parseCommitRefs(object.payload);
      const parent = refs.parents[0];
      if (!parent || stopSet.has(parent)) continue;

      stopSet.add(parent);
      mainlineQueue.push(parent);
      walked++;
    }

    log.debug("fast:mainline-enriched", { stopSize: stopSet.size, walked });
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
  let missingRefs = 0;

  log.info("fast:starting-closure", { wants: wants.length, stopSet: stopSet.size });

  while (queue.length > 0) {
    if (Date.now() - startTime > timeoutMs) {
      log.warn("fast:timeout", { seen: seen.size, needed: needed.size });
      if (cacheCtx) {
        cacheCtx.memo = cacheCtx.memo || {};
        cacheCtx.memo.flags = cacheCtx.memo.flags || new Set<string>();
        cacheCtx.memo.flags.add("closure-timeout");
      }
      break;
    }

    const batch = queue.splice(0, Math.min(128, queue.length));
    const unseenBatch = batch.filter((oid) => !seen.has(oid));
    if (unseenBatch.length === 0) continue;

    const toProcess: string[] = [];
    for (const oid of unseenBatch) {
      seen.add(oid);
      const oidLc = oid.toLowerCase();
      if (stopSet.has(oidLc)) {
        log.debug("fast:hit-stop", { oid });
        continue;
      }

      needed.add(oid);
      toProcess.push(oid);
    }

    if (toProcess.length === 0) continue;

    const refsMap = new Map<string, string[]>();
    if (cacheCtx?.memo?.refs) {
      for (const oid of toProcess) {
        const refs = cacheCtx.memo.refs.get(oid.toLowerCase());
        if (refs === undefined) continue;
        refsMap.set(oid, refs);
        memoRefsHits++;
      }
    }

    const batchOids = toProcess.filter((oid) => !refsMap.has(oid));
    if (batchOids.length > 0) {
      try {
        const batchMap = await readObjectRefsBatch(env, repoId, batchOids, cacheCtx);
        refsBatchCalls++;

        for (const oid of batchOids) {
          const refs = batchMap.get(oid);
          if (refs === undefined) continue;

          refsMap.set(oid, refs);
          if (cacheCtx?.memo) {
            cacheCtx.memo.refs = cacheCtx.memo.refs || new Map<string, string[]>();
            cacheCtx.memo.refs.set(oid.toLowerCase(), refs);
          }
        }
      } catch (error) {
        log.debug("fast:batch-error", { error: String(error) });
      }
    }

    missingRefs += toProcess.length - refsMap.size;
    for (const refs of refsMap.values()) {
      for (const ref of refs) {
        if (!seen.has(ref)) {
          queue.push(ref);
        }
      }
    }
  }

  log.info("fast:completed", {
    needed: needed.size,
    seen: seen.size,
    stopSet: stopSet.size,
    memoHits: memoRefsHits,
    refsBatches: refsBatchCalls,
    missingRefs,
    timeMs: Date.now() - startTime,
  });

  return Array.from(needed);
}
