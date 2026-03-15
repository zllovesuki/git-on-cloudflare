import type { GitObjectType } from "@/git/core/index.ts";
import type { Logger } from "@/common/index.ts";
import type { CacheContext } from "@/cache/index.ts";
import type { RepoDurableObject } from "@/index.ts";

import { pktLine, flushPkt, delimPkt, concatChunks } from "@/git/core/index.ts";
import {
  getRepoStub,
  createLogger,
  createInflateStream,
  createBlobFromBytes,
  asBodyInit,
} from "@/common/index.ts";
import { assemblePackFromMultiplePacks, assemblePackFromR2 } from "@/git/pack/assembler.ts";
import { readLooseObjectRaw } from "./read.ts";
import { getPackCandidates } from "./packDiscovery.ts";
import { getLimiter, countSubrequest } from "./limits.ts";
import { beginClosurePhase, endClosurePhase } from "./heavyMode.ts";
import { buildPackV2 } from "@/git/pack/index.ts";
import { parseFetchArgs } from "./args.ts";
import {
  findCommonHaves,
  buildUnionNeededForKeys,
  countMissingRootTreesFromWants,
} from "./closure.ts";

// Helper: expand candidate packs via R2 and return an expanded sliced list up to packCap.
async function expandCandidates(
  env: Env,
  stub: DurableObjectStub<RepoDurableObject>,
  doId: string,
  heavy: boolean,
  cacheCtx: CacheContext | undefined,
  packCap: number,
  currentLen: number,
  log: Logger
): Promise<string[] | undefined> {
  try {
    const expanded = await getPackCandidates(env, stub, doId, heavy, cacheCtx, { expandR2: true });
    const SLICE2 = Math.min(packCap, expanded.length);
    if (SLICE2 > currentLen) return expanded.slice(0, SLICE2);
  } catch (e) {
    log.debug("fetch:expand-candidates-failed", { error: String(e) });
  }
  return undefined;
}

// Helper: derive pack cap from env with clamping.
function getPackCapFromEnv(env: Env): number {
  const raw = Number(env.REPO_PACKLIST_MAX ?? 20);
  const n = Number.isFinite(raw) ? Math.floor(raw) : 20;
  return Math.max(1, Math.min(100, n));
}

// Helper: try assembling from a single pack (first key, then others) with logging.
async function tryAssembleSinglePackPath(
  env: Env,
  packKeys: string[],
  needed: string[],
  options: {
    signal?: AbortSignal;
    limiter?: { run<T>(name: string, fn: () => Promise<T>): Promise<T> };
    countSubrequest?: (n?: number) => void;
  },
  log: Logger
): Promise<Uint8Array | undefined> {
  const firstKey = Array.isArray(packKeys) && packKeys.length > 0 ? packKeys[0] : undefined;
  if (!firstKey) {
    log.warn("fetch:single-pack:missing-meta", {});
  } else {
    log.info("fetch:try:single-pack", { key: firstKey, needed: needed.length });
    const assembled = await assemblePackFromR2(env, firstKey, needed, options);
    if (assembled) {
      log.info("fetch:path:single-pack", { key: firstKey });
      return assembled;
    } else {
      log.info("fetch:single-pack-miss", { key: firstKey });
    }
  }
  if (Array.isArray(packKeys)) {
    for (const k of packKeys) {
      try {
        log.info("fetch:try:single-pack:any", { key: k, needed: needed.length });
        const assembled = await assemblePackFromR2(env, k, needed, options);
        if (assembled) {
          log.info("fetch:path:single-pack:any", { key: k });
          return assembled;
        } else {
          log.info("fetch:single-pack-any-miss", { key: k });
        }
      } catch {}
    }
    log.info("fetch:single-pack-all-miss", { attempted: packKeys.length });
  }
  return undefined;
}

/**
 * @deprecated This function is deprecated in favor of the streaming implementation.
 * Use handleFetchV2Streaming from uploadStream.ts instead.
 * This buffered implementation will be removed in a future version.
 *
 * Handles Git fetch protocol v2 requests.
 * Optimizations:
 * - Initial clone union-first fast path: directly assemble a thick pack by unioning across recent packs,
 *   avoiding object-by-object closure when haves.length === 0 and hydration packs exist.
 * - Standard incremental path: compute minimal closure and assemble from single or multiple packs.
 * - Timeout fallback: safe multi-pack union when closure times out.
 * @param env - Worker environment
 * @param repoId - Repository identifier (owner/repo)
 * @param body - Raw request body containing fetch arguments
 * @param signal - Optional AbortSignal for request cancellation
 * @returns Response with packfile or acknowledgments
 */
export async function handleFetchV2(
  env: Env,
  repoId: string,
  body: Uint8Array,
  signal?: AbortSignal,
  cacheCtx?: CacheContext
) {
  const { wants, haves, done } = parseFetchArgs(body);
  const log = createLogger(env.LOG_LEVEL, { service: "FetchV2", repoId });
  const limiter = getLimiter(cacheCtx);
  if (signal?.aborted) return new Response("client aborted\n", { status: 499 });
  // Worker-side cap for how many candidate packs we union/assemble across.
  // Clamp to [1,100] and default to 20 when unset, mirroring DO-side config bounds.
  const packCap = getPackCapFromEnv(env);
  if (wants.length === 0) {
    // No wants: respond with ack-only
    const chunks = [pktLine("acknowledgments\n"), pktLine("NAK\n"), flushPkt()];
    return new Response(asBodyInit(concatChunks(chunks)), {
      status: 200,
      headers: {
        "Content-Type": "application/x-git-upload-pack-result",
        "Cache-Control": "no-cache",
      },
    });
  }

  // Per Git fetch v2: if negotiation is not complete (done=false), respond with
  // acknowledgments only and do NOT include any other sections in the same response.
  // This allows the client to continue negotiation or send a follow-up request with 'done'.
  if (!done) {
    const chunks: Uint8Array[] = [pktLine("acknowledgments\n")];
    if (haves.length > 0) {
      const ackOids = await findCommonHaves(env, repoId, haves, cacheCtx);
      log.debug("fetch:negotiation", { haves: haves.length, acks: ackOids.length });
      if (ackOids.length > 0) {
        for (let i = 0; i < ackOids.length; i++) {
          const suffix = i === ackOids.length - 1 ? "ready" : "common";
          chunks.push(pktLine(`ACK ${ackOids[i]} ${suffix}\n`));
        }
      } else {
        chunks.push(pktLine("NAK\n"));
      }
    } else {
      // No haves provided: send NAK to indicate no common base identified yet
      chunks.push(pktLine("NAK\n"));
    }
    chunks.push(flushPkt());
    return new Response(asBodyInit(concatChunks(chunks)), {
      status: 200,
      headers: {
        "Content-Type": "application/x-git-upload-pack-result",
        "Cache-Control": "no-cache",
      },
    });
  }

  // Enter heavy closure phase only when we are going to compute closure or assemble a pack
  beginClosurePhase(cacheCtx, { loaderCap: 400, doBatchBudget: 20 });

  const stub = getRepoStub(env, repoId);

  // Initial clone fast path: assemble pack from hydration packs which contain full delta chains.
  // This avoids expensive object-by-object closure computation.
  if (haves.length === 0) {
    try {
      const doId = stub.id.toString();
      const heavy = cacheCtx?.memo?.flags?.has("no-cache-read") === true;
      const packKeys = await getPackCandidates(env, stub, doId, heavy, cacheCtx);
      if (Array.isArray(packKeys) && packKeys.length >= 2) {
        const MAX_KEYS = Math.min(packCap, packKeys.length);
        let keys = packKeys.slice(0, MAX_KEYS);
        let unionNeeded = await buildUnionNeededForKeys(stub, keys, limiter, cacheCtx, log);
        // Quick root-tree coverage guard: if union seems insufficient, expand to full candidate window
        if (unionNeeded.length > 0) {
          try {
            const missingRoots = await countMissingRootTreesFromWants(
              env,
              repoId,
              wants,
              cacheCtx,
              new Set(unionNeeded)
            );
            if (missingRoots > 0 && keys.length < Math.min(packCap, packKeys.length)) {
              const SLICE = Math.min(packCap, packKeys.length);
              const moreKeys = packKeys.slice(0, SLICE);
              if (moreKeys.length > keys.length) {
                keys = moreKeys;
                unionNeeded = await buildUnionNeededForKeys(stub, keys, limiter, cacheCtx, log);
              }
              log.warn("fetch:init-union-missing-roots", { missingRoots, packs: keys.length });
            }
          } catch {}
        }
        if (unionNeeded.length > 0) {
          const mp = await assemblePackFromMultiplePacks(env, keys, unionNeeded, {
            signal,
            limiter,
            countSubrequest: (n?: number) => {
              countSubrequest(cacheCtx, n);
            },
          });
          if (mp) {
            log.info("fetch:path:init-union", { packs: keys.length, union: unionNeeded.length });
            // Exit heavy mode before streaming
            endClosurePhase(cacheCtx);
            // For done=true, send packfile immediately; ack section is omitted in respondWithPackfile
            return respondWithPackfile(mp, done, [], signal);
          }
          // One-time expand-on-miss using R2 to pull in a few more candidates and retry
          const keys2 = await expandCandidates(
            env,
            stub,
            doId,
            heavy,
            cacheCtx,
            packCap,
            keys.length,
            log
          );
          if (keys2 && keys2.length > keys.length) {
            const union2 = await buildUnionNeededForKeys(stub, keys2, limiter, cacheCtx, log);
            if (union2.length > 0) {
              const mp2 = await assemblePackFromMultiplePacks(env, keys2, union2, {
                signal,
                limiter,
                countSubrequest: (n?: number) => {
                  countSubrequest(cacheCtx, n);
                },
              });
              if (mp2) {
                log.info("fetch:path:init-union:expand-r2", {
                  packs: keys2.length,
                  union: union2.length,
                });
                endClosurePhase(cacheCtx);
                return respondWithPackfile(mp2, done, [], signal);
              }
            }
          }
        }
      }
    } catch (e) {
      log.debug("fetch:init-fastpath-failed", { error: String(e) });
    }
  }

  // Standard path: compute needed objects
  const needed = await computeNeeded(env, repoId, wants, haves, cacheCtx);
  log.debug("fetch:incremental", { closure: needed.length, haves: haves.length });

  // Exit heavy closure phase and prepare for downstream reads
  endClosurePhase(cacheCtx);

  // If closure timed out, avoid using the partial set. Try a safe multi-pack union fallback
  // that assembles a complete, thick pack from recent packs without computing tree closure.
  if (cacheCtx?.memo?.flags?.has("closure-timeout")) {
    log.warn("fetch:closure-timeout", { closure: needed.length });
    try {
      const doId = stub.id.toString();
      const heavy = cacheCtx?.memo?.flags?.has("no-cache-read") === true;
      const packKeys = await getPackCandidates(env, stub, doId, heavy, cacheCtx);
      if (packKeys.length > 0) {
        const MAX_KEYS = Math.min(packCap, packKeys.length);
        const keys = packKeys.slice(0, MAX_KEYS);
        const unionNeeded = await buildUnionNeededForKeys(stub, keys, limiter, cacheCtx, log);
        if (keys.length >= 2 && unionNeeded.length > 0) {
          const mp = await assemblePackFromMultiplePacks(env, keys, unionNeeded, {
            signal,
            limiter,
            countSubrequest: (n?: number) => {
              countSubrequest(cacheCtx, n);
            },
          });
          if (mp) {
            log.info("fetch:path:multi-pack-timeout-fallback", {
              packs: keys.length,
              union: unionNeeded.length,
            });
            const ackOids = done ? [] : await findCommonHaves(env, repoId, haves, cacheCtx);
            return respondWithPackfile(mp, done, ackOids, signal);
          }
        } else if (keys.length === 1 && unionNeeded.length > 0) {
          // Only one pack available: try a single-pack assembly as a last attempt
          const k = keys[0];
          try {
            log.info("fetch:timeout-fallback:try-single", { key: k, union: unionNeeded.length });
            const single = await assemblePackFromR2(env, k, unionNeeded, {
              signal,
              limiter,
              countSubrequest: (n?: number) => {
                countSubrequest(cacheCtx, n);
              },
            });
            if (single) {
              log.info("fetch:timeout-fallback:path-single", { key: k });
              const ackOids = done ? [] : await findCommonHaves(env, repoId, haves, cacheCtx);
              return respondWithPackfile(single, done, ackOids, signal);
            }
          } catch (e) {
            log.info("fetch:timeout-fallback:failed", { error: String(e) });
          }
        }
      }
    } catch (e) {
      log.debug("fetch:timeout-fallback-failed", { error: String(e) });
    }
    // Still no luck: ask client to retry shortly
    log.warn("fetch:timeout-fallback-503", { closure: needed.length });
    return new Response("Server busy computing object closure; please retry\n", {
      status: 503,
      headers: { "Retry-After": "3", "Content-Type": "text/plain; charset=utf-8" },
    });
  }

  // Coverage guard for initial clones: ensure root tree(s) of wanted commits are present
  // in the computed closure. If not, skip single-pack assembly (it likely won't cover)
  // but still allow multi-pack union which can include delta bases across packs.
  let skipSinglePack = false;
  if (haves.length === 0) {
    try {
      const missingRoots = await countMissingRootTreesFromWants(
        env,
        repoId,
        wants,
        cacheCtx,
        new Set(needed)
      );
      if (missingRoots > 0) {
        skipSinglePack = true;
        log.warn("fetch:coverage-guard-triggered", { missingRoots, wants: wants.length });
      }
      if (!skipSinglePack) {
        log.info("fetch:coverage-guard-pass", { wants: wants.length });
      }
    } catch (e) {
      // Don't block on guard errors; proceed with normal path
      log.debug("fetch:coverage-guard-error", { error: String(e) });
    }
  }

  // From here on, done === true; proceed to assemble a pack

  // Compute set of common haves we can ACK (limit for perf)
  const ackOids = done ? [] : await findCommonHaves(env, repoId, haves, cacheCtx);
  if (signal?.aborted) return new Response("client aborted\n", { status: 499 });

  // Try to assemble from R2 packs (single-pack first, then multi-pack union)
  const doId = stub.id.toString();
  const heavy = cacheCtx?.memo?.flags?.has("no-cache-read") === true;
  const packKeys: string[] = await getPackCandidates(env, stub, doId, heavy, cacheCtx);

  if (!skipSinglePack) {
    try {
      const assembled = await tryAssembleSinglePackPath(
        env,
        packKeys,
        needed,
        {
          signal,
          limiter,
          countSubrequest: (n?: number) => {
            countSubrequest(cacheCtx, n);
          },
        },
        log
      );
      if (assembled) return respondWithPackfile(assembled, done, ackOids, signal);
    } catch (e) {
      log.warn("fetch:single-pack:failed", { error: String(e) });
      // ignore and move on to multi-pack
    }
  } else {
    log.debug("fetch:skip-single-pack", {});
  }

  // Multi-pack union: only makes sense when we have at least 2 packs
  if (Array.isArray(packKeys) && packKeys.length >= 2) {
    try {
      const SLICE = Math.min(packCap, packKeys.length);
      log.info("fetch:try:multi-pack", { packs: SLICE, needed: needed.length });
      const mpAssembled = await assemblePackFromMultiplePacks(
        env,
        packKeys.slice(0, SLICE),
        needed,
        {
          signal,
          limiter,
          countSubrequest: (n?: number) => {
            countSubrequest(cacheCtx, n);
          },
        }
      );
      if (mpAssembled) {
        log.info("fetch:path:multi-pack", { packs: SLICE });
        return respondWithPackfile(mpAssembled, done, ackOids, signal);
      } else {
        log.info("fetch:multi-pack-failed", { packs: SLICE });
        // One-time expand-on-miss using R2 and retry
        const keys2 = await expandCandidates(env, stub, doId, heavy, cacheCtx, packCap, SLICE, log);
        if (keys2 && keys2.length > SLICE) {
          log.info("fetch:try:multi-pack:expand-r2", {
            packs: keys2.length,
            needed: needed.length,
          });
          const mp2 = await assemblePackFromMultiplePacks(env, keys2, needed, {
            signal,
            limiter,
            countSubrequest: (n?: number) => {
              countSubrequest(cacheCtx, n);
            },
          });
          if (mp2) {
            log.info("fetch:path:multi-pack:expand-r2", { packs: keys2.length });
            return respondWithPackfile(mp2, done, ackOids, signal);
          }
        }
      }
    } catch {
      // fall through
    }
  }

  // Note: avoid streaming raw packs as a last resort because packs may be thin
  // (contain REF_DELTA with bases outside the pack) which breaks clients. We prefer
  // multi-pack assembly or the loose fallback below.

  // Fallback: build a minimal pack from loose objects for non-initial clones.
  // Strategy: If many objects are needed, avoid a huge DO batch (which can exceed subrequest limits
  // inside the DO). Instead, try pack-first via readLooseObjectRaw with a small concurrency so pack
  // files are reused across OIDs within this request.
  log.debug("fetch:fallback-loose-objects", { count: needed.length });
  const oids = needed;
  const objs: { type: GitObjectType; payload: Uint8Array }[] = [];
  const found = new Set<string>();

  if (oids.length <= 200) {
    // Small batch: DO-side reads first (cheap and consistent), then pack path for the rest
    let dataMap: Map<string, Uint8Array | null>;
    try {
      dataMap = await limiter.run("do:getObjectsBatch", async () => {
        countSubrequest(cacheCtx);
        return await stub.getObjectsBatch(oids);
      });
    } catch (e) {
      log.error("fetch:batch-read-error", { error: String(e), batch: oids.length });
      dataMap = new Map(oids.map((oid) => [oid, null] as const));
    }

    // Decompress and parse headers with small concurrency
    const CONC = 6;
    let idx = 0;
    const work: Promise<void>[] = [];
    const parseOne = async () => {
      while (idx < oids.length) {
        const j = idx++;
        const oid = oids[j];
        const z = dataMap.get(oid) || null;
        if (!z) continue; // skip; we'll try pack path later
        try {
          const stream = createBlobFromBytes(z).stream().pipeThrough(createInflateStream());
          const raw = new Uint8Array(await new Response(stream).arrayBuffer());
          // header: <type> <len>\0
          let p = 0;
          while (p < raw.length && raw[p] !== 0x20) p++;
          const type = new TextDecoder().decode(raw.subarray(0, p)) as GitObjectType;
          let nul = p + 1;
          while (nul < raw.length && raw[nul] !== 0x00) nul++;
          const payload = raw.subarray(nul + 1);
          objs.push({ type, payload });
          found.add(oid);
        } catch (e) {
          log.warn("fetch:decompress-failed", { oid, error: String(e) });
        }
      }
    };
    for (let c = 0; c < CONC; c++) work.push(parseOne());
    await Promise.all(work);
  }

  // Pack-first for remaining or large batches
  const needPack = oids.filter((oid) => !found.has(oid));
  if (needPack.length > 0) {
    const CONC2 = 4; // keep small to avoid subrequest bursts
    let mIdx = 0;
    const workers: Promise<void>[] = [];
    const runOne = async () => {
      while (mIdx < needPack.length) {
        const k = mIdx++;
        const oid = needPack[k];
        try {
          const o = await readLooseObjectRaw(env, repoId, oid, cacheCtx);
          if (!o) {
            log.warn("fetch:missing-object", { oid });
            continue;
          }
          objs.push({ type: o.type as GitObjectType, payload: o.payload });
          found.add(oid);
        } catch (e) {
          log.warn("fetch:read-pack-missing", { oid, error: String(e) });
        }
      }
    };
    for (let c = 0; c < CONC2; c++) workers.push(runOne());
    await Promise.all(workers);
  }

  // For small leftover only, try one more DO batch to fill gaps
  const leftover = oids.filter((oid) => !found.has(oid));
  if (leftover.length > 0 && leftover.length <= 200) {
    try {
      const more = await limiter.run("do:getObjectsBatch", async () => {
        countSubrequest(cacheCtx);
        return await stub.getObjectsBatch(leftover);
      });
      for (const [oid, z] of more) {
        if (!z) continue;
        try {
          const stream = createBlobFromBytes(z).stream().pipeThrough(createInflateStream());
          const raw = new Uint8Array(await new Response(stream).arrayBuffer());
          let p = 0;
          while (p < raw.length && raw[p] !== 0x20) p++;
          const type = new TextDecoder().decode(raw.subarray(0, p)) as GitObjectType;
          let nul = p + 1;
          while (nul < raw.length && raw[nul] !== 0x00) nul++;
          const payload = raw.subarray(nul + 1);
          objs.push({ type, payload });
          found.add(oid);
        } catch {}
      }
    } catch (e) {
      log.error("fetch:batch-read-error", { error: String(e), batch: leftover.length });
    }
  }

  if (found.size !== oids.length) {
    log.error("fetch:loose-incomplete", { have: found.size, need: oids.length });
    log.warn("fetch:loose-incomplete-503", { have: found.size, need: oids.length });
    return new Response("Server busy assembling pack; please retry\n", {
      status: 503,
      headers: {
        "Retry-After": "3",
        "Content-Type": "text/plain; charset=utf-8",
      },
    });
  }
  const packfile = await buildPackV2(objs);
  log.info("fetch:loose-pack-success", { bytes: packfile.byteLength, objects: objs.length });
  return respondWithPackfile(packfile, done, ackOids, signal);
}

/**
 * @deprecated This function is deprecated in favor of the streaming implementation.
 * Use the streaming response builder from uploadStream.ts instead.
 * This buffered implementation will be removed in a future version.
 *
 * Constructs a Git protocol v2 response with packfile data.
 * Formats the response with proper pkt-line encoding and acknowledgments.
 * @param packfile - The assembled pack data
 * @param done - Whether the client sent 'done' (no negotiation needed)
 * @param ackOids - Object IDs to acknowledge as common
 * @param signal - Optional AbortSignal for streaming cancellation
 * @returns Response with properly formatted Git protocol v2 packfile
 */
export function respondWithPackfile(
  packfile: Uint8Array,
  done: boolean,
  ackOids: string[],
  signal?: AbortSignal
) {
  const chunks: Uint8Array[] = [];
  // Only include acknowledgments block when continuing negotiation (!done)
  // The protocol expects acknowledgments only during negotiation, not for initial clones with done=true
  if (!done) {
    chunks.push(pktLine("acknowledgments\n"));
    if (ackOids && ackOids.length > 0) {
      for (let i = 0; i < ackOids.length; i++) {
        const oid = ackOids[i];
        const suffix = i === ackOids.length - 1 ? "ready" : "common";
        chunks.push(pktLine(`ACK ${oid} ${suffix}\n`));
      }
    } else {
      chunks.push(pktLine("NAK\n"));
    }
    chunks.push(delimPkt());
  }
  chunks.push(pktLine("packfile\n"));
  // Max pkt-line payload for sideband-64k: 65536 - 4 (pkt-line header) - 1 (sideband byte) = 65531
  // Use 65515 for safety margin
  const maxChunk = 65515;
  for (let off = 0; off < packfile.byteLength; off += maxChunk) {
    if (signal?.aborted) return new Response("client aborted\n", { status: 499 });
    const slice = packfile.subarray(off, Math.min(off + maxChunk, packfile.byteLength));
    const banded = new Uint8Array(1 + slice.byteLength);
    banded[0] = 0x01;
    banded.set(slice, 1);
    chunks.push(pktLine(banded));
  }
  chunks.push(flushPkt());
  return new Response(asBodyInit(concatChunks(chunks)), {
    status: 200,
    headers: {
      "Content-Type": "application/x-git-upload-pack-result",
      "Cache-Control": "no-cache",
    },
  });
}

/**
 * Collects the complete object closure starting from root commits.
 * Traverses commit trees and includes all reachable objects.
 * @param env - Worker environment
 * @param repoId - Repository identifier
 * @param roots - Starting commit OIDs
 * @returns Array of all reachable object OIDs
 */
async function collectClosure(
  env: Env,
  repoId: string,
  roots: string[],
  cacheCtx?: CacheContext
): Promise<string[]> {
  const stub = getRepoStub(env, repoId);
  const limiter = getLimiter(cacheCtx);
  const seen = new Set<string>();
  const queue = [...roots];
  const log = createLogger(env.LOG_LEVEL, { service: "CollectClosure", repoId });
  // Prevent thousands of Cache API reads during traversal; we will still write results.
  if (cacheCtx) {
    cacheCtx.memo = cacheCtx.memo || {};
    cacheCtx.memo.flags = cacheCtx.memo.flags || new Set<string>();
    cacheCtx.memo.flags.add("no-cache-read");
  }
  const startTime = Date.now();
  const timeout = 49000; // Bump to 49s to avoid premature abort under production wall times
  // DO RPC budget: each getObjectRefsBatch() is a DO subrequest. Cap it per worker request.
  // In heavy mode, avoid DO refs batches entirely and rely on pack-based fallback.
  const heavy = cacheCtx?.memo?.flags?.has("no-cache-read") === true;
  // Aggregated stats for info-level logging
  let memoRefsHitsTotal = 0;
  let doBatchCalls = 0;
  let doBatchRefsTotal = 0;
  let fallbackReadsTotal = 0;
  let fallbackResolvedTotal = 0;
  let fallbackBlobHints = 0;
  // Initialize per-request shared budget and refs memo
  if (cacheCtx) {
    cacheCtx.memo = cacheCtx.memo || {};
    cacheCtx.memo.refs = cacheCtx.memo.refs || new Map<string, string[]>();
  }
  let doBatchBudget = cacheCtx?.memo?.doBatchBudget ?? (heavy ? 16 : 20);
  let doBatchDisabled = cacheCtx?.memo?.doBatchDisabled ?? false;

  // Use batch API for much faster object traversal
  while (queue.length > 0) {
    // If DO-backed loose loader has been capped, stop the closure early.
    if (cacheCtx?.memo?.flags?.has("loader-capped")) {
      log.warn("collectClosure:loader-capped-stop", { seen: seen.size, queued: queue.length });
      if (cacheCtx) {
        cacheCtx.memo = cacheCtx.memo || {};
        cacheCtx.memo.flags = cacheCtx.memo.flags || new Set<string>();
        cacheCtx.memo.flags.add("closure-timeout");
      }
      break;
    }
    if (Date.now() - startTime > timeout) {
      log.warn("collectClosure:timeout", { seen: seen.size, queued: queue.length });
      // Record in memo so caller can decide to abort the fetch rather than send partial sets
      if (cacheCtx) {
        cacheCtx.memo = cacheCtx.memo || {};
        cacheCtx.memo.flags = cacheCtx.memo.flags || new Set<string>();
        cacheCtx.memo.flags.add("closure-timeout");
      }
      break;
    }

    // Process larger batches with the new batch API
    const batchSize = Math.min(256, queue.length);
    const batch = queue.splice(0, batchSize);

    // Filter out already seen objects
    const unseenBatch = batch.filter((oid) => !seen.has(oid));
    if (unseenBatch.length === 0) continue;

    // Mark as seen
    for (const oid of unseenBatch) {
      seen.add(oid);
    }

    try {
      // Seed refsMap from memo.refs for any already known OIDs
      let refsMap: Map<string, string[]> = new Map();
      if (cacheCtx?.memo?.refs) {
        for (const oid of unseenBatch) {
          const lc = oid.toLowerCase();
          const cached = cacheCtx.memo.refs.get(lc);
          if (cached && cached.length > 0) {
            refsMap.set(oid, cached);
            memoRefsHitsTotal++;
          }
        }
      }

      // Probe DO batch only for OIDs missing in memo and budget allows
      const toBatch = unseenBatch.filter((oid) => !refsMap.has(oid));
      if (toBatch.length > 0 && !doBatchDisabled && doBatchBudget > 0) {
        try {
          const t0 = Date.now();
          const batchMap = await limiter.run("do:getObjectRefsBatch", async () => {
            countSubrequest(cacheCtx);
            return await stub.getObjectRefsBatch(toBatch);
          });
          doBatchBudget--;
          doBatchCalls++;
          log.info("collectClosure:do-batch", {
            count: toBatch.length,
            timeMs: Date.now() - t0,
          });
          for (const [oid, refs] of batchMap) {
            const lc = oid.toLowerCase();
            const memoArr = cacheCtx?.memo?.refs?.get(lc);
            if (refs && refs.length > 0) {
              // Normal case: DO parsed commit/tree and returned refs
              refsMap.set(oid, refs);
              doBatchRefsTotal += refs.length;
              if (cacheCtx?.memo) {
                cacheCtx.memo.refs = cacheCtx.memo.refs || new Map<string, string[]>();
                cacheCtx.memo.refs.set(lc, refs);
              }
            } else if (Array.isArray(memoArr) && memoArr.length === 0) {
              // We previously pre-marked this OID as a blob via tree parsing; treat as resolved leaf
              refsMap.set(oid, []);
              if (cacheCtx?.memo) {
                cacheCtx.memo.refs = cacheCtx.memo.refs || new Map<string, string[]>();
                cacheCtx.memo.refs.set(lc, []);
              }
            } else {
              // Empty without prior blob hint: leave unresolved so fallback can parse commit/tree from packs
              // Do not set refsMap here; it will be included in `missing` below.
            }
          }
        } catch (e) {
          log.error("collectClosure:batch-error", {
            error: String(e),
            batchSize: toBatch.length,
            seen: seen.size,
          });
          // Disable further DO batches for this request and fall back
          doBatchDisabled = true;
        }
      }

      // Identify objects we still need to resolve client-side
      const missing: string[] = [];
      for (const oid of unseenBatch) {
        if (!refsMap.has(oid)) missing.push(oid);
      }

      // Worker-side fallback: resolve refs via readLooseObjectRaw for missing items
      if (missing.length > 0) {
        log.debug("collectClosure:fallback-missing", { count: missing.length });
        fallbackReadsTotal += missing.length;
        const CONC = heavy ? 4 : 6;
        let mIdx = 0;
        const workers: Promise<void>[] = [];
        const runOne = async () => {
          const td = new TextDecoder();
          while (mIdx < missing.length) {
            if (cacheCtx?.memo?.flags?.has("loader-capped")) return;
            const i = mIdx++;
            const oid = missing[i];
            try {
              const obj = await readLooseObjectRaw(env, repoId, oid, cacheCtx);
              if (!obj) continue;
              const outRefs: string[] = [];
              if (obj.type === "commit") {
                const text = td.decode(obj.payload);
                const m = text.match(/^tree ([0-9a-f]{40})/m);
                if (m) outRefs.push(m[1]);
                for (const pm of text.matchAll(/^parent ([0-9a-f]{40})/gm)) outRefs.push(pm[1]);
              } else if (obj.type === "tree") {
                let i2 = 0;
                const buf = obj.payload;
                while (i2 < buf.length) {
                  let sp = i2;
                  while (sp < buf.length && buf[sp] !== 0x20) sp++;
                  if (sp >= buf.length) break;
                  let nul = sp + 1;
                  while (nul < buf.length && buf[nul] !== 0x00) nul++;
                  if (nul + 20 > buf.length) break;
                  // mode is ascii before the space; tree mode is "40000"
                  const mode = td.decode(buf.subarray(i2, sp));
                  const oidBytes = buf.subarray(nul + 1, nul + 21);
                  const oidHex = [...oidBytes].map((b) => b.toString(16).padStart(2, "0")).join("");
                  outRefs.push(oidHex);
                  // Pre-mark blobs (non-tree) as resolved (no refs) to avoid fallback reads later
                  if (mode !== "40000" && cacheCtx?.memo) {
                    const lc = oidHex.toLowerCase();
                    cacheCtx.memo.refs = cacheCtx.memo.refs || new Map<string, string[]>();
                    if (!cacheCtx.memo.refs.has(lc)) {
                      cacheCtx.memo.refs.set(lc, []);
                      fallbackBlobHints++;
                    }
                  }
                  i2 = nul + 21;
                }
              }
              if (outRefs.length > 0) {
                refsMap.set(oid, outRefs);
                fallbackResolvedTotal++;
                // Persist parsed refs to memo for reuse across closures
                if (cacheCtx?.memo) {
                  const lc = oid.toLowerCase();
                  cacheCtx.memo.refs = cacheCtx.memo.refs || new Map<string, string[]>();
                  cacheCtx.memo.refs.set(lc, outRefs);
                }
              }
            } catch {}
          }
        };
        for (let c = 0; c < CONC; c++) workers.push(runOne());
        await Promise.all(workers);
      }

      // Queue all referenced objects (including those resolved via fallback)
      for (const [oid, refs] of refsMap) {
        for (const ref of refs) {
          if (!seen.has(ref)) {
            queue.push(ref);
          }
        }
      }

      // Yield control periodically to avoid blocking
      if (seen.size % 500 === 0) {
        await new Promise((r) => setTimeout(r, 0));
      }
      // Persist DO batch state back to memo for subsequent closures in this request
      if (cacheCtx?.memo) {
        cacheCtx.memo.doBatchBudget = doBatchBudget;
        cacheCtx.memo.doBatchDisabled = doBatchDisabled;
      }
    } catch {}
  }

  log.info("collectClosure:complete", {
    objects: seen.size,
    timeMs: Date.now() - startTime,
    heavy,
    doBatchCalls,
    doBatchRefs: doBatchRefsTotal,
    doBatchBudget,
    doBatchDisabled,
    memoRefsHits: memoRefsHitsTotal,
    fallbackReads: fallbackReadsTotal,
    fallbackResolved: fallbackResolvedTotal,
    fallbackBlobHints,
    loaderCalls: cacheCtx?.memo?.loaderCalls,
    timedOut: cacheCtx?.memo?.flags?.has("closure-timeout") === true,
  });
  return Array.from(seen);
}

/**
 * @deprecated This function is deprecated in favor of computeNeededFast from uploadStream.ts.
 * The new implementation uses frontier-subtract approach for better performance.
 * This buffered implementation will be removed in a future version.
 *
 * Computes the minimal set of objects needed by the client.
 * Uses closure calculation to find all objects reachable from wants but not from haves.
 * @param env - Worker environment
 * @param repoId - Repository identifier
 * @param wants - Client's wanted commit OIDs
 * @param haves - Client's existing commit OIDs
 * @returns Array of object OIDs needed by the client
 */
export async function computeNeeded(
  env: Env,
  repoId: string,
  wants: string[],
  haves: string[],
  cacheCtx?: CacheContext
): Promise<string[]> {
  const wantSet = new Set(await collectClosure(env, repoId, wants, cacheCtx));
  const haveRoots = haves.slice(0, 128);
  if (haveRoots.length > 0) {
    const haveSet = new Set(await collectClosure(env, repoId, haveRoots, cacheCtx));
    for (const oid of haveSet) wantSet.delete(oid);
  }
  return Array.from(wantSet);
}
