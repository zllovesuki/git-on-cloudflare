import type { CacheContext } from "@/cache/index.ts";

import {
  pktLine,
  flushPkt,
  delimPkt,
  concatChunks,
  parseCommitRefs,
  parseTreeChildOids,
  parseTagTarget,
} from "@/git/core/index.ts";
import { asBodyInit, getRepoStub, createLogger } from "@/common/index.ts";
import { streamPackFromR2, streamPackFromMultiplePacks } from "@/git/pack/assemblerStream.ts";
import { getPackCandidates } from "./packDiscovery.ts";
import { getLimiter, countSubrequest } from "./limits.ts";
import { beginClosurePhase, endClosurePhase } from "./heavyMode.ts";
import { parseFetchArgs } from "./args.ts";
import {
  findCommonHaves,
  buildUnionNeededForKeys,
  countMissingRootTreesFromWants,
} from "./closure.ts";
import { readLooseObjectRaw } from "./read.ts";

/**
 * Plan types for the streaming fetch flow
 */
type AssemblerPlan =
  | {
      type: "InitCloneUnion";
      repoId: string;
      packKeys: string[];
      needed: string[];
      wants: string[];
      ackOids: string[];
      signal?: AbortSignal;
      cacheCtx?: CacheContext;
    }
  | {
      type: "IncrementalSingle";
      repoId: string;
      packKey: string;
      needed: string[];
      ackOids: string[];
      signal?: AbortSignal;
      cacheCtx?: CacheContext;
    }
  | {
      type: "IncrementalMulti";
      repoId: string;
      packKeys: string[];
      needed: string[];
      ackOids: string[];
      signal?: AbortSignal;
      cacheCtx?: CacheContext;
    }
  | {
      type: "RepositoryNotReady";
    };

/**
 * Derive pack cap from env with clamping.
 */
function getPackCapFromEnv(env: Env): number {
  const raw = Number(env.REPO_PACKLIST_MAX ?? 20);
  const n = Number.isFinite(raw) ? Math.floor(raw) : 20;
  return Math.max(1, Math.min(100, n));
}

/**
 * Computes minimal object closure using frontier-subtract approach.
 * Starts from wants and stops when hitting a "stop set" derived from haves.
 * This avoids computing two full closures and subtracting.
 *
 * @param env - Worker environment
 * @param repoId - Repository identifier
 * @param wants - Object IDs requested by client
 * @param haves - Object IDs the client already has
 * @param cacheCtx - Optional cache context for memoization
 * @returns Array of needed object IDs
 */
async function computeNeededFast(
  env: Env,
  repoId: string,
  wants: string[],
  haves: string[],
  cacheCtx?: CacheContext
): Promise<string[]> {
  const log = createLogger(env.LOG_LEVEL, { service: "NeededFast", repoId });
  const stub = getRepoStub(env, repoId);
  const limiter = getLimiter(cacheCtx);
  const startTime = Date.now();

  // Build stop set from acknowledged haves
  log.debug("fast:building-stop-set", { haves: haves.length });
  const stopSet = new Set<string>();
  const timeout = 49000; // 49s timeout

  // Step 1: Find common haves that actually exist
  let ackOids: string[] = [];
  if (haves.length > 0) {
    ackOids = await findCommonHaves(env, repoId, haves.slice(0, 128), cacheCtx); // Cap at 128 for perf
    for (const oid of ackOids) {
      stopSet.add(oid.toLowerCase());
    }

    // If we have haves but none were acknowledged, there's no common base
    if (ackOids.length === 0) {
      log.debug("fast:no-common-base", { haves: haves.length });
    }
  }

  // Optional Step 2: Limited first-parent walk for recent haves (very small budget)
  // This enriches the stop set with a handful of recent mainline commits
  if (ackOids.length > 0 && ackOids.length < 10) {
    const MAINLINE_BUDGET = 20; // Only walk 20 commits max
    const mainlineQueue = [...ackOids];
    let mainlineCount = 0;

    while (mainlineQueue.length > 0 && mainlineCount < MAINLINE_BUDGET) {
      if (Date.now() - startTime > 2000) break; // 2s budget for mainline walk

      const oid = mainlineQueue.shift()!;
      try {
        const obj = await readLooseObjectRaw(env, repoId, oid, cacheCtx);
        if (obj?.type === "commit") {
          const refs = parseCommitRefs(obj.payload);
          if (refs.parents && refs.parents.length > 0) {
            const parent = refs.parents[0]; // First parent for mainline
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

  // Step 3: Compute closure from wants, stopping at stop set
  const seen = new Set<string>();
  const needed = new Set<string>();
  const queue = [...wants];

  // Initialize memoization structures
  if (cacheCtx) {
    cacheCtx.memo = cacheCtx.memo || {};
    cacheCtx.memo.refs = cacheCtx.memo.refs || new Map<string, string[]>();
    cacheCtx.memo.flags = cacheCtx.memo.flags || new Set<string>();
  }

  let doBatchBudget = cacheCtx?.memo?.doBatchBudget ?? 20;
  let doBatchDisabled = cacheCtx?.memo?.doBatchDisabled ?? false;
  let doBatchCalls = 0;
  let memoRefsHits = 0;
  let fallbackReads = 0;

  log.info("fast:starting-closure", { wants: wants.length, stopSet: stopSet.size });

  while (queue.length > 0) {
    // Check timeout
    if (Date.now() - startTime > timeout) {
      log.warn("fast:timeout", { seen: seen.size, needed: needed.size });
      if (cacheCtx) {
        cacheCtx.memo = cacheCtx.memo || {};
        cacheCtx.memo.flags = cacheCtx.memo.flags || new Set<string>();
        cacheCtx.memo.flags.add("closure-timeout");
      }
      break;
    }

    // Check loader cap
    if (cacheCtx?.memo?.flags?.has("loader-capped")) {
      log.warn("fast:loader-capped", { seen: seen.size, needed: needed.size });
      if (cacheCtx) {
        cacheCtx.memo = cacheCtx.memo || {};
        cacheCtx.memo.flags = cacheCtx.memo.flags || new Set<string>();
        cacheCtx.memo.flags.add("closure-timeout");
      }
      break;
    }

    // Process batch
    const batchSize = Math.min(128, queue.length);
    const batch = queue.splice(0, batchSize);
    const unseenBatch = batch.filter((oid) => !seen.has(oid));

    if (unseenBatch.length === 0) continue;

    // Mark as seen and check stop set
    const toProcess: string[] = [];
    for (const oid of unseenBatch) {
      seen.add(oid);
      const lc = oid.toLowerCase();

      // If this OID is in the stop set, don't traverse it
      if (stopSet.has(lc)) {
        log.debug("fast:hit-stop", { oid });
        continue;
      }

      needed.add(oid);
      toProcess.push(oid);
    }

    if (toProcess.length === 0) continue;

    // Get refs for objects not in stop set
    let refsMap: Map<string, string[]> = new Map();

    // Check memo cache first
    if (cacheCtx?.memo?.refs) {
      for (const oid of toProcess) {
        const lc = oid.toLowerCase();
        const cached = cacheCtx.memo.refs.get(lc);
        if (cached && cached.length >= 0) {
          // Include empty arrays (blobs)
          refsMap.set(oid, cached);
          memoRefsHits++;
        }
      }
    }

    // Batch fetch missing refs from DO
    const toBatch = toProcess.filter((oid) => !refsMap.has(oid));
    if (toBatch.length > 0 && !doBatchDisabled && doBatchBudget > 0) {
      try {
        const batchMap = await limiter.run("do:getObjectRefsBatch", async () => {
          countSubrequest(cacheCtx);
          return await stub.getObjectRefsBatch(toBatch);
        });
        doBatchBudget--;
        doBatchCalls++;

        for (const [oid, refs] of batchMap) {
          const lc = oid.toLowerCase();
          if (refs && refs.length >= 0) {
            refsMap.set(oid, refs);
            // Store in memo
            if (cacheCtx?.memo) {
              cacheCtx.memo.refs = cacheCtx.memo.refs || new Map<string, string[]>();
              cacheCtx.memo.refs.set(lc, refs);
            }
          }
        }
      } catch (e) {
        log.debug("fast:batch-error", { error: String(e) });
        doBatchDisabled = true;
      }
    }

    // Fallback for any still missing
    const stillMissing = toProcess.filter((oid) => !refsMap.has(oid));
    if (stillMissing.length > 0) {
      // Use pack-aware fallback with limited concurrency
      const CONC = 4;
      let idx = 0;
      const workers: Promise<void>[] = [];

      const fetchOne = async () => {
        while (idx < stillMissing.length) {
          const oid = stillMissing[idx++];
          fallbackReads++;

          try {
            const obj = await readLooseObjectRaw(env, repoId, oid, cacheCtx);
            if (!obj) continue;

            const refs: string[] = [];
            if (obj.type === "commit") {
              const commitRefs = parseCommitRefs(obj.payload);
              if (commitRefs.tree) refs.push(commitRefs.tree);
              if (commitRefs.parents) refs.push(...commitRefs.parents);
            } else if (obj.type === "tree") {
              const childOids = parseTreeChildOids(obj.payload);
              refs.push(...childOids);
            } else if (obj.type === "tag") {
              // Peel annotated tags to their targets
              const tagInfo = parseTagTarget(obj.payload);
              if (tagInfo?.targetOid) refs.push(tagInfo.targetOid);
            }
            // Blobs have no refs

            refsMap.set(oid, refs);
            if (cacheCtx?.memo) {
              const lc = oid.toLowerCase();
              cacheCtx.memo.refs = cacheCtx.memo.refs || new Map<string, string[]>();
              cacheCtx.memo.refs.set(lc, refs);
            }
          } catch {}
        }
      };

      for (let c = 0; c < CONC; c++) workers.push(fetchOne());
      await Promise.all(workers);
    }

    // Queue child refs
    for (const [oid, refs] of refsMap) {
      for (const ref of refs) {
        if (!seen.has(ref)) {
          queue.push(ref);
        }
      }
    }
  }

  // Update shared state
  if (cacheCtx?.memo) {
    cacheCtx.memo.doBatchBudget = doBatchBudget;
    cacheCtx.memo.doBatchDisabled = doBatchDisabled;
  }

  const elapsed = Date.now() - startTime;
  log.info("fast:completed", {
    needed: needed.size,
    seen: seen.size,
    stopSet: stopSet.size,
    memoHits: memoRefsHits,
    doBatches: doBatchCalls,
    fallbacks: fallbackReads,
    timeMs: elapsed,
  });

  return Array.from(needed);
}

/**
 * Plans the fetch operation based on wants and haves.
 * Returns a plan object that can be executed later.
 */
async function planUploadPack(
  env: Env,
  repoId: string,
  wants: string[],
  haves: string[],
  done: boolean,
  signal?: AbortSignal,
  cacheCtx?: CacheContext
): Promise<AssemblerPlan | null> {
  const log = createLogger(env.LOG_LEVEL, { service: "StreamPlan", repoId });
  const stub = getRepoStub(env, repoId);
  const doId = stub.id.toString();
  const heavy = cacheCtx?.memo?.flags?.has("no-cache-read") === true;
  const packCap = getPackCapFromEnv(env);
  const limiter = getLimiter(cacheCtx);

  // Discover pack candidates once
  const packKeys = await getPackCandidates(env, stub, doId, heavy, cacheCtx);

  // If initial clone (no haves), use union approach
  if (haves.length === 0 && packKeys.length >= 2) {
    let MAX_KEYS = Math.min(packCap, packKeys.length);
    let keys = packKeys.slice(0, MAX_KEYS);
    let unionNeeded = await buildUnionNeededForKeys(stub, keys, limiter, cacheCtx, log);

    // Quick root-tree coverage guard: if union seems insufficient, expand to full candidate window
    if (unionNeeded.length > 0) {
      try {
        const unionSet = new Set<string>(unionNeeded);
        const missingRoots = await countMissingRootTreesFromWants(
          env,
          repoId,
          wants,
          cacheCtx,
          unionSet
        );
        if (missingRoots > 0) {
          log.info("stream:plan:init-union:missing-roots", { missingRoots, keys: keys.length });
          // Expand to full window
          MAX_KEYS = packCap;
          keys = packKeys.slice(0, MAX_KEYS);
          unionNeeded = await buildUnionNeededForKeys(stub, keys, limiter, cacheCtx, log);
        }
      } catch {}
    }

    if (unionNeeded.length > 0) {
      log.info("stream:plan:init-union", { packs: keys.length, union: unionNeeded.length });
      return {
        type: "InitCloneUnion",
        repoId,
        packKeys: keys,
        needed: unionNeeded,
        wants,
        ackOids: [],
        signal,
        cacheCtx,
      };
    }
  }

  // For incremental or when union fails, compute closure using optimized algorithm
  beginClosurePhase(cacheCtx, { loaderCap: 400, doBatchBudget: 20 });
  const needed = await computeNeededFast(env, repoId, wants, haves, cacheCtx);
  endClosurePhase(cacheCtx);

  // Check for timeout
  if (cacheCtx?.memo?.flags?.has("closure-timeout")) {
    log.warn("stream:plan:closure-timeout", { needed: needed.length });

    // Try union fallback
    if (packKeys.length >= 2) {
      const MAX_KEYS = Math.min(packCap, packKeys.length);
      const keys = packKeys.slice(0, MAX_KEYS);
      const unionNeeded = await buildUnionNeededForKeys(stub, keys, limiter, cacheCtx, log);

      if (unionNeeded.length > 0) {
        const ackOids = done ? [] : await findCommonHaves(env, repoId, haves, cacheCtx);
        return {
          type: "IncrementalMulti",
          repoId,
          packKeys: keys,
          needed: unionNeeded,
          ackOids,
          signal,
          cacheCtx,
        };
      }
    }
    return null; // Will result in 503
  }

  const ackOids = done ? [] : await findCommonHaves(env, repoId, haves, cacheCtx);

  // Choose strategy based on available packs
  if (packKeys.length === 1) {
    // Single pack available
    log.info("stream:plan:single-pack", {
      packKey: packKeys[0],
      needed: needed.length,
    });

    return {
      type: "IncrementalSingle",
      repoId,
      packKey: packKeys[0],
      needed,
      ackOids,
      signal,
      cacheCtx,
    };
  } else if (packKeys.length >= 2) {
    // Multi-pack available - try single first with fallback to multi
    log.info("stream:plan:multi-pack-available", {
      packs: packKeys.length,
      needed: needed.length,
    });

    // Try single pack first (it might contain all needed objects)
    return {
      type: "IncrementalSingle",
      repoId,
      packKey: packKeys[0],
      needed,
      ackOids,
      signal,
      cacheCtx,
    };
  }

  // No packs available - this is a new/empty repository or loose-only repository
  // With hydration, all repositories should have packs. Block the fetch.
  log.warn("stream:plan:no-packs-blocking", { needed: needed.length });

  // Return a special marker to indicate the repository is not ready
  // The caller should return a 503 response
  return { type: "RepositoryNotReady" };
}

/**
 * Handles progress message multiplexing for sideband protocol.
 * Manages queuing, throttling, and ordering guarantees.
 */
class SidebandProgressMux {
  private progressMessages: string[] = [];
  private progressIdx = 0;
  private lastProgressTime = 0;
  private inProgress = false;
  private resolveFirstProgress?: () => void;
  private firstProgressPromise: Promise<void>;
  private readonly intervalMs: number;

  constructor(intervalMs = 100) {
    this.intervalMs = intervalMs;
    this.firstProgressPromise = new Promise<void>((resolve) => {
      this.resolveFirstProgress = resolve;
    });
  }

  /**
   * Add a progress message to the queue.
   */
  push(msg: string): void {
    this.progressMessages.push(msg);
    // Signal arrival of first progress message (once)
    if (this.resolveFirstProgress) {
      this.resolveFirstProgress();
      this.resolveFirstProgress = undefined;
    }
  }

  /**
   * Wait for first progress message or timeout.
   */
  async waitForFirst(timeoutMs = 20): Promise<void> {
    await Promise.race([this.firstProgressPromise, new Promise((r) => setTimeout(r, timeoutMs))]);
  }

  /**
   * Check if progress should be sent based on throttling.
   */
  shouldSendProgress(): boolean {
    const now = Date.now();
    return (
      now - this.lastProgressTime >= this.intervalMs &&
      !this.inProgress &&
      this.progressIdx < this.progressMessages.length
    );
  }

  /**
   * Send pending progress messages.
   */
  async sendPending(emitFn: (msg: string) => void): Promise<void> {
    if (this.shouldSendProgress()) {
      this.inProgress = true;
      while (this.progressIdx < this.progressMessages.length) {
        emitFn(this.progressMessages[this.progressIdx++]);
      }
      this.lastProgressTime = Date.now();
      this.inProgress = false;
    }
  }

  /**
   * Send all remaining progress messages.
   */
  sendRemaining(emitFn: (msg: string) => void): void {
    while (this.progressIdx < this.progressMessages.length) {
      emitFn(this.progressMessages[this.progressIdx++]);
    }
  }
}

/**
 * Creates a transform stream that wraps pack data in sideband-64k pkt-lines.
 * Supports progress messages on band 2 and fatal errors on band 3.
 */
function createSidebandTransform(options?: {
  onProgress?: (msg: string) => void;
  signal?: AbortSignal;
}): TransformStream<Uint8Array, Uint8Array> {
  // Max pkt-line payload for sideband-64k: 65536 - 4 (pkt-line header) - 1 (sideband byte) = 65531
  // Use 65515 for safety margin
  const maxChunk = 65515;

  return new TransformStream<Uint8Array, Uint8Array>({
    async transform(chunk, controller) {
      if (options?.signal?.aborted) {
        controller.terminate();
        return;
      }

      // Split chunk if needed and wrap in sideband channel 1
      for (let off = 0; off < chunk.byteLength; off += maxChunk) {
        const slice = chunk.subarray(off, Math.min(off + maxChunk, chunk.byteLength));
        const banded = new Uint8Array(1 + slice.byteLength);
        banded[0] = 0x01; // Channel 1: data
        banded.set(slice, 1);
        controller.enqueue(pktLine(banded));
      }
    },

    flush(controller) {
      // No-op: flush packet will be sent by executePlan after draining progress
    },
  });
}

/**
 * Sends a progress message on sideband channel 2.
 */
function emitProgress(controller: ReadableStreamDefaultController<Uint8Array>, message: string) {
  const msg = new TextEncoder().encode(message);
  const banded = new Uint8Array(1 + msg.byteLength);
  banded[0] = 0x02; // Channel 2: progress
  banded.set(msg, 1);
  controller.enqueue(pktLine(banded));
}

/**
 * Sends a fatal error on sideband channel 3.
 */
function emitFatal(controller: ReadableStreamDefaultController<Uint8Array>, message: string) {
  const msg = new TextEncoder().encode(`fatal: ${message}\n`);
  const banded = new Uint8Array(1 + msg.byteLength);
  banded[0] = 0x03; // Channel 3: fatal error
  banded.set(msg, 1);
  controller.enqueue(pktLine(banded));
}

/**
 * Builds acknowledgment section for git protocol v2.
 */
function buildAckSection(ackOids: string[], done: boolean): Uint8Array[] {
  const chunks: Uint8Array[] = [];

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

  return chunks;
}

/**
 * Resolves a pack stream based on the plan, handling fallback logic.
 */
async function resolvePackStream(
  env: Env,
  plan: Exclude<AssemblerPlan, { type: "RepositoryNotReady" }>,
  options?: {
    limiter?: { run<T>(label: string, fn: () => Promise<T>): Promise<T> };
    countSubrequest?: (n?: number) => void;
    onProgress?: (msg: string) => void;
    signal?: AbortSignal;
  }
): Promise<ReadableStream<Uint8Array> | undefined> {
  const log = createLogger(env.LOG_LEVEL, { service: "ResolvePackStream" });
  let packStream: ReadableStream<Uint8Array> | undefined;

  switch (plan.type) {
    case "InitCloneUnion":
    case "IncrementalMulti":
      packStream = await streamPackFromMultiplePacks(env, plan.packKeys, plan.needed, options);
      break;

    case "IncrementalSingle":
      packStream = await streamPackFromR2(env, plan.packKey, plan.needed, options);

      // If single pack fails, try multi-pack fallback
      if (!packStream && plan.cacheCtx) {
        const stub = getRepoStub(env, plan.repoId);
        const doId = stub.id.toString();
        const heavy = plan.cacheCtx.memo?.flags?.has("no-cache-read") === true;
        const packKeys = await getPackCandidates(env, stub, doId, heavy, plan.cacheCtx);

        if (packKeys.length >= 2) {
          const packCap = getPackCapFromEnv(env);
          const SLICE = Math.min(packCap, packKeys.length);
          log.debug("pack-stream:single-fallback-to-multi", { packs: SLICE });
          packStream = await streamPackFromMultiplePacks(
            env,
            packKeys.slice(0, SLICE),
            plan.needed,
            options
          );
        }
      }
      break;
  }

  return packStream;
}

/**
 * Pipes a pack stream through sideband encoding with progress multiplexing.
 */
async function pipePackWithSideband(
  packStream: ReadableStream<Uint8Array>,
  controller: ReadableStreamDefaultController<Uint8Array>,
  options: {
    signal?: AbortSignal;
    progressMux: SidebandProgressMux;
    log: ReturnType<typeof createLogger>;
  }
): Promise<void> {
  const { signal, progressMux, log } = options;

  try {
    // Create sideband transform
    const sidebandTransform = createSidebandTransform({ signal });
    const reader = packStream.pipeThrough(sidebandTransform).getReader();

    // Wait briefly for first progress then flush it BEFORE any packfile bytes
    await progressMux.waitForFirst();
    progressMux.sendRemaining((msg) => emitProgress(controller, msg));

    // Stream the pack data with periodic progress checks
    while (true) {
      // Check abort signal to allow early termination
      if (signal?.aborted) {
        log.debug("pipe:aborted");
        reader.cancel();
        break;
      }

      const { done, value } = await reader.read();
      if (done) break;

      // Send any pending progress messages
      await progressMux.sendPending((msg) => emitProgress(controller, msg));

      // Send the data
      controller.enqueue(value);
    }

    // Send any remaining progress messages before closing
    progressMux.sendRemaining((msg) => emitProgress(controller, msg));

    // Send final flush packet to end the packfile section
    controller.enqueue(flushPkt());
  } catch (error) {
    log.error("pipe:error", { error: String(error) });
    // Try to send fatal message if possible
    try {
      emitFatal(controller, String(error));
    } catch {}
    throw error;
  }
}

/**
 * Executes an assembler plan and returns a streaming response.
 * For internal use when plan is already computed.
 * Note: RepositoryNotReady plans should be handled before calling this function.
 */
async function executePlan(
  env: Env,
  plan: Exclude<AssemblerPlan, { type: "RepositoryNotReady" }>,
  done: boolean
): Promise<Response> {
  const log = createLogger(env.LOG_LEVEL, { service: "StreamExecute" });
  const limiter = plan.cacheCtx ? getLimiter(plan.cacheCtx) : undefined;

  // Prepare the acknowledgments block
  const ackChunks = buildAckSection(plan.ackOids || [], done);

  // Create progress multiplexer
  const progressMux = new SidebandProgressMux();

  // Get the pack stream using the helper
  const packStream = await resolvePackStream(env, plan, {
    signal: plan.signal,
    limiter,
    countSubrequest: (n?: number) => countSubrequest(plan.cacheCtx, n),
    onProgress: (msg) => progressMux.push(msg),
  });

  if (!packStream) {
    log.warn("stream:execute:no-stream", { type: plan.type });
    return new Response("Server unable to assemble pack; please retry\n", {
      status: 503,
      headers: {
        "Retry-After": "3",
        "Content-Type": "text/plain; charset=utf-8",
      },
    });
  }

  // Create the response stream
  const responseStream = new ReadableStream<Uint8Array>({
    async start(controller) {
      try {
        // Send acknowledgments block
        for (const chunk of ackChunks) {
          controller.enqueue(chunk);
        }

        // Pipe pack stream through sideband with progress
        await pipePackWithSideband(packStream, controller, {
          signal: plan.signal,
          progressMux,
          log,
        });

        controller.close();
      } catch (error) {
        log.error("stream:response:error", { error: String(error) });
        controller.error(error);
      }
    },
  });

  return new Response(responseStream, {
    status: 200,
    headers: {
      "Content-Type": "application/x-git-upload-pack-result",
      "Cache-Control": "no-cache",
    },
  });
}

// Export computeNeededFast for use by other modules
export { computeNeededFast };

/**
 * Handles Git fetch protocol v2 requests with streaming.
 * This is the main entry point for streaming fetch operations.
 */
export async function handleFetchV2Streaming(
  env: Env,
  repoId: string,
  body: Uint8Array,
  signal?: AbortSignal,
  cacheCtx?: CacheContext
): Promise<Response> {
  const { wants, haves, done } = parseFetchArgs(body);
  const log = createLogger(env.LOG_LEVEL, { service: "StreamFetchV2", repoId });

  if (signal?.aborted) {
    return new Response("client aborted\n", { status: 499 });
  }

  // No wants: respond with ack-only
  if (wants.length === 0) {
    const chunks = [pktLine("acknowledgments\n"), pktLine("NAK\n"), flushPkt()];
    return new Response(asBodyInit(concatChunks(chunks)), {
      status: 200,
      headers: {
        "Content-Type": "application/x-git-upload-pack-result",
        "Cache-Control": "no-cache",
      },
    });
  }

  // For non-done requests, send only acknowledgments (per Git v2 spec)
  // This applies regardless of whether there are haves or not
  if (!done) {
    const chunks: Uint8Array[] = [pktLine("acknowledgments\n")];

    if (haves.length > 0) {
      const ackOids = await findCommonHaves(env, repoId, haves, cacheCtx);
      log.debug("stream:fetch:negotiation", { haves: haves.length, acks: ackOids.length });

      if (ackOids.length > 0) {
        for (let i = 0; i < ackOids.length; i++) {
          const suffix = i === ackOids.length - 1 ? "ready" : "common";
          chunks.push(pktLine(`ACK ${ackOids[i]} ${suffix}\n`));
        }
      } else {
        chunks.push(pktLine("NAK\n"));
      }
    } else {
      // Even with no haves, we still send NAK in the acknowledgments section
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

  // For done=true, check for special cases first before streaming
  if (done) {
    // Quick check if repository has no packs - we need to return 503 immediately
    // This is a lightweight check before we start streaming
    const stub = getRepoStub(env, repoId);
    const doId = stub.id.toString();
    const heavy = cacheCtx?.memo?.flags?.has("no-cache-read") === true;
    const packKeys = await getPackCandidates(env, stub, doId, heavy, cacheCtx);

    // If no packs available, return 503 immediately (don't stream)
    if (packKeys.length === 0) {
      log.warn("stream:fetch:repository-not-ready");
      return new Response(
        "Repository not ready for fetch. Objects are being packed, please retry in a few moments.\n",
        {
          status: 503,
          headers: {
            "Retry-After": "5",
            "Content-Type": "text/plain; charset=utf-8",
            "X-Git-Error": "repository-not-ready",
          },
        }
      );
    }

    log.info("stream:fetch:immediate-stream", { wants: wants.length, haves: haves.length });

    // Create the response stream that does planning inside
    const responseStream = new ReadableStream<Uint8Array>({
      async start(controller) {
        const streamLog = createLogger(env.LOG_LEVEL, { service: "StreamFetchV2", repoId });
        try {
          // Start packfile section immediately
          controller.enqueue(pktLine("packfile\n"));

          // Emit initial progress
          emitProgress(controller, "remote: Preparing pack...\n");

          // Plan the operation
          const planStart = Date.now();
          const plan = await planUploadPack(env, repoId, wants, haves, done, signal, cacheCtx);

          if (!plan) {
            emitFatal(controller, "Unable to create fetch plan");
            controller.close();
            return;
          }

          // Check if repository is not ready (loose-only)
          if (plan.type === "RepositoryNotReady") {
            emitFatal(controller, "Repository not ready - objects are being packed");
            controller.close();
            return;
          }

          const planTime = Date.now() - planStart;
          streamLog.info("stream:fetch:plan-complete", { type: plan.type, timeMs: planTime });

          // Create progress multiplexer
          const progressMux = new SidebandProgressMux();

          // Now execute the plan inline (streaming the pack data)
          const limiter = plan.cacheCtx ? getLimiter(plan.cacheCtx) : undefined;

          // Get the pack stream using the helper
          const packStream = await resolvePackStream(env, plan, {
            signal: plan.signal,
            limiter,
            countSubrequest: (n?: number) => countSubrequest(plan.cacheCtx, n),
            onProgress: (msg) => progressMux.push(msg),
          });

          if (!packStream) {
            emitFatal(controller, "Unable to assemble pack");
            controller.close();
            return;
          }

          // Pipe pack stream through sideband with progress
          await pipePackWithSideband(packStream, controller, {
            signal: plan.signal,
            progressMux,
            log: streamLog,
          });

          controller.close();
        } catch (error) {
          streamLog.error("stream:response:error", { error: String(error) });
          // Try to send fatal message if possible
          try {
            emitFatal(controller, String(error));
          } catch {}
          controller.error(error);
        }
      },
    });

    return new Response(responseStream, {
      status: 200,
      headers: {
        "Content-Type": "application/x-git-upload-pack-result",
        "Cache-Control": "no-cache",
      },
    });
  }

  // For non-done (negotiation), use the old path with pre-computed plan
  log.info("stream:fetch:planning", { wants: wants.length, haves: haves.length, done });
  const plan = await planUploadPack(env, repoId, wants, haves, done, signal, cacheCtx);

  if (!plan) {
    log.error("stream:fetch:no-plan-unexpected");
    return new Response(
      "Server temporarily unable to create fetch plan. Please retry in a few moments.\n",
      {
        status: 503,
        headers: {
          "Retry-After": "5",
          "Content-Type": "text/plain; charset=utf-8",
          "X-Git-Error": "fetch-plan-failed",
        },
      }
    );
  }

  // Check if repository is not ready (loose-only)
  if (plan.type === "RepositoryNotReady") {
    log.warn("stream:fetch:repository-not-ready");
    return new Response(
      "Repository not ready for fetch. Objects are being packed, please retry in a few moments.\n",
      {
        status: 503,
        headers: {
          "Retry-After": "5",
          "Content-Type": "text/plain; charset=utf-8",
          "X-Git-Error": "repository-not-ready",
        },
      }
    );
  }

  // Execute the plan
  log.info("stream:fetch:executing", { type: plan.type });
  return await executePlan(env, plan, done);
}
