import { hexToBytes, createLogger, createDigestStream } from "@/common/index.ts";
import {
  readPackHeaderEx,
  readPackRange,
  readPackHeaderExFromBuf,
  encodeOfsDeltaDistance,
  mapWithConcurrency,
} from "@/git/pack/packMeta.ts";
import { loadIdxParsed } from "./idxCache.ts";

/**
 * Creates a streaming pack assembler that emits pack bytes on-the-fly.
 * This is now a simple wrapper around streamPackFromMultiplePacks for a single pack.
 *
 * @param env - Worker environment
 * @param packKey - R2 key for the .pack file
 * @param neededOids - List of object IDs the output pack must contain
 * @param options - Signal, limiter, and subrequest counter
 * @returns ReadableStream<Uint8Array> that emits pack data with SHA-1 trailer
 */
export async function streamPackFromR2(
  env: Env,
  packKey: string,
  neededOids: string[],
  options?: {
    signal?: AbortSignal;
    limiter?: { run<T>(label: string, fn: () => Promise<T>): Promise<T> };
    countSubrequest?: (n?: number) => void;
    onProgress?: (msg: string) => void;
  }
): Promise<ReadableStream<Uint8Array> | undefined> {
  // Simply delegate to the multi-pack implementation with a single pack
  return streamPackFromMultiplePacks(env, [packKey], neededOids, options);
}

/**
 * Helper function to build a PACK header.
 * @param objectCount - Number of objects in the pack
 * @returns 12-byte PACK header
 */
function buildPackHeader(objectCount: number): Uint8Array {
  const header = new Uint8Array(12);
  header.set(new TextEncoder().encode("PACK"), 0);
  const dv = new DataView(header.buffer);
  dv.setUint32(4, 2); // version
  dv.setUint32(8, objectCount);
  return header;
}

/**
 * Groups adjacent ranges for coalesced reading.
 * @param ranges - Array of ranges to group
 * @param gap - Maximum gap between ranges to coalesce (default: 8KB)
 * @param maxGroup - Maximum size of a coalesced group (default: 512KB)
 * @returns Grouped ranges
 */
function groupRanges(
  ranges: { start: number; len: number }[],
  gap: number = 8 * 1024,
  maxGroup: number = 512 * 1024
): { start: number; end: number; items: typeof ranges }[] {
  const groups: { start: number; end: number; items: typeof ranges }[] = [];
  let current: { start: number; end: number; items: typeof ranges } | null = null;

  for (const r of ranges) {
    if (!current) {
      current = { start: r.start, end: r.start + r.len, items: [r] };
      groups.push(current);
    } else {
      const gapSize = r.start - current.end;
      const newSize = r.start + r.len - current.start;
      if (gapSize <= gap && newSize <= maxGroup) {
        current.items.push(r);
        current.end = r.start + r.len;
      } else {
        current = { start: r.start, end: r.start + r.len, items: [r] };
        groups.push(current);
      }
    }
  }

  return groups;
}

// Legacy single-pack implementation removed - now using wrapper above

/**
 * Creates a streaming multi-pack assembler that emits pack bytes on-the-fly.
 * Handles delta chains across multiple packs with proper offset rewriting.
 *
 * @param env - Worker environment
 * @param packKeys - Array of R2 pack keys to use as sources
 * @param neededOids - List of object IDs the output pack must contain
 * @param options - Signal, limiter, and subrequest counter
 * @returns ReadableStream<Uint8Array> that emits pack data with SHA-1 trailer
 */
export async function streamPackFromMultiplePacks(
  env: Env,
  packKeys: string[],
  neededOids: string[],
  options?: {
    signal?: AbortSignal;
    limiter?: { run<T>(label: string, fn: () => Promise<T>): Promise<T> };
    countSubrequest?: (n?: number) => void;
    onProgress?: (msg: string) => void;
  }
): Promise<ReadableStream<Uint8Array> | undefined> {
  const log = createLogger(env.LOG_LEVEL, { service: "StreamPackAssemblerMulti" });
  const started = Date.now();
  log.debug("stream:multi:start", { packs: packKeys.length, needed: neededOids.length });

  if (options?.signal?.aborted) return undefined;

  type Meta = {
    key: string;
    oids: string[];
    offsets: number[];
    oidToIndex: Map<string, number>;
    offsetToIndex: Map<number, number>;
    packSize: number;
    nextOffset: Map<number, number>;
    wholePack?: Uint8Array;
  };

  const metas: Meta[] = [];
  const CONC = 6;
  const WHOLE_PACK_MAX = 8 * 1024 * 1024; // 8 MiB threshold for whole-pack preload

  const metaResults = await mapWithConcurrency<string, Meta | undefined>(
    packKeys,
    CONC,
    async (key) => {
      if (options?.signal?.aborted) return undefined;

      const parsed = await loadIdxParsed(env, key, options);
      const head = options?.limiter
        ? await options.limiter.run("r2:head-pack", async () => {
            options.countSubrequest?.();
            return await env.REPO_BUCKET.head(key);
          })
        : await env.REPO_BUCKET.head(key);

      if (!parsed || !head) {
        log.debug("stream:multi:missing-pack-or-idx", { key, idx: !!parsed, head: !!head });
        return undefined;
      }

      const oidToIndex = new Map<string, number>();
      for (let i = 0; i < parsed.oids.length; i++) oidToIndex.set(parsed.oids[i], i);

      const offsetToIndex = new Map<number, number>();
      for (let i = 0; i < parsed.offsets.length; i++) offsetToIndex.set(parsed.offsets[i], i);

      const sortedOffs = parsed.offsets.slice().sort((a, b) => a - b);
      const nextOffset = new Map<number, number>();
      for (let i = 0; i < sortedOffs.length; i++) {
        const cur = sortedOffs[i];
        const nxt = i + 1 < sortedOffs.length ? sortedOffs[i + 1] : head.size - 20;
        nextOffset.set(cur, nxt);
      }

      let wholePack: Uint8Array | undefined;
      if (head.size <= WHOLE_PACK_MAX) {
        try {
          const obj = options?.limiter
            ? await options.limiter.run("r2:get-pack", async () => {
                options.countSubrequest?.();
                return await env.REPO_BUCKET.get(key);
              })
            : await env.REPO_BUCKET.get(key);
          if (obj) {
            wholePack = new Uint8Array(await obj.arrayBuffer());
          }
        } catch {}
      }

      const meta: Meta = {
        key,
        oids: parsed.oids,
        offsets: parsed.offsets,
        oidToIndex,
        offsetToIndex,
        packSize: head.size,
        nextOffset,
        wholePack,
      };
      return meta;
    }
  );

  for (const m of metaResults) if (m) metas.push(m);

  if (metas.length === 0) {
    log.debug("stream:multi:no-metas", {});
    return undefined;
  }

  // Selection logic
  type Sel = { m: Meta; i: number };
  const chosen = new Map<string, Sel>();
  const selected = new Map<string, Sel>();

  const byOid = (oid: string): Sel | undefined => {
    for (const m of metas) {
      const i = m.oidToIndex.get(oid);
      if (i !== undefined) return { m, i };
    }
    return undefined;
  };

  const getChosenSel = (oid: string): Sel | undefined => {
    const x = chosen.get(oid);
    if (x) return x;
    const sel = byOid(oid);
    if (!sel) return undefined;
    chosen.set(oid, sel);
    return sel;
  };

  const pending: Sel[] = [];
  for (const oid of neededOids) {
    const sel = getChosenSel(oid);
    if (!sel) {
      log.debug("stream:multi:cannot-cover", { oid });
      return undefined;
    }
    const key = `${sel.m.key}#${sel.i}`;
    if (!selected.has(key)) selected.set(key, sel);
    pending.push(sel);
  }

  // Include delta bases
  while (pending.length) {
    if (options?.signal?.aborted) return undefined;
    const { m, i } = pending.pop()!;
    const off = m.offsets[i];

    const header = m.wholePack
      ? readPackHeaderExFromBuf(m.wholePack, off)
      : await readPackHeaderEx(env, m.key, off, options);

    if (!header) {
      log.warn("stream:multi:read-header-failed", { key: m.key, off });
      return undefined;
    }

    if (header.type === 6) {
      const baseOff = off - (header.baseRel || 0);
      const bIdx = m.offsetToIndex.get(baseOff);
      if (bIdx === undefined) return undefined;
      const baseOid = m.oids[bIdx];
      const baseSel = getChosenSel(baseOid);
      if (!baseSel) return undefined;
      const key = `${baseSel.m.key}#${baseSel.i}`;
      if (!selected.has(key)) {
        selected.set(key, baseSel);
        pending.push(baseSel);
      }
    } else if (header.type === 7) {
      const base = header.baseOid!;
      const baseSel = getChosenSel(base);
      if (!baseSel) return undefined;
      const key = `${baseSel.m.key}#${baseSel.i}`;
      if (!selected.has(key)) {
        selected.set(key, baseSel);
        pending.push(baseSel);
      }
    }
  }

  // Build nodes with headers
  type Node = Sel & {
    oid: string;
    origHeaderLen: number;
    sizeVarBytes: Uint8Array;
    type: number;
    base?: Sel;
    payloadLen: number;
  };

  const nodes: Node[] = [];
  const nodeKey = (s: Sel) => `${s.m.key}#${s.i}`;
  const nodeMap = new Map<string, Node>();

  for (const s of selected.values()) {
    const off = s.m.offsets[s.i];
    const objEnd = s.m.nextOffset.get(off)!;
    const h = s.m.wholePack
      ? readPackHeaderExFromBuf(s.m.wholePack, off)
      : await readPackHeaderEx(env, s.m.key, off, options);

    if (!h) return undefined;

    const payloadLen = objEnd - off - h.headerLen;
    const n: Node = {
      ...s,
      oid: s.m.oids[s.i],
      origHeaderLen: h.headerLen,
      sizeVarBytes: h.sizeVarBytes,
      type: h.type,
      payloadLen,
    };

    if (h.type === 6) {
      const baseOff = s.m.offsets[s.i] - (h.baseRel || 0);
      const bi = s.m.offsetToIndex.get(baseOff);
      if (bi === undefined) return undefined;
      const baseOid = s.m.oids[bi];
      const baseSel = chosen.get(baseOid) || byOid(baseOid);
      if (!baseSel) return undefined;
      n.base = baseSel;
    } else if (h.type === 7) {
      const baseOid = h.baseOid!;
      const baseSel = chosen.get(baseOid) || byOid(baseOid);
      if (!baseSel) return undefined;
      n.base = baseSel;
    }

    nodes.push(n);
    nodeMap.set(nodeKey(s), n);
  }

  // Topological sort (same as non-streaming)
  const indeg = new Map<string, number>();
  const children = new Map<string, string[]>();

  for (const n of nodes) {
    indeg.set(nodeKey(n), 0);
  }

  for (const n of nodes) {
    if (!n.base) continue;
    const bkey = nodeKey(n.base);
    indeg.set(nodeKey(n), (indeg.get(nodeKey(n)) || 0) + 1);
    const arr = children.get(bkey) || [];
    arr.push(nodeKey(n));
    children.set(bkey, arr);
  }

  const packOrder = new Map<string, number>();
  for (let pi = 0; pi < metas.length; pi++) packOrder.set(metas[pi].key, pi);

  const ready: Node[] = nodes.filter((n) => (indeg.get(nodeKey(n)) || 0) === 0);
  ready.sort(
    (a, b) =>
      packOrder.get(a.m.key)! - packOrder.get(b.m.key)! || a.m.offsets[a.i] - b.m.offsets[b.i]
  );

  const order: Node[] = [];
  while (ready.length) {
    const n = ready.shift()!;
    order.push(n);
    const arr = children.get(nodeKey(n)) || [];
    for (const ck of arr) {
      const v = indeg.get(ck)! - 1;
      indeg.set(ck, v);
      if (v === 0) ready.push(nodeMap.get(ck)!);
    }
    ready.sort(
      (a, b) =>
        packOrder.get(a.m.key)! - packOrder.get(b.m.key)! || a.m.offsets[a.i] - b.m.offsets[b.i]
    );
  }

  if (order.length !== nodes.length) {
    order.length = 0;
    const arr = Array.from(nodes);
    arr.sort(
      (a, b) =>
        packOrder.get(a.m.key)! - packOrder.get(b.m.key)! || a.m.offsets[a.i] - b.m.offsets[b.i]
    );
    order.push(...arr);
  }

  // Compute header lengths with iterative convergence
  const newHeaderLen = new Map<string, number>();
  for (const n of order) {
    if (n.type === 6) {
      const baseOff = n.base!.m.offsets[n.base!.i];
      const guessRel = n.m.offsets[n.i] - baseOff;
      newHeaderLen.set(nodeKey(n), n.sizeVarBytes.length + encodeOfsDeltaDistance(guessRel).length);
    } else if (n.type === 7) {
      newHeaderLen.set(nodeKey(n), n.sizeVarBytes.length + 20);
    } else {
      newHeaderLen.set(nodeKey(n), n.sizeVarBytes.length);
    }
  }

  let newOffsets = new Map<string, number>();
  let cur = 12;
  let iter = 0;

  while (true) {
    if (options?.signal?.aborted) return undefined;
    newOffsets = new Map<string, number>();
    cur = 12;
    for (const n of order) {
      const k = nodeKey(n);
      newOffsets.set(k, cur);
      cur += newHeaderLen.get(k)! + n.payloadLen;
    }

    let changed = false;
    for (const n of order) {
      if (n.type !== 6) continue;
      const k = nodeKey(n);
      const rel = newOffsets.get(k)! - newOffsets.get(nodeKey(n.base!))!;
      const desired = n.sizeVarBytes.length + encodeOfsDeltaDistance(rel).length;
      if (desired !== newHeaderLen.get(k)) {
        newHeaderLen.set(k, desired);
        changed = true;
      }
    }
    if (!changed || ++iter >= 16) break;
  }

  // Create the stream
  let r2PayloadGets = 0;
  let r2WholeGets = metas.filter((m) => m.wholePack).length;

  return new ReadableStream<Uint8Array>({
    async start(controller) {
      try {
        // Create a digest stream for SHA-1
        const digestStream = createDigestStream("SHA-1");
        const writer = digestStream.getWriter();

        // Helper to write and emit chunks
        const emit = async (chunk: Uint8Array) => {
          await writer.write(chunk);
          controller.enqueue(chunk);
        };

        // Write PACK header
        await emit(buildPackHeader(order.length));

        options?.onProgress?.(`Enumerating objects: ${order.length}, from ${metas.length} packs\n`);

        // Build coalesced read groups per pack
        type Range = { node: Node; start: number; len: number };
        const rangesByPack = new Map<string, Range[]>();

        for (const n of order) {
          const ranges = rangesByPack.get(n.m.key) || [];
          ranges.push({
            node: n,
            start: n.m.offsets[n.i] + n.origHeaderLen,
            len: n.payloadLen,
          });
          rangesByPack.set(n.m.key, ranges);
        }

        // Group adjacent ranges per pack for coalescing
        type Group = { packKey: string; start: number; end: number; items: Range[] };
        const allGroups = new Map<string, Group[]>();

        for (const [packKey, ranges] of rangesByPack.entries()) {
          // Sort by start position
          ranges.sort((a, b) => a.start - b.start);

          // Use our helper to group ranges
          const groupedRanges = groupRanges(ranges);
          const groups = groupedRanges.map((g) => ({
            packKey,
            ...g,
            items: g.items as Range[],
          }));

          allGroups.set(packKey, groups);
        }

        // Preload groups into cache
        const groupCache = new Map<string, Uint8Array>();

        // Stream entries
        let objectsStreamed = 0;
        const progressInterval = Math.max(1, Math.floor(order.length / 10));

        for (const n of order) {
          if (options?.signal?.aborted) {
            controller.close();
            return;
          }

          const k = nodeKey(n);

          // Write header
          await emit(n.sizeVarBytes);

          if (n.type === 6) {
            const rel = newOffsets.get(k)! - newOffsets.get(nodeKey(n.base!))!;
            const ofsBytes = encodeOfsDeltaDistance(rel);
            await emit(ofsBytes);
          } else if (n.type === 7) {
            const baseOid = n.base!.m.oids[n.base!.i];
            await emit(hexToBytes(baseOid));
          }

          // Write payload
          const payloadStart = n.m.offsets[n.i] + n.origHeaderLen;
          let payload: Uint8Array | undefined;

          if (n.m.wholePack) {
            // Use in-memory pack
            payload = n.m.wholePack.subarray(payloadStart, payloadStart + n.payloadLen);
          } else {
            // Find the group containing this payload
            const groups = allGroups.get(n.m.key);
            if (groups) {
              const group = groups.find((g) => g.start <= payloadStart && payloadStart < g.end);
              if (group) {
                const cacheKey = `${n.m.key}:${group.start}:${group.end}`;

                // Load group if not cached
                if (!groupCache.has(cacheKey)) {
                  r2PayloadGets++;
                  const groupData = await readPackRange(
                    env,
                    n.m.key,
                    group.start,
                    group.end - group.start,
                    options
                  );
                  if (!groupData) {
                    controller.error(new Error(`Failed to read range at ${group.start}`));
                    return;
                  }
                  groupCache.set(cacheKey, groupData);

                  // Clean up old cache entries if too many
                  if (groupCache.size > 10) {
                    const firstKey = groupCache.keys().next().value;
                    if (firstKey) groupCache.delete(firstKey);
                  }
                }

                // Extract from cached group data
                const groupData = groupCache.get(cacheKey)!;
                const rel = payloadStart - group.start;
                payload = groupData.subarray(rel, rel + n.payloadLen);
              }
            }

            // Fallback to individual read if not in a group
            if (!payload) {
              r2PayloadGets++;
              payload = await readPackRange(env, n.m.key, payloadStart, n.payloadLen, options);
            }
          }

          if (!payload) {
            controller.error(new Error(`Failed to read payload for ${n.oid}`));
            return;
          }

          await emit(payload);

          objectsStreamed++;
          if (objectsStreamed % progressInterval === 0 || objectsStreamed === order.length) {
            const percent = Math.round((objectsStreamed / order.length) * 100);

            if (objectsStreamed === order.length) {
              options?.onProgress?.(
                `Counting objects: 100% (${order.length}/${order.length}), done.\n`
              );
            } else {
              options?.onProgress?.(
                `Counting objects: ${percent}% (${objectsStreamed}/${order.length})\r`
              );
            }
          }
        }

        // Close writer and get digest
        await writer.close();
        const digest = await digestStream.digest;
        const sha = new Uint8Array(digest);

        // Emit SHA-1 trailer
        controller.enqueue(sha);

        log.info("stream:multi:completed", {
          objects: order.length,
          payloadGets: r2PayloadGets,
          wholeGets: r2WholeGets,
          timeMs: Date.now() - started,
        });

        controller.close();
      } catch (error) {
        log.error("stream:multi:error", { error: String(error) });
        controller.error(error);
      }
    },
  });
}
