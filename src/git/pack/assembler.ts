import { packIndexKey } from "@/keys.ts";
import { asBufferSource, hexToBytes, createLogger } from "@/common/index.ts";
import {
  encodeOfsDeltaDistance,
  mapWithConcurrency,
  readPackHeaderEx,
  readPackHeaderExFromBuf,
  readPackRange,
} from "@/git/pack/packMeta.ts";
import { loadIdxParsed } from "./idxCache.ts";

/**
 * @deprecated This function is deprecated in favor of streamPackFromR2 from assemblerStream.ts.
 * The streaming implementation reduces memory usage and supports backpressure.
 * This buffered implementation will be removed in a future version.
 *
 * Assembles a pack from a single pack file in R2.
 * @param env - Worker environment
 * @param packKey - R2 key for the .pack file
 * @param neededOids - List of object IDs the output pack must contain
 * @param options - Signal, limiter, and subrequest counter
 * @returns Assembled pack bytes
 */
export async function assemblePackFromR2(
  env: Env,
  packKey: string,
  neededOids: string[],
  options?: {
    signal?: AbortSignal;
    limiter?: { run<T>(label: string, fn: () => Promise<T>): Promise<T> };
    countSubrequest?: (n?: number) => void;
  }
): Promise<Uint8Array | undefined> {
  const log = createLogger(env.LOG_LEVEL, { service: "PackAssembler", repoId: packKey });
  const started = Date.now();
  let r2Gets = 0;
  log.debug("single:start", { needed: neededOids.length, packKey });
  if (options?.signal?.aborted) return undefined;
  // Fetch and parse .idx for this pack
  const parsed = await loadIdxParsed(env, packKey, options);
  if (!parsed) {
    const idxKey = packIndexKey(packKey);
    log.info("single:no-idx", { idxKey });
    return undefined;
  }
  const { oids, offsets } = parsed;
  const oidToIndex = new Map<string, number>();
  for (let i = 0; i < oids.length; i++) oidToIndex.set(oids[i], i);
  const offsetToIndex = new Map<number, number>();
  for (let i = 0; i < offsets.length; i++) offsetToIndex.set(offsets[i], i);

  // Ensure all needed objects are present in this pack
  for (const oid of neededOids)
    if (!oidToIndex.has(oid)) {
      log.info("single:missing-oid", { oid });
      return undefined;
    }

  // Build mapping of entry -> header info and payload length
  const headResp = options?.limiter
    ? await options.limiter.run("r2:head-pack", async () => {
        options.countSubrequest?.();
        return await env.REPO_BUCKET.head(packKey);
      })
    : await env.REPO_BUCKET.head(packKey);
  if (!headResp) {
    log.info("single:no-pack", { packKey });
    return undefined;
  }
  const packSize = headResp.size;
  // Heuristic: for small packs or when we need many objects, load the entire pack once.
  const shouldLoadWholePack =
    packSize <= 16 * 1024 * 1024 || neededOids.length >= oids.length * 0.25;
  const wholePack: Uint8Array | undefined = shouldLoadWholePack
    ? (r2Gets++,
      new Uint8Array(
        await (await (options?.limiter
          ? options.limiter.run("r2:get-pack", async () => {
              options.countSubrequest?.();
              return await env.REPO_BUCKET.get(packKey);
            })
          : env.REPO_BUCKET.get(packKey)))!.arrayBuffer()
      ))
    : undefined;
  if (wholePack) log.debug("single:fast-path:whole-pack-loaded", { bytes: wholePack.byteLength });
  const sortedOffs = offsets.slice().sort((a, b) => a - b);
  const nextOffset = new Map<number, number>();
  for (let i = 0; i < sortedOffs.length; i++) {
    const cur = sortedOffs[i];
    // Exclude 20-byte SHA-1 trailer from last object's end
    const nxt = i + 1 < sortedOffs.length ? sortedOffs[i + 1] : packSize - 20;
    nextOffset.set(cur, nxt);
  }

  type Entry = {
    index: number;
    oid: string;
    origOffset: number;
    origHeaderLen: number;
    sizeVarBytes: Uint8Array;
    type: number;
    baseOid?: string;
    baseIndex?: number;
    payloadLen: number;
  };

  const selected = new Set<number>();
  for (const oid of neededOids) selected.add(oidToIndex.get(oid)!);

  // Read headers and include delta bases
  const pending: number[] = Array.from(selected);
  const entries = new Map<number, Entry>();
  let externalRefDelta = false; // REF_DELTA base outside this pack -> would produce a thin pack
  while (pending.length) {
    if (options?.signal?.aborted) return undefined;
    const idx = pending.pop()!;
    if (entries.has(idx)) continue;
    const off = offsets[idx];
    const objEnd = nextOffset.get(off)!;
    const header = wholePack
      ? readPackHeaderExFromBuf(wholePack, off)
      : await readPackHeaderEx(env, packKey, off, options);
    if (!header) {
      log.warn("single:read-header-failed", { off });
      return undefined;
    }
    const payloadLen = objEnd - off - header.headerLen;
    const ent: Entry = {
      index: idx,
      oid: oids[idx],
      origOffset: off,
      origHeaderLen: header.headerLen,
      sizeVarBytes: header.sizeVarBytes,
      type: header.type,
      baseOid: header.baseOid,
      baseIndex: header.baseOid ? oidToIndex.get(header.baseOid) : undefined,
      payloadLen,
    };
    entries.set(idx, ent);
    if (header.type === 6) {
      // OFS_DELTA
      const baseOff = off - (header.baseRel || 0);
      const bIdx = offsetToIndex.get(baseOff);
      // Record base index for OFS deltas so we can compute accurate new distances
      if (bIdx !== undefined) ent.baseIndex = bIdx;
      if (bIdx !== undefined && !selected.has(bIdx)) {
        selected.add(bIdx);
        pending.push(bIdx);
      }
    } else if (header.type === 7) {
      // REF_DELTA
      if (ent.baseIndex !== undefined && !selected.has(ent.baseIndex)) {
        selected.add(ent.baseIndex);
        pending.push(ent.baseIndex);
      } else if (ent.baseIndex === undefined) {
        // Base oid is not in this pack -> thin source pack
        externalRefDelta = true;
      }
    }
  }

  if (externalRefDelta) {
    log.info("single:thin-pack-detected", { packKey });
    return undefined; // signal caller to use multi-pack assembly
  }

  // Determine order by original offsets
  const order = Array.from(selected.values()).sort((a, b) => offsets[a] - offsets[b]);
  // Iteratively recompute OFS varint lengths and offsets until stable (like multi-pack path)
  const newHeaderLen = new Map<number, number>();
  for (const i of order) {
    if (options?.signal?.aborted) return undefined;
    const e = entries.get(i)!;
    if (e.type === 6) {
      // Initial guess using original distance
      const origBaseOff = e.baseIndex !== undefined ? offsets[e.baseIndex] : 0;
      const guessRel = offsets[i] - origBaseOff;
      newHeaderLen.set(i, e.sizeVarBytes.length + encodeOfsDeltaDistance(guessRel).length);
    } else if (e.type === 7) {
      newHeaderLen.set(i, e.sizeVarBytes.length + 20);
    } else {
      newHeaderLen.set(i, e.sizeVarBytes.length);
    }
  }

  let newOffsets = new Map<number, number>();
  let cur = 12; // after PACK header
  let iter = 0;
  while (true) {
    if (options?.signal?.aborted) return undefined;
    // Compute offsets based on current header lengths
    newOffsets = new Map<number, number>();
    cur = 12;
    for (const i of order) {
      newOffsets.set(i, cur);
      cur += newHeaderLen.get(i)! + entries.get(i)!.payloadLen;
    }
    // Re-evaluate OFS varints with accurate distances
    let changed = false;
    for (const i of order) {
      const e = entries.get(i)!;
      if (e.type !== 6 || e.baseIndex === undefined) continue;
      const rel = newOffsets.get(i)! - newOffsets.get(e.baseIndex)!;
      const desired = e.sizeVarBytes.length + encodeOfsDeltaDistance(rel).length;
      if (desired !== newHeaderLen.get(i)) {
        newHeaderLen.set(i, desired);
        changed = true;
      }
    }
    if (!changed || ++iter >= 16) break; // converge or cap iterations
  }

  // Recompute exact header lengths using final offsets for consistency and exact allocation
  const finalHeaderLen = new Map<number, number>();
  for (const i of order) {
    const e = entries.get(i)!;
    if (e.type === 6 && e.baseIndex !== undefined) {
      const rel = newOffsets.get(i)! - newOffsets.get(e.baseIndex)!;
      finalHeaderLen.set(i, e.sizeVarBytes.length + encodeOfsDeltaDistance(rel).length);
    } else if (e.type === 7) {
      finalHeaderLen.set(i, e.sizeVarBytes.length + 20);
    } else {
      finalHeaderLen.set(i, e.sizeVarBytes.length);
    }
  }
  // Recompute final offsets using the final header lengths
  newOffsets = new Map<number, number>();
  let acc = 12;
  for (const i of order) {
    newOffsets.set(i, acc);
    acc += (finalHeaderLen.get(i) || 0) + entries.get(i)!.payloadLen;
  }
  const finalSize = acc;
  // Update header lengths map to the final values
  for (const [k, v] of finalHeaderLen) newHeaderLen.set(k, v);

  // Write output using exact final size
  const body = new Uint8Array(finalSize);
  // PACK header
  body.set(new TextEncoder().encode("PACK"), 0);
  const dv = new DataView(body.buffer);
  dv.setUint32(4, 2); // version
  dv.setUint32(8, order.length);

  // Fill entries
  if (wholePack) {
    for (const i of order) {
      const e = entries.get(i)!;
      let p = newOffsets.get(i)!;
      // size varint (includes type bits)
      body.set(e.sizeVarBytes, p);
      p += e.sizeVarBytes.length;
      if (e.type === 6) {
        const rel = newOffsets.get(i)! - newOffsets.get(e.baseIndex!)!;
        const ofsBytes = encodeOfsDeltaDistance(rel);
        body.set(ofsBytes, p);
        p += ofsBytes.length;
      } else if (e.type === 7) {
        body.set(hexToBytes(e.baseOid!), p);
        p += 20;
      }
      const payloadStart = offsets[i] + e.origHeaderLen;
      const payload = wholePack.subarray(payloadStart, payloadStart + e.payloadLen);
      body.set(payload, p);
    }
  } else {
    // Coalesce adjacent ranges to reduce number of R2 GET requests
    type Range = { entryIndex: number; start: number; len: number };
    const ranges: Range[] = order.map((i) => {
      const e = entries.get(i)!;
      return { entryIndex: i, start: offsets[i] + e.origHeaderLen, len: e.payloadLen };
    });
    const GAP = 8 * 1024; // max gap to coalesce
    const MAX_GROUP = 512 * 1024; // max bytes per coalesced request
    type Group = { start: number; end: number; items: Range[] };
    const groups: Group[] = [];
    let current: Group | null = null;
    for (const r of ranges) {
      if (!current) {
        current = { start: r.start, end: r.start + r.len, items: [r] };
        groups.push(current);
      } else {
        const gap = r.start - current.end;
        const newSize = r.start + r.len - current.start;
        if (gap <= GAP && newSize <= MAX_GROUP) {
          current.items.push(r);
          current.end = r.start + r.len;
        } else {
          current = { start: r.start, end: r.start + r.len, items: [r] };
          groups.push(current);
        }
      }
    }

    // Fetch coalesced groups
    const blobs: Uint8Array[] = [];
    for (const g of groups) {
      r2Gets++;
      const payload = await readPackRange(env, packKey, g.start, g.end - g.start, options);
      if (!payload) {
        log.warn("single:read-range-failed", { offset: g.start, length: g.end - g.start });
        return undefined;
      }
      blobs.push(payload);
    }

    // Write entries using slices from coalesced blobs
    let groupIdx = 0;
    for (const i of order) {
      const e = entries.get(i)!;
      let p = newOffsets.get(i)!;
      body.set(e.sizeVarBytes, p);
      p += e.sizeVarBytes.length;
      if (e.type === 6) {
        const rel = newOffsets.get(i)! - newOffsets.get(e.baseIndex!)!;
        const ofsBytes = encodeOfsDeltaDistance(rel);
        body.set(ofsBytes, p);
        p += ofsBytes.length;
      } else if (e.type === 7) {
        body.set(hexToBytes(e.baseOid!), p);
        p += 20;
      }
      const startAbs = offsets[i] + e.origHeaderLen;
      // Advance to the group that contains this entry
      while (groupIdx < groups.length && groups[groupIdx].end <= startAbs) groupIdx++;
      const g = groups[groupIdx];
      const buf = blobs[groupIdx];
      const rel = startAbs - g.start;
      body.set(buf.subarray(rel, rel + e.payloadLen), p);
    }
    log.debug("single:coalesce", { groups: groups.length });
  }

  // Append SHA-1 trailer
  const sha = new Uint8Array(await crypto.subtle.digest("SHA-1", asBufferSource(body)));
  const out = new Uint8Array(body.byteLength + 20);
  out.set(body, 0);
  out.set(sha, body.byteLength);
  log.info("single:assembled", {
    objects: order.length,
    bytes: out.byteLength,
    r2Gets,
    timeMs: Date.now() - started,
  });
  return out;
}

/**
 * @deprecated This function is deprecated in favor of streamPackFromMultiplePacks from assemblerStream.ts.
 * The streaming implementation reduces memory usage and supports backpressure.
 * This buffered implementation will be removed in a future version.
 *
 * Assembles a pack from multiple pack files in R2.
 * @param env - Worker environment
 * @param packKeys - Array of R2 pack keys to use as sources
 * @param neededOids - List of object IDs the output pack must contain
 * @param options - Signal, limiter, and subrequest counter
 * @returns Assembled pack bytes
 */
export async function assemblePackFromMultiplePacks(
  env: Env,
  packKeys: string[],
  neededOids: string[],
  options?: {
    signal?: AbortSignal;
    limiter?: { run<T>(label: string, fn: () => Promise<T>): Promise<T> };
    countSubrequest?: (n?: number) => void;
  }
): Promise<Uint8Array | undefined> {
  const log = createLogger(env.LOG_LEVEL, { service: "PackAssemblerMulti" });
  const started = Date.now();
  let r2PayloadGets = 0;
  let r2WholeGets = 0;
  log.debug("multi:start", { packs: packKeys.length, needed: neededOids.length });
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
  const metaResults = await mapWithConcurrency(packKeys, CONC, async (key) => {
    if (options?.signal?.aborted) return undefined;
    const parsed = await loadIdxParsed(env, key, options);
    const head = options?.limiter
      ? await options.limiter.run("r2:head-pack", async () => {
          options.countSubrequest?.();
          return await env.REPO_BUCKET.head(key);
        })
      : await env.REPO_BUCKET.head(key);
    if (!parsed || !head) {
      log.debug("multi:missing-pack-or-idx", { key, idx: !!parsed, head: !!head });
      return undefined;
    }
    const oidToIndex = new Map<string, number>();
    for (let i = 0; i < parsed.oids.length; i++) oidToIndex.set(parsed.oids[i], i);
    const offsetToIndex = new Map<number, number>();
    for (let i = 0; i < parsed.offsets.length; i++) offsetToIndex.set(parsed.offsets[i], i);
    // Build per-pack nextOffset map using offsets sorted by physical position
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
          r2WholeGets++;
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
  });
  for (const m of metaResults) if (m) metas.push(m);
  if (metas.length === 0) {
    log.debug("multi:no-metas", {});
    return undefined;
  }

  // Selection: map each needed oid to ONE canonical source pack entry and reuse it everywhere
  type Sel = { m: Meta; i: number };
  const selected = new Map<string, Sel>(); // key: `${m.key}#${i}`
  const chosen = new Map<string, Sel>(); // key: oid -> chosen selection
  const pending: Sel[] = [];
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
  for (const oid of neededOids) {
    const sel = getChosenSel(oid);
    if (!sel) {
      log.debug("multi:cannot-cover", { oid });
      return undefined; // cannot cover
    }
    const key = `${sel.m.key}#${sel.i}`;
    if (!selected.has(key)) selected.set(key, sel);
    pending.push(sel);
  }

  // Include delta bases (use chosen sel for base OIDs to avoid duplicates across packs)
  while (pending.length) {
    if (options?.signal?.aborted) return undefined;
    const { m, i } = pending.pop()!;
    const off = m.offsets[i];
    const header = m.wholePack
      ? readPackHeaderExFromBuf(m.wholePack, off)
      : await readPackHeaderEx(env, m.key, off, options);
    if (!header) {
      log.warn("multi:read-header-failed", { key: m.key, off });
      return undefined;
    }
    if (header.type === 6) {
      // OFS_DELTA: compute base OID, then choose a canonical source for that OID
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

  // Build dependency graph for topo order
  type Node = Sel & {
    oid: string;
    origHeaderLen: number;
    sizeVarBytes: Uint8Array;
    type: number;
    base?: Sel;
  };
  const nodes: Node[] = [];
  const nodeKey = (s: Sel) => `${s.m.key}#${s.i}`;
  const nodeMap = new Map<string, Node>();
  for (const s of selected.values()) {
    const off = s.m.offsets[s.i];
    const h = s.m.wholePack
      ? readPackHeaderExFromBuf(s.m.wholePack, off)
      : await readPackHeaderEx(env, s.m.key, off, options);
    if (!h) return undefined;
    const n: Node = {
      ...s,
      oid: s.m.oids[s.i],
      origHeaderLen: h.headerLen,
      sizeVarBytes: h.sizeVarBytes,
      type: h.type,
    };
    if (h.type === 6) {
      // Resolve base by OID then map to chosen selection
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
  // Kahn's algorithm with tie-breaker: by pack key order then offset
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
    // Fallback: simple order by pack then offset
    order.length = 0;
    const arr = Array.from(nodes);
    arr.sort(
      (a, b) =>
        packOrder.get(a.m.key)! - packOrder.get(b.m.key)! || a.m.offsets[a.i] - b.m.offsets[b.i]
    );
    order.push(...arr);
  }

  // Sanity: ensure all delta entries have their bases included
  for (const n of order) {
    if (n.type === 6 || n.type === 7) {
      if (!n.base) {
        log.warn("multi:delta-missing-base", { key: n.m.key, oid: n.m.oids[n.i], type: n.type });
        return undefined;
      }
      const bk = nodeKey(n.base);
      if (!nodeMap.has(bk)) {
        log.warn("multi:base-not-included", { key: n.m.key, oid: n.m.oids[n.i], baseKey: bk });
        return undefined;
      }
    }
  }

  // Precompute original payload lengths once per node
  const payloadLenByKey = new Map<string, number>();
  for (const n of order) {
    const k = nodeKey(n);
    const off = n.m.offsets[n.i];
    const objEnd = n.m.nextOffset.get(off)!;
    const pl = objEnd - off - n.origHeaderLen;
    payloadLenByKey.set(k, pl);
  }

  // Iteratively recompute OFS varint lengths and offsets until stable.
  // A single second pass can still underestimate sizes when subsequent
  // entries cross varint boundaries due to offset shifts.
  const newHeaderLen = new Map<string, number>();
  for (const n of order) {
    if (options?.signal?.aborted) return undefined;
    if (n.type === 6) {
      // Initial guess using original distance
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
    // Compute offsets based on current header lengths
    newOffsets = new Map<string, number>();
    cur = 12;
    for (const n of order) {
      const k = nodeKey(n);
      newOffsets.set(k, cur);
      const pl = payloadLenByKey.get(k)!;
      cur += newHeaderLen.get(k)! + pl;
    }
    // Re-evaluate OFS varints with accurate distances
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
    if (!changed || ++iter >= 16) break; // converge or cap iterations
  }

  // Recompute exact header lengths from final offsets to ensure consistency,
  // then allocate the body buffer accordingly to avoid any underestimation.
  const finalHeaderLen = new Map<string, number>();
  for (const n of order) {
    const k = nodeKey(n);
    if (n.type === 6) {
      const rel = newOffsets.get(k)! - newOffsets.get(nodeKey(n.base!))!;
      finalHeaderLen.set(k, n.sizeVarBytes.length + encodeOfsDeltaDistance(rel).length);
    } else if (n.type === 7) {
      finalHeaderLen.set(k, n.sizeVarBytes.length + 20);
    } else {
      finalHeaderLen.set(k, n.sizeVarBytes.length);
    }
  }
  // Recompute final offsets using the final header lengths to match allocation
  newOffsets = new Map<string, number>();
  let acc = 12;
  for (const n of order) {
    const k = nodeKey(n);
    newOffsets.set(k, acc);
    acc += (finalHeaderLen.get(k) || 0) + (payloadLenByKey.get(k) || 0);
  }
  const finalSize = acc;
  // Update maps to reflect final header lengths
  for (const [k, v] of finalHeaderLen) newHeaderLen.set(k, v);

  // Compose body with exact final size
  const body = new Uint8Array(finalSize);
  body.set(new TextEncoder().encode("PACK"), 0);
  const dv = new DataView(body.buffer);
  dv.setUint32(4, 2);
  dv.setUint32(8, order.length);
  for (const n of order) {
    if (options?.signal?.aborted) return undefined;
    let p = newOffsets.get(nodeKey(n))!;
    body.set(n.sizeVarBytes, p);
    p += n.sizeVarBytes.length;
    if (n.type === 6) {
      const rel = newOffsets.get(nodeKey(n))! - newOffsets.get(nodeKey(n.base!))!;
      const ofsBytes = encodeOfsDeltaDistance(rel);
      body.set(ofsBytes, p);
      p += ofsBytes.length;
    } else if (n.type === 7) {
      // REF_DELTA: write the base OID that was properly resolved during node building
      const baseOid = n.base!.m.oids[n.base!.i];
      body.set(hexToBytes(baseOid), p);
      p += 20;
    }
    const k2 = nodeKey(n);
    const payloadStart = n.m.offsets[n.i] + n.origHeaderLen;
    const payloadLen = payloadLenByKey.get(k2)!;
    // Validate computed ranges before issuing R2 GETs or writing into body
    if (
      payloadLen <= 0 ||
      payloadStart < 0 ||
      payloadStart + payloadLen >
        n.m.packSize - 0 /* allow up to end (header excluded via -20 above) */
    ) {
      log.warn("multi:invalid-range", {
        key: n.m.key,
        offset: payloadStart,
        length: payloadLen,
        packSize: n.m.packSize,
      });
      return undefined;
    }
    const payload = n.m.wholePack
      ? n.m.wholePack.subarray(payloadStart, payloadStart + payloadLen)
      : await readPackRange(env, n.m.key, payloadStart, payloadLen, options);
    if (!payload) {
      log.warn("multi:read-range-failed", {
        key: n.m.key,
        offset: payloadStart,
        length: payloadLen,
      });
      return undefined;
    }
    // Ensure we won't write past the end of the allocated body buffer
    const k = nodeKey(n);
    const entryStart = newOffsets.get(k)!;
    const entryTotal = newHeaderLen.get(k)! + payload.length;
    if (entryStart + entryTotal > body.byteLength) {
      log.warn("multi:entry-overflow", {
        at: entryStart,
        entryBytes: entryTotal,
        bodyBytes: body.byteLength,
        oid: n.oid,
        packKey: n.m.key,
        type: n.type,
      });
      return undefined;
    }
    body.set(payload, p);
    if (!n.m.wholePack) r2PayloadGets++;
  }
  const sha = new Uint8Array(await crypto.subtle.digest("SHA-1", asBufferSource(body)));
  const out = new Uint8Array(body.byteLength + 20);
  out.set(body, 0);
  out.set(sha, body.byteLength);
  log.info("multi:assembled", {
    objects: order.length,
    bytes: out.byteLength,
    payloadGets: r2PayloadGets,
    wholeGets: r2WholeGets,
    timeMs: Date.now() - started,
  });
  return out;
}
