import { bytesToHex } from "@/common/hex.ts";
import { readObject } from "@/git/object-store/store.ts";
import { applyGitDelta } from "@/git/object-store/delta.ts";
import { typeCodeToObjectType } from "@/git/object-store/support.ts";

import { getRefBaseOidAt } from "../types.ts";
import type { PackEntryTable, ResolveOptions } from "../types.ts";

import { throwIfAborted } from "./errors.ts";
import type { CacheEntry } from "./payloadCache.ts";
import { PayloadLRU } from "./payloadCache.ts";
import { inflateFromReader, type SequentialReader } from "./reader.ts";

/**
 * Get a resolved base payload from the LRU cache, or rematerialize it from the
 * pack/object store when an earlier eviction forced it out of memory.
 */
export async function getBasePayload(
  opts: ResolveOptions,
  index: number,
  lru: PayloadLRU,
  reader: SequentialReader,
  table: PackEntryTable,
  baseIndexArr: Int32Array
): Promise<CacheEntry> {
  const cached = lru.get(index);
  if (cached) return cached;
  return await materializeEntry(opts, index, lru, reader, table, baseIndexArr);
}

/**
 * Materialize an already-resolved entry's payload without recursive calls. Deep
 * delta chains are valid input, and cache misses on a long chain should not be
 * able to exhaust the worker's JS call stack.
 */
async function materializeEntry(
  opts: ResolveOptions,
  index: number,
  lru: PayloadLRU,
  reader: SequentialReader,
  table: PackEntryTable,
  baseIndexArr: Int32Array
): Promise<CacheEntry> {
  const pending: number[] = [];
  let currentIndex = index;
  let base: CacheEntry | undefined;
  let traversalSteps = 0;

  while (!base) {
    throwIfAborted(opts.signal, opts.log, "materialize:walk");
    if (traversalSteps >= baseIndexArr.length) {
      throw new Error(`materialize: base chain cycle or runaway traversal for entry ${index}`);
    }
    traversalSteps++;

    const cached = lru.get(currentIndex);
    if (cached) {
      base = cached;
      break;
    }

    const baseType = typeCodeToObjectType(table.types[currentIndex]);
    if (baseType) {
      const payload = await inflateFromReader(reader, table, currentIndex);
      base = { type: baseType, payload };
      lru.set(currentIndex, base);
      break;
    }

    pending.push(currentIndex);
    const bi = baseIndexArr[currentIndex];
    if (bi >= 0) {
      currentIndex = bi;
      continue;
    }

    if (table.types[currentIndex] === 7) {
      const baseOid = bytesToHex(getRefBaseOidAt(opts.scanResult.refBaseOids, currentIndex));
      const obj = await readObject(opts.env, opts.repoId, baseOid, opts.cacheCtx);
      if (!obj) throw new Error(`materialize: external base ${baseOid} not found`);
      base = { type: obj.type, payload: obj.payload };
      break;
    }

    throw new Error(`materialize: unresolved in-pack base for entry ${currentIndex}`);
  }

  if (!base) {
    throw new Error(`materialize: failed to resolve base for entry ${index}`);
  }

  while (pending.length > 0) {
    throwIfAborted(opts.signal, opts.log, "materialize:replay");
    const deltaIndex = pending.pop()!;
    const deltaPayload = await inflateFromReader(reader, table, deltaIndex);
    throwIfAborted(opts.signal, opts.log, "materialize:replay");
    const result = applyGitDelta(base.payload, deltaPayload);
    if (result.length !== table.decompressedSizes[deltaIndex]) {
      throw new Error(
        `materialize: delta result size mismatch at offset ${table.offsets[deltaIndex]} (expected ${table.decompressedSizes[deltaIndex]}, got ${result.length})`
      );
    }
    base = { type: base.type, payload: result };
    lru.set(deltaIndex, base);
  }

  return base;
}
