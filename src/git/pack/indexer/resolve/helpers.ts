import { computeOidBytes, objTypeCode } from "@/git/core/objects.ts";
import { applyGitDelta } from "@/git/object-store/delta.ts";

import type { PackEntryTable, ResolveOptions } from "../types.ts";

import { getBasePayload } from "./materialize.ts";
import { promoteReadyInPackDependents, type InPackDependencyQueue } from "./dependencies.ts";
import { throwIfAborted } from "./errors.ts";
import type { PayloadLRU } from "./payloadCache.ts";
import { inflateFromReader, type SequentialReader } from "./reader.ts";
import { promoteWaitingRefDeltas, type RefBaseLookup } from "./refLookup.ts";

export function storeOid(table: PackEntryTable, index: number, oidBytes: Uint8Array): void {
  table.oids.set(oidBytes, index * 20);
  table.resolved[index] = 1;
}

interface ResolveDeltaEntryArgs {
  index: number;
  resolveOpts: ResolveOptions;
  table: PackEntryTable;
  lru: PayloadLRU;
  deltaReader: SequentialReader;
  baseReader: SequentialReader;
  baseIndex: Int32Array;
  resolvedTypeCodes: Uint8Array;
  isBase: Uint8Array;
  deadlines: Uint32Array;
  refLookup: RefBaseLookup | null;
  dependencyQueue?: InPackDependencyQueue;
  readyDeferred?: number[];
  deferredQueued?: Uint8Array;
}

/**
 * Resolve a delta whose base is already known to be available. The base may be
 * in-pack or external; `getBasePayload()` hides that distinction.
 */
export async function resolveDeltaEntry(args: ResolveDeltaEntryArgs): Promise<void> {
  throwIfAborted(args.resolveOpts.signal, args.resolveOpts.log, "resolve:delta-entry");
  const bi = args.baseIndex[args.index];
  if (bi < 0 || !args.table.resolved[bi]) {
    throw new Error(`resolve: base entry ${bi} is not ready for ${args.index}`);
  }

  const base = await getBasePayload(
    args.resolveOpts,
    bi,
    args.lru,
    args.baseReader,
    args.table,
    args.baseIndex
  );
  const deltaPayload = await inflateFromReader(args.deltaReader, args.table, args.index);
  throwIfAborted(args.resolveOpts.signal, args.resolveOpts.log, "resolve:delta-apply");
  const result = applyGitDelta(base.payload, deltaPayload);
  if (result.length !== args.table.decompressedSizes[args.index]) {
    throw new Error(
      `resolve: delta result size mismatch at offset ${args.table.offsets[args.index]} (expected ${args.table.decompressedSizes[args.index]}, got ${result.length})`
    );
  }

  storeOid(args.table, args.index, await computeOidBytes(base.type, result));
  args.resolvedTypeCodes[args.index] = objTypeCode(base.type);
  args.table.objectTypes[args.index] = args.resolvedTypeCodes[args.index];
  args.resolveOpts.scanResult.refsBuilder?.recordObject(args.index, base.type, result);
  if (args.refLookup) {
    promoteWaitingRefDeltas(
      args.refLookup,
      args.index,
      args.table,
      args.baseIndex,
      args.isBase,
      args.deadlines,
      args.readyDeferred,
      args.deferredQueued
    );
  }
  if (args.dependencyQueue && args.readyDeferred && args.deferredQueued) {
    // OFS_DELTA dependents can already know their base entry index even when
    // that base is still unresolved. Wake them here now that this entry's OID
    // and payload are available, or they remain stranded in the deferred set.
    promoteReadyInPackDependents(
      args.dependencyQueue,
      args.index,
      args.readyDeferred,
      args.deferredQueued,
      args.table,
      args.baseIndex
    );
  }
  if (args.isBase[args.index]) {
    args.lru.set(args.index, { type: base.type, payload: result });
  }
}
