import type { PackEntryTable, ResolveOptions } from "../types.ts";

import type { InPackDependencyQueue } from "./dependencies.ts";
import { throwIfAborted } from "./errors.ts";
import { resolveDeltaEntry } from "./helpers.ts";
import type { PayloadLRU } from "./payloadCache.ts";
import { type SequentialReader } from "./reader.ts";
import type { RefBaseLookup } from "./refLookup.ts";

interface DrainReadyDeferredQueueArgs {
  readyDeferred: number[];
  deferredQueued: Uint8Array;
  resolved: number;
  initialResolvedCount: number;
  totalUnresolved: number;
  log: ResolveOptions["log"];
  resolveOpts: ResolveOptions;
  table: PackEntryTable;
  lru: PayloadLRU;
  reader: SequentialReader;
  resolvedTypeCodes: Uint8Array;
  refLookup: RefBaseLookup | null;
  dependencyQueue?: InPackDependencyQueue;
  baseIndex: Int32Array;
  isBase: Uint8Array;
  deadlines: Uint32Array;
}

export async function drainReadyDeferredQueue(args: DrainReadyDeferredQueueArgs): Promise<number> {
  for (let readyPos = 0; readyPos < args.readyDeferred.length; readyPos++) {
    throwIfAborted(args.resolveOpts.signal, args.log, "resolve:deferred-drain");
    // The queue can grow while we iterate: resolving one deferred delta may
    // publish the OID needed by additional deferred REF_DELTAs. Iterating by
    // index lets us drain those follow-on entries in the same pass.
    const index = args.readyDeferred[readyPos];
    args.deferredQueued[index] = 0;
    if (args.table.resolved[index]) continue;

    const bi = args.baseIndex[index];
    if (bi < 0 || !args.table.resolved[bi]) continue;

    args.lru.setCurrentOffset(args.table.offsets[index]);
    await resolveDeltaEntry({
      index,
      resolveOpts: args.resolveOpts,
      table: args.table,
      lru: args.lru,
      deltaReader: args.reader,
      baseReader: args.reader,
      baseIndex: args.baseIndex,
      resolvedTypeCodes: args.resolvedTypeCodes,
      isBase: args.isBase,
      deadlines: args.deadlines,
      refLookup: args.refLookup,
      dependencyQueue: args.dependencyQueue,
      readyDeferred: args.readyDeferred,
      deferredQueued: args.deferredQueued,
    });
    args.resolved++;
    const newlyResolved = args.resolved - args.initialResolvedCount;
    if (newlyResolved > 0 && newlyResolved % 10000 === 0) {
      args.log.debug("resolve:progress", {
        resolved: newlyResolved,
        total: args.totalUnresolved,
      });
    }
  }

  args.readyDeferred.length = 0;
  return args.resolved;
}
