/**
 * Streaming pack indexer — public API.
 */

export { scanPack } from "./scan.ts";
export { resolveDeltasAndWriteIdx } from "./resolve/index.ts";
export { ResolveAbortedError, isResolveAbortedError } from "./resolve/errors.ts";
export { runPackConnectivityCheck } from "./connectivity.ts";
export { writeIdxV2 } from "./writeIdx.ts";
export { InflateCursor, CRC32_INIT, crc32Update, crc32Finish } from "./inflateCursor.ts";
export { allocateEntryTable, searchOffsetIndex, getRefBaseOidAt } from "./types.ts";
export type {
  PackEntryTable,
  RefBaseOids,
  ScanResult,
  ResolveResult,
  IndexerOptions,
  ResolveOptions,
  ConnectivityCheckOptions,
} from "./types.ts";
