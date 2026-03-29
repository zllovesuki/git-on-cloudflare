import type { GitObjectType } from "@/git/core/index.ts";
export type { PackCatalogRow } from "@/do/repo/db/schema.ts";

export type PackedObjectLocation = {
  packKey: string;
  objectIndex: number;
  offset: number;
  nextOffset: number;
};

export type PackedObjectResult = PackedObjectLocation & {
  oid: string;
  type: GitObjectType;
  payload: Uint8Array;
};

export type PackedObjectReadResult = PackedObjectResult | undefined;

export type IdxView = {
  packKey: string;
  count: number;
  fanout: Uint32Array;
  rawNames: Uint8Array;
  offsets: Float64Array;
  /** Start of the next pack entry for each object index; last entry points at the pack trailer. */
  nextOffsetByIndex: Float64Array;
  /** Pack offsets sorted ascending, paired with sortedOffsetIndices for binary search. */
  sortedOffsets: Float64Array;
  /** Entry indices corresponding to each element of sortedOffsets. */
  sortedOffsetIndices: Uint32Array;
  packSize: number;
};
