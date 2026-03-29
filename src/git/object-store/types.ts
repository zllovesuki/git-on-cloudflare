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

export type IdxView = {
  packKey: string;
  count: number;
  fanout: Uint32Array;
  rawNames: Uint8Array;
  offsets: number[];
  offsetToIndex: Map<number, number>;
  nextOffset: Map<number, number>;
  packSize: number;
};
