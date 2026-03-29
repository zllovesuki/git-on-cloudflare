import type { IdxView, PackCatalogRow } from "@/git/object-store/types.ts";

import { inflate } from "@/common/index.ts";
import { applyGitDelta } from "@/git/object-store/delta.ts";
import {
  findOidIndex,
  findOffsetIndex,
  getNextOffset,
  getNextOffsetByIndex,
  getOidHexAt,
  loadIdxView,
} from "@/git/object-store/idxView.ts";
import { typeCodeToObjectType } from "@/git/object-store/support.ts";
import { readPackHeaderExFromBuf, readPackRange } from "@/git/pack/packMeta.ts";

type DebugResolvedLocation = {
  pack: PackCatalogRow;
  idx: IdxView;
  objectIndex: number;
  offset: number;
  nextOffset: number;
  oid: string;
};

async function findCatalogObject(
  env: Env,
  rows: PackCatalogRow[],
  oid: string
): Promise<DebugResolvedLocation | undefined> {
  const oidLc = oid.toLowerCase();
  for (const pack of rows) {
    const idx = await loadIdxView(env, pack.packKey, undefined, pack.packBytes);
    if (!idx) continue;
    const objectIndex = findOidIndex(idx, oidLc);
    if (objectIndex < 0) continue;
    const offset = idx.offsets[objectIndex];
    const noff = getNextOffset(idx, offset);
    if (noff === undefined) continue;
    return {
      pack,
      idx,
      objectIndex,
      offset,
      nextOffset: noff,
      oid: oidLc,
    };
  }
  return undefined;
}

async function readPackedObjectFromLocation(
  env: Env,
  location: DebugResolvedLocation,
  rows: PackCatalogRow[],
  visited: Set<string>
): Promise<{ type: "commit" | "tree" | "blob" | "tag"; payload: Uint8Array } | undefined> {
  const visitKey = `${location.pack.packKey}#${location.objectIndex}`;
  if (visited.has(visitKey)) throw new Error("pack object recursion cycle");
  visited.add(visitKey);
  try {
    const entryLength = location.nextOffset - location.offset;
    if (entryLength <= 0) return undefined;
    const entry = await readPackRange(env, location.pack.packKey, location.offset, entryLength);
    if (!entry) return undefined;

    const header = readPackHeaderExFromBuf(entry, 0);
    if (!header) return undefined;
    const inflated = await inflate(entry.subarray(header.headerLen));

    const baseType = typeCodeToObjectType(header.type);
    if (baseType) return { type: baseType, payload: inflated };

    if (header.type === 6) {
      const baseOffset = location.offset - (header.baseRel || 0);
      const baseIndex = findOffsetIndex(location.idx, baseOffset);
      if (baseIndex === undefined) return undefined;
      const baseNextOffset = getNextOffsetByIndex(location.idx, baseIndex);
      if (baseNextOffset === undefined) return undefined;
      const base = await readPackedObjectFromLocation(
        env,
        {
          pack: location.pack,
          idx: location.idx,
          objectIndex: baseIndex,
          offset: baseOffset,
          nextOffset: baseNextOffset,
          oid: getOidHexAt(location.idx, baseIndex),
        },
        rows,
        visited
      );
      if (!base) return undefined;
      return {
        type: base.type,
        payload: applyGitDelta(base.payload, inflated),
      };
    }

    if (header.type === 7 && header.baseOid) {
      const base = await readPackedObjectFromCatalogRows(env, rows, header.baseOid, visited);
      if (!base) return undefined;
      return {
        type: base.type,
        payload: applyGitDelta(base.payload, inflated),
      };
    }

    return undefined;
  } finally {
    visited.delete(visitKey);
  }
}

export async function readPackedObjectFromCatalogRows(
  env: Env,
  rows: PackCatalogRow[],
  oid: string,
  visited: Set<string> = new Set<string>()
): Promise<{ type: "commit" | "tree" | "blob" | "tag"; payload: Uint8Array } | undefined> {
  const location = await findCatalogObject(env, rows, oid);
  if (!location) return undefined;
  return await readPackedObjectFromLocation(env, location, rows, visited);
}

export async function findCatalogPacksContainingOid(
  env: Env,
  rows: PackCatalogRow[],
  oid: string
): Promise<string[]> {
  const oidLc = oid.toLowerCase();
  const matches: string[] = [];
  for (const row of rows) {
    const idx = await loadIdxView(env, row.packKey, undefined, row.packBytes);
    if (!idx) continue;
    if (findOidIndex(idx, oidLc) >= 0) matches.push(row.packKey);
  }
  return matches;
}
