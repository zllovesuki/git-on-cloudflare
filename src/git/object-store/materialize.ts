import type { Logger } from "@/common/logger.ts";
import type { Limiter } from "@/git/operations/limits.ts";
import type { PackedObjectResult } from "./types.ts";
import type { PackedObjectCandidate } from "./candidates.ts";

import { inflate } from "@/common/index.ts";
import { readPackHeaderExFromBuf, readPackRange } from "@/git/pack/packMeta.ts";
import { applyGitDelta } from "./delta.ts";
import { findOffsetIndex, getNextOffsetByIndex, getOidHexAt } from "./idxView.ts";
import { toPackedObjectResult, typeCodeToObjectType } from "./support.ts";

export type PackedMaterializerCyclePolicy = "throw" | "miss";

export type PackedRefBaseResolver = (
  oid: string,
  visited: Set<string>
) => Promise<PackedObjectResult | undefined>;

export type PackedMaterializerAbortCheck = (stage: string) => void;

export type PackedMaterializerOptions = {
  env: Env;
  candidate: PackedObjectCandidate;
  limiter: Limiter;
  countSubrequest: (n?: number) => boolean | void;
  log: Logger;
  cyclePolicy: PackedMaterializerCyclePolicy;
  resolveRefBase: PackedRefBaseResolver;
  visited: Set<string>;
  signal?: AbortSignal;
  checkAborted?: PackedMaterializerAbortCheck;
};

function makeVisitKey(candidate: PackedObjectCandidate): string {
  return `${candidate.source.packKey}#${candidate.objectIndex}`;
}

function makeCandidateFromOffset(
  source: PackedObjectCandidate["source"],
  packSlot: number,
  objectIndex: number,
  offset: number
): PackedObjectCandidate | undefined {
  const nextOffset = getNextOffsetByIndex(source.idx, objectIndex);
  if (nextOffset === undefined) return undefined;

  return {
    source,
    packSlot,
    objectIndex,
    offset,
    nextOffset,
    oid: getOidHexAt(source.idx, objectIndex),
  };
}

async function materializeCandidate(
  options: PackedMaterializerOptions,
  candidate: PackedObjectCandidate
): Promise<PackedObjectResult | undefined> {
  options.checkAborted?.("packed-materialize:entry");

  const visitKey = makeVisitKey(candidate);
  if (options.visited.has(visitKey)) {
    options.log.debug("packed-materialize:cycle", {
      packKey: candidate.source.packKey,
      objectIndex: candidate.objectIndex,
      oid: candidate.oid,
      cyclePolicy: options.cyclePolicy,
    });
    if (options.cyclePolicy === "throw") throw new Error("pack object recursion cycle");
    return undefined;
  }

  options.visited.add(visitKey);
  try {
    const entryLength = candidate.nextOffset - candidate.offset;
    if (entryLength <= 0) {
      options.log.warn("packed-materialize:invalid-entry-span", {
        packKey: candidate.source.packKey,
        objectIndex: candidate.objectIndex,
        oid: candidate.oid,
        offset: candidate.offset,
        nextOffset: candidate.nextOffset,
      });
      return undefined;
    }

    options.log.debug("packed-materialize:read-entry", {
      packKey: candidate.source.packKey,
      objectIndex: candidate.objectIndex,
      oid: candidate.oid,
      offset: candidate.offset,
      length: entryLength,
    });
    const entry = await readPackRange(
      options.env,
      candidate.source.packKey,
      candidate.offset,
      entryLength,
      {
        limiter: options.limiter,
        countSubrequest: options.countSubrequest,
        signal: options.signal,
      }
    );
    if (!entry) {
      options.log.debug("packed-materialize:entry-miss", {
        packKey: candidate.source.packKey,
        objectIndex: candidate.objectIndex,
        oid: candidate.oid,
      });
      return undefined;
    }

    options.checkAborted?.("packed-materialize:inflate");
    const header = readPackHeaderExFromBuf(entry, 0);
    if (!header) {
      options.log.warn("packed-materialize:bad-header", {
        packKey: candidate.source.packKey,
        objectIndex: candidate.objectIndex,
        oid: candidate.oid,
      });
      return undefined;
    }

    const payload = await inflate(entry.subarray(header.headerLen));
    const objectType = typeCodeToObjectType(header.type);
    if (objectType) return toPackedObjectResult(candidate, objectType, payload);

    let base: PackedObjectResult | undefined;
    if (header.type === 6) {
      const baseOffset = candidate.offset - (header.baseRel || 0);
      const baseIndex = findOffsetIndex(candidate.source.idx, baseOffset);
      if (baseIndex === undefined) {
        options.log.debug("packed-materialize:ofs-base-miss", {
          packKey: candidate.source.packKey,
          objectIndex: candidate.objectIndex,
          oid: candidate.oid,
          baseOffset,
        });
        return undefined;
      }

      const baseCandidate = makeCandidateFromOffset(
        candidate.source,
        candidate.packSlot,
        baseIndex,
        baseOffset
      );
      if (!baseCandidate) return undefined;
      base = await materializeCandidate(options, baseCandidate);
    } else if (header.type === 7 && header.baseOid) {
      base = await options.resolveRefBase(header.baseOid, options.visited);
    }

    if (!base) {
      options.log.debug("packed-materialize:base-miss", {
        packKey: candidate.source.packKey,
        objectIndex: candidate.objectIndex,
        oid: candidate.oid,
        typeCode: header.type,
        baseOid: header.baseOid,
      });
      return undefined;
    }

    options.checkAborted?.("packed-materialize:delta");
    return toPackedObjectResult(candidate, base.type, applyGitDelta(base.payload, payload));
  } finally {
    options.visited.delete(visitKey);
  }
}

export async function materializePackedObjectCandidate(
  options: PackedMaterializerOptions
): Promise<PackedObjectResult | undefined> {
  return await materializeCandidate(options, options.candidate);
}
