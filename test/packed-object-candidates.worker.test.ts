import { describe, expect, it } from "vitest";
import { env } from "cloudflare:test";

import { buildCopyPrefixDelta, buildPack } from "./util/git-pack.ts";
import { uniqueRepoId } from "./util/test-helpers.ts";
import {
  makeActiveCatalogRow,
  makeLimiter,
  packIndexerLog as log,
} from "./util/pack-indexer.helpers.ts";

import { computeOid } from "@/git/core/objects.ts";
import {
  collectPackedObjectCandidates,
  findFirstPackedObjectCandidate,
  findOidRunInIdx,
  loadIdxView,
  materializePackedObjectCandidate,
  type IndexedPackSource,
  type PackedMaterializerCyclePolicy,
  type PackedObjectCandidate,
} from "@/git/object-store/index.ts";
import type { PackedObjectResult, PackCatalogRow } from "@/git/object-store/types.ts";
import { scanPack, resolveDeltasAndWriteIdx } from "@/git/pack/indexer/index.ts";

type IndexedTestPack = {
  packKey: string;
  packBytes: number;
  objectCount: number;
  idxBytes: number;
  source: IndexedPackSource;
};

async function putIndexedPack(args: {
  packKey: string;
  packBytes: Uint8Array;
  activeCatalog?: PackCatalogRow[];
}): Promise<IndexedTestPack> {
  await env.REPO_BUCKET.put(args.packKey, args.packBytes);
  const packSize = args.packBytes.byteLength;
  const scanResult = await scanPack({
    env,
    packKey: args.packKey,
    packSize,
    limiter: makeLimiter(),
    countSubrequest: () => {},
    log,
  });
  const resolveResult = await resolveDeltasAndWriteIdx({
    env,
    packKey: args.packKey,
    packSize,
    limiter: makeLimiter(),
    countSubrequest: () => {},
    log,
    scanResult,
    activeCatalog: args.activeCatalog,
    repoId: "test",
  });

  const idx = await loadIdxView(env, args.packKey, undefined, packSize);
  if (!idx) throw new Error(`test failed to load idx for ${args.packKey}`);

  return {
    packKey: args.packKey,
    packBytes: packSize,
    objectCount: scanResult.objectCount,
    idxBytes: resolveResult.idxBytes,
    source: {
      packKey: args.packKey,
      packBytes: packSize,
      idx,
    },
  };
}

function catalogRow(pack: IndexedTestPack): PackCatalogRow {
  return makeActiveCatalogRow({
    packKey: pack.packKey,
    packBytes: pack.packBytes,
    objectCount: pack.objectCount,
    idxBytes: pack.idxBytes,
  });
}

async function materializeWithFirstHitRefs(args: {
  candidate: PackedObjectCandidate;
  sources: IndexedPackSource[];
  cyclePolicy: PackedMaterializerCyclePolicy;
  visited: Set<string>;
}): Promise<PackedObjectResult | undefined> {
  return await materializePackedObjectCandidate({
    env,
    candidate: args.candidate,
    limiter: makeLimiter(),
    countSubrequest: () => {},
    log,
    cyclePolicy: args.cyclePolicy,
    resolveRefBase: async (baseOid, nextVisited) => {
      const baseCandidate = findFirstPackedObjectCandidate(args.sources, baseOid);
      if (!baseCandidate) return undefined;
      return await materializeWithFirstHitRefs({
        ...args,
        candidate: baseCandidate,
        visited: nextVisited,
      });
    },
    visited: args.visited,
  });
}

describe("packed object candidate helpers", () => {
  it("enumerates the full duplicate OID run in an idx", async () => {
    const prefix = uniqueRepoId("candidate-duplicate-run");
    const duplicatePayload = new TextEncoder().encode("duplicate blob\n");
    const duplicateOid = await computeOid("blob", duplicatePayload);
    const otherPayload = new TextEncoder().encode("other blob\n");
    const packBytes = await buildPack([
      { type: "blob", payload: otherPayload },
      { type: "blob", payload: duplicatePayload },
      { type: "blob", payload: duplicatePayload },
      { type: "blob", payload: duplicatePayload },
    ]);
    const pack = await putIndexedPack({
      packKey: `test/${prefix}.pack`,
      packBytes,
    });

    const run = findOidRunInIdx(pack.source.idx, duplicateOid);
    if (!run) throw new Error("test failed to find duplicate OID run");
    expect(run.endIndex - run.startIndex + 1).toBe(3);

    const candidates = collectPackedObjectCandidates([pack.source], duplicateOid);
    expect(candidates).toHaveLength(3);
    expect(new Set(candidates.map((candidate) => candidate.objectIndex)).size).toBe(3);
    expect(candidates.every((candidate) => candidate.oid === duplicateOid)).toBe(true);
  });

  it("keeps first-candidate lookup in snapshot order", async () => {
    const prefix = uniqueRepoId("candidate-first-hit");
    const payload = new TextEncoder().encode("shared object\n");
    const oid = await computeOid("blob", payload);
    const older = await putIndexedPack({
      packKey: `test/${prefix}-older.pack`,
      packBytes: await buildPack([{ type: "blob", payload }]),
    });
    const newer = await putIndexedPack({
      packKey: `test/${prefix}-newer.pack`,
      packBytes: await buildPack([{ type: "blob", payload }]),
    });

    const first = findFirstPackedObjectCandidate([newer.source, older.source], oid);
    expect(first?.source.packKey).toBe(newer.packKey);
    expect(first?.packSlot).toBe(0);

    const reversed = findFirstPackedObjectCandidate([older.source, newer.source], oid);
    expect(reversed?.source.packKey).toBe(older.packKey);
    expect(reversed?.packSlot).toBe(0);
  });

  it("returns a miss for cyclic candidates so callers can try an older duplicate", async () => {
    const prefix = uniqueRepoId("candidate-cycle-miss");
    const payload = new TextEncoder().encode("cycle base\n");
    const oid = await computeOid("blob", payload);
    const older = await putIndexedPack({
      packKey: `test/${prefix}-older.pack`,
      packBytes: await buildPack([{ type: "blob", payload }]),
    });
    const newer = await putIndexedPack({
      packKey: `test/${prefix}-newer.pack`,
      packBytes: await buildPack([
        {
          type: "ref-delta",
          baseOid: oid,
          delta: buildCopyPrefixDelta(payload, payload.length),
        },
      ]),
      activeCatalog: [catalogRow(older)],
    });

    const sources = [newer.source, older.source];
    const candidates = collectPackedObjectCandidates(sources, oid);
    let resolved: PackedObjectResult | undefined;
    for (const candidate of candidates) {
      resolved = await materializeWithFirstHitRefs({
        candidate,
        sources,
        cyclePolicy: "miss",
        visited: new Set<string>(),
      });
      if (resolved) break;
    }

    expect(resolved?.packKey).toBe(older.packKey);
    expect(resolved?.payload).toEqual(payload);
  });

  it("throws on recursion cycles when object-store policy is requested", async () => {
    const prefix = uniqueRepoId("candidate-cycle-throw");
    const payload = new TextEncoder().encode("throwing cycle base\n");
    const oid = await computeOid("blob", payload);
    const older = await putIndexedPack({
      packKey: `test/${prefix}-older.pack`,
      packBytes: await buildPack([{ type: "blob", payload }]),
    });
    const newer = await putIndexedPack({
      packKey: `test/${prefix}-newer.pack`,
      packBytes: await buildPack([
        {
          type: "ref-delta",
          baseOid: oid,
          delta: buildCopyPrefixDelta(payload, payload.length),
        },
      ]),
      activeCatalog: [catalogRow(older)],
    });

    const sources = [newer.source, older.source];
    const first = findFirstPackedObjectCandidate(sources, oid);
    if (!first) throw new Error("test failed to find cyclic candidate");

    await expect(
      materializeWithFirstHitRefs({
        candidate: first,
        sources,
        cyclePolicy: "throw",
        visited: new Set<string>(),
      })
    ).rejects.toThrow("pack object recursion cycle");
  });
});
