import type { RepoDurableObject } from "@/index";

import { describe, expect, it } from "vitest";
import { env } from "cloudflare:test";

import { listCommitChangedFiles } from "@/git";
import {
  hasObjectsBatch,
  loadIdxView,
  parseIdxView,
  readObject,
  getNextOffsetByIndex,
} from "@/git/object-store/index.ts";
import { computeNeededFast } from "@/git/operations/fetch/neededFast.ts";
import { hexToBytes } from "@/common/hex.ts";
import {
  callStubWithRetry,
  deleteLooseObjectCopies,
  seedLegacyPackedRepo,
  uniqueRepoId,
} from "./util/test-helpers.ts";
import { buildPack } from "./util/git-pack.ts";
import { buildTreePayload } from "./util/packed-repo.ts";
import { createTestCacheContext, seedPackFirstRepo } from "./util/pack-first.ts";
import { encodeGitObject } from "@/git/core/index.ts";

const UINT32_SPAN = 0x1_0000_0000;

function buildSingleEntryIdx(
  oidHex: string,
  offset: number,
  includeLargeOffset = true
): Uint8Array {
  const count = 1;
  const totalSize =
    8 + 256 * 4 + count * 20 + count * 4 + count * 4 + (includeLargeOffset ? 8 : 0) + 20 + 20;
  const buf = new Uint8Array(totalSize);
  const dv = new DataView(buf.buffer);
  let pos = 0;

  buf[pos++] = 0xff;
  buf[pos++] = 0x74;
  buf[pos++] = 0x4f;
  buf[pos++] = 0x63;
  dv.setUint32(pos, 2, false);
  pos += 4;

  const oidBytes = hexToBytes(oidHex);
  for (let bucket = 0; bucket < 256; bucket++) {
    dv.setUint32(pos, bucket >= oidBytes[0] ? count : 0, false);
    pos += 4;
  }

  buf.set(oidBytes, pos);
  pos += 20;

  dv.setUint32(pos, 0, false);
  pos += 4;

  if (includeLargeOffset) {
    dv.setUint32(pos, 0x80000000, false);
  } else {
    dv.setUint32(pos, 0, false);
  }
  pos += 4;

  if (includeLargeOffset) {
    const hi = Math.floor(offset / UINT32_SPAN);
    const lo = offset % UINT32_SPAN;
    dv.setUint32(pos, hi, false);
    dv.setUint32(pos + 4, lo, false);
  }

  return buf;
}

function makeTracingLimiter(labels: string[]) {
  return {
    async run<T>(label: string, fn: () => Promise<T>): Promise<T> {
      labels.push(label);
      return await fn();
    },
  };
}

describe("pack-first read path closure", () => {
  it("treats empty batch ref results as resolved without per-object fallback reads", async () => {
    const repo = uniqueRepoId("pack-needed-fast-empty-refs");
    const repoId = `o/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    await deleteLooseObjectCopies(env, seeded.getStub, seeded.objectOids);

    const cacheCtx = createTestCacheContext(`https://example.com/${repoId}/git-upload-pack`);
    const needed = await computeNeededFast(env, repoId, [seeded.nextTree.oid], [], cacheCtx);

    expect(new Set(needed)).toEqual(new Set([seeded.nextTree.oid, seeded.nextBlob.oid]));
    expect(cacheCtx.memo?.refs?.get(seeded.nextBlob.oid)).toEqual([]);
    expect(cacheCtx.memo?.objects?.has(seeded.nextBlob.oid)).not.toBe(true);
  });

  it("keeps loose-only wants as partial results instead of inventing compatibility refs", async () => {
    const repo = uniqueRepoId("pack-needed-fast-pack-only");
    const repoId = `o/${repo}`;
    const getStub = () =>
      env.REPO_DO.get(env.REPO_DO.idFromName(repoId)) as DurableObjectStub<RepoDurableObject>;
    const author = "You <you@example.com> 0 +0000";

    const blobPayload = new TextEncoder().encode("pack only\n");
    const blob = await encodeGitObject("blob", blobPayload);
    const treePayload = buildTreePayload([{ mode: "100644", name: "README.md", oid: blob.oid }]);
    const tree = await encodeGitObject("tree", treePayload);
    const commitPayload = new TextEncoder().encode(
      `tree ${tree.oid}\n` + `author ${author}\n` + `committer ${author}\n\n` + `pack only commit\n`
    );
    const commit = await encodeGitObject("commit", commitPayload);

    const packBytes = await buildPack([
      { type: "blob", payload: blobPayload },
      { type: "tree", payload: treePayload },
    ]);

    await seedLegacyPackedRepo({
      env,
      repoId,
      getStub,
      packs: [{ name: "pack-needed-fast-pack-only.pack", packBytes }],
      refs: [{ name: "refs/heads/main", oid: commit.oid }],
      head: { target: "refs/heads/main", oid: commit.oid },
      looseObjects: [commit],
    });
    const cacheCtx = createTestCacheContext(`https://example.com/${repoId}/git-upload-pack`);
    const needed = await computeNeededFast(env, repoId, [commit.oid], [], cacheCtx);

    expect(needed).toEqual([commit.oid]);
    expect(cacheCtx.memo?.refs?.has(commit.oid)).not.toBe(true);
  });

  it("coalesces concurrent idx loads and only decrements the request budget once per pack", async () => {
    const repo = uniqueRepoId("pack-idx-coalesce");
    const repoId = `o/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    const activeCatalog = await callStubWithRetry(seeded.getStub, (stub) =>
      stub.getActivePackCatalog()
    );

    const cacheCtx = createTestCacheContext(`https://example.com/${repoId}/git-upload-pack`, 10);
    cacheCtx.memo = {
      ...(cacheCtx.memo || {}),
      packCatalog: activeCatalog,
    };

    const found = await hasObjectsBatch(
      env,
      repoId,
      [
        seeded.baseBlob.oid,
        seeded.baseTree.oid,
        seeded.baseCommit.oid,
        seeded.nextBlob.oid,
        seeded.nextTree.oid,
        seeded.nextCommit.oid,
      ],
      cacheCtx
    );

    expect(found).toEqual([true, true, true, true, true, true]);
    // The idx loader now uses packCatalog.packBytes as the pack-size hint,
    // so a cold load for this single seeded pack only spends the idx fetch budget.
    expect(cacheCtx.memo?.subreqBudget).toBe(9);
  });

  it("reuses hinted idx loads across requests without another R2 fetch", async () => {
    const repo = uniqueRepoId("pack-idx-cross-request");
    const repoId = `o/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    const activeCatalog = await callStubWithRetry(seeded.getStub, (stub) =>
      stub.getActivePackCatalog()
    );
    const pack = activeCatalog[0]!;

    const warmLabels: string[] = [];
    const warmCtx = createTestCacheContext(`https://example.com/${repoId}/git-upload-pack`, 10);
    warmCtx.memo = {
      ...(warmCtx.memo || {}),
      limiter: makeTracingLimiter(warmLabels),
    };

    const warmed = await loadIdxView(env, pack.packKey, warmCtx, pack.packBytes);

    expect(warmed).toBeDefined();
    expect(warmLabels).toEqual(["r2:get-pack-idx"]);
    expect(warmCtx.memo?.subreqBudget).toBe(9);

    const reuseLabels: string[] = [];
    const reuseCtx = createTestCacheContext(`https://example.com/${repoId}/git-upload-pack`, 10);
    reuseCtx.memo = {
      ...(reuseCtx.memo || {}),
      limiter: makeTracingLimiter(reuseLabels),
    };

    const reused = await loadIdxView(env, pack.packKey, reuseCtx, pack.packBytes);

    expect(reused).toBeDefined();
    expect(reuseLabels).toEqual([]);
    expect(reuseCtx.memo?.subreqBudget).toBe(10);
  });

  it("reads a packed base object with one coalesced range read", async () => {
    const repo = uniqueRepoId("pack-read-single-range");
    const repoId = `o/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    await deleteLooseObjectCopies(env, seeded.getStub, seeded.objectOids);
    const activeCatalog = await callStubWithRetry(seeded.getStub, (stub) =>
      stub.getActivePackCatalog()
    );

    const labels: string[] = [];
    const cacheCtx = createTestCacheContext(
      `https://example.com/${repoId}/blob/${seeded.nextBlob.oid}`,
      20
    );
    cacheCtx.memo = {
      ...(cacheCtx.memo || {}),
      packCatalog: activeCatalog,
      limiter: makeTracingLimiter(labels),
    };

    const obj = await readObject(env, repoId, seeded.nextBlob.oid, cacheCtx);

    expect(obj?.type).toBe("blob");
    expect(labels.filter((label) => label === "r2:get-range")).toHaveLength(1);
    expect(cacheCtx.memo?.subreqBudget).toBe(18);
  });

  it("does not let a bad pack-size hint poison later idx loads", async () => {
    const repo = uniqueRepoId("pack-idx-hint-poison");
    const repoId = `o/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    const activeCatalog = await callStubWithRetry(seeded.getStub, (stub) =>
      stub.getActivePackCatalog()
    );
    const pack = activeCatalog[0]!;

    const poisonCtx = createTestCacheContext(`https://example.com/${repoId}/git-upload-pack`);
    const poisoned = await loadIdxView(env, pack.packKey, poisonCtx, pack.packBytes - 7);
    expect(poisoned).toBeDefined();
    const poisonedLastPackIndex = poisoned!.sortedOffsetIndices[poisoned!.count - 1]!;
    expect(getNextOffsetByIndex(poisoned!, poisonedLastPackIndex)).toBe(pack.packBytes - 27);

    const cleanCtx = createTestCacheContext(`https://example.com/${repoId}/git-upload-pack`);
    const healed = await loadIdxView(env, pack.packKey, cleanCtx, pack.packBytes);

    expect(healed).toBeDefined();
    const healedLastPackIndex = healed!.sortedOffsetIndices[healed!.count - 1]!;
    expect(getNextOffsetByIndex(healed!, healedLastPackIndex)).toBe(pack.packBytes - 20);
  });

  it("parses 64-bit idx offsets without truncating them to 32 bits", () => {
    const oidHex = "12".repeat(20);
    const offset = UINT32_SPAN + 123;
    const packSize = offset + 4096;
    const idxBuf = buildSingleEntryIdx(oidHex, offset);

    const view = parseIdxView("test/large-offset.pack", idxBuf, packSize);

    expect(view).toBeDefined();
    expect(view!.offsets[0]).toBe(offset);
    expect(view!.sortedOffsets[0]).toBe(offset);
    expect(getNextOffsetByIndex(view!, 0)).toBe(packSize - 20);
  });

  it("rejects truncated 64-bit idx offset tables cleanly", () => {
    const oidHex = "34".repeat(20);
    const offset = UINT32_SPAN + 456;
    const packSize = offset + 1024;
    const idxBuf = buildSingleEntryIdx(oidHex, offset, false);
    const dv = new DataView(idxBuf.buffer);
    const offsetTablePos = 8 + 256 * 4 + 20 + 4;
    dv.setUint32(offsetTablePos, 0x80000000, false);

    expect(parseIdxView("test/truncated-large-offset.pack", idxBuf, packSize)).toBeUndefined();
  });

  it("rejects 64-bit idx offsets above the safe integer range", () => {
    const oidHex = "56".repeat(20);
    const idxBuf = buildSingleEntryIdx(oidHex, UINT32_SPAN + 1);
    const dv = new DataView(idxBuf.buffer);
    const largeOffsetPos = 8 + 256 * 4 + 20 + 4 + 4;
    dv.setUint32(largeOffsetPos, 0x00200000, false);
    dv.setUint32(largeOffsetPos + 4, 1, false);

    expect(() =>
      parseIdxView("test/unsafe-large-offset.pack", idxBuf, Number.MAX_SAFE_INTEGER)
    ).toThrow(/safe integer support/);
  });

  it("marks packed diff reads as soft-budget truncated once packed subrequests are counted", async () => {
    const repo = uniqueRepoId("pack-soft-budget");
    const repoId = `o/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    await deleteLooseObjectCopies(env, seeded.getStub, seeded.objectOids);

    const cacheCtx = createTestCacheContext(
      `https://example.com/${repoId}/commit/${seeded.nextCommit.oid}`,
      0
    );
    const diff = await listCommitChangedFiles(env, repoId, seeded.nextCommit.oid, cacheCtx, {
      timeBudgetMs: 2000,
    });

    expect(diff.truncated).toBe(true);
    expect(diff.truncateReason).toBe("soft_budget");
  });
});
