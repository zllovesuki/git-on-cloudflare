import { describe, expect, it } from "vitest";
import { env } from "cloudflare:test";

import { buildPack, buildAppendOnlyDelta, makeCommit, makeTree } from "./util/git-pack.ts";
import {
  makeLimiter,
  packIndexerLog as log,
  rewritePackChecksum,
  setSingleBytePackHeaderSize,
} from "./util/pack-indexer.helpers.ts";

import { scanPack } from "@/git/pack/indexer/index.ts";
import { computeOid } from "@/git/core/objects.ts";
import { bytesToHex } from "@/common/hex.ts";

describe("scanPack", () => {
  it("indexes a pack with non-delta objects", async () => {
    const tree = await makeTree();
    const commit = await makeCommit(tree.oid, "initial commit");
    const blobPayload = new TextEncoder().encode("hello world\n");
    const blobOid = await computeOid("blob", blobPayload);

    const packBytes = await buildPack([
      { type: "blob", payload: blobPayload },
      { type: "tree", payload: tree.payload },
      { type: "commit", payload: commit.payload },
    ]);

    const packKey = "test/scan-basic.pack";
    await env.REPO_BUCKET.put(packKey, packBytes);
    const head = await env.REPO_BUCKET.head(packKey);

    const result = await scanPack({
      env,
      packKey,
      packSize: head!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
    });

    expect(result.objectCount).toBe(3);
    expect(result.resolvedCount).toBe(3);
    expect(result.refDeltaCount).toBe(0);

    for (let i = 0; i < 3; i++) {
      expect(result.table.resolved[i]).toBe(1);
      expect(result.table.crc32s[i]).not.toBe(0);
    }

    const oids = [];
    for (let i = 0; i < 3; i++) {
      oids.push(bytesToHex(result.table.oids.subarray(i * 20, i * 20 + 20)));
    }
    expect(oids).toContain(blobOid);
    expect(oids).toContain(tree.oid);
    expect(oids).toContain(commit.oid);

    expect(result.table.types[0]).toBe(3);
    expect(result.table.types[1]).toBe(2);
    expect(result.table.types[2]).toBe(1);
    expect(result.packChecksum.length).toBe(20);
  });

  it("rejects absurd pack object counts before allocating entry tables", async () => {
    const blobPayload = new TextEncoder().encode("guarded header count\n");
    const packBytes = await buildPack([{ type: "blob", payload: blobPayload }]);
    const mutated = await rewritePackChecksum(packBytes, (next) => {
      const dv = new DataView(next.buffer, next.byteOffset, next.byteLength);
      dv.setUint32(8, 1_000_000, false);
    });

    const packKey = "test/scan-object-count-guard.pack";
    await env.REPO_BUCKET.put(packKey, mutated);
    const head = await env.REPO_BUCKET.head(packKey);

    await expect(
      scanPack({
        env,
        packKey,
        packSize: head!.size,
        limiter: makeLimiter(),
        countSubrequest: () => {},
        log,
      })
    ).rejects.toThrow(/safe isolate limit/);
  });

  it("handles packs when 1-byte range reads split the zlib header and trailer", async () => {
    const blobPayload = new TextEncoder().encode("chunk-boundary payload\n".repeat(8));
    const blobOid = await computeOid("blob", blobPayload);
    const packBytes = await buildPack([{ type: "blob", payload: blobPayload }]);

    const packKey = "test/scan-byte-chunks.pack";
    await env.REPO_BUCKET.put(packKey, packBytes);
    const head = await env.REPO_BUCKET.head(packKey);

    const result = await scanPack({
      env,
      packKey,
      packSize: head!.size,
      chunkSize: 1,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
    });

    expect(result.objectCount).toBe(1);
    expect(result.table.resolved[0]).toBe(1);
    expect(bytesToHex(result.table.oids.subarray(0, 20))).toBe(blobOid);
    expect(result.table.spanEnds[0]).toBe(head!.size - 20);
  });

  it("rejects a base object whose pack header size disagrees with the inflated payload", async () => {
    const blobPayload = new TextEncoder().encode("tiny");
    const packBytes = await buildPack([{ type: "blob", payload: blobPayload }]);
    const mutated = await rewritePackChecksum(packBytes, (next) => {
      setSingleBytePackHeaderSize(next, 12, 3, blobPayload.length + 1);
    });

    const packKey = "test/scan-bad-blob-size.pack";
    await env.REPO_BUCKET.put(packKey, mutated);
    const head = await env.REPO_BUCKET.head(packKey);

    await expect(
      scanPack({
        env,
        packKey,
        packSize: head!.size,
        limiter: makeLimiter(),
        countSubrequest: () => {},
        log,
      })
    ).rejects.toThrow(/size mismatch/);
  });

  it("rejects a delta whose pack header size disagrees with the delta result size", async () => {
    const baseBlobPayload = new TextEncoder().encode("base");
    const suffix = new TextEncoder().encode("xy");
    const delta = buildAppendOnlyDelta(baseBlobPayload, suffix);

    const packBytes = await buildPack([
      { type: "blob", payload: baseBlobPayload },
      { type: "ofs-delta", baseIndex: 0, delta },
    ]);

    const goodPackKey = "test/scan-good-delta-size.pack";
    await env.REPO_BUCKET.put(goodPackKey, packBytes);
    const goodHead = await env.REPO_BUCKET.head(goodPackKey);
    const goodScan = await scanPack({
      env,
      packKey: goodPackKey,
      packSize: goodHead!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
    });

    const mutated = await rewritePackChecksum(packBytes, (next) => {
      setSingleBytePackHeaderSize(next, goodScan.table.offsets[1], 6, delta.byteLength + 1);
    });

    const badPackKey = "test/scan-bad-delta-size.pack";
    await env.REPO_BUCKET.put(badPackKey, mutated);
    const badHead = await env.REPO_BUCKET.head(badPackKey);

    await expect(
      scanPack({
        env,
        packKey: badPackKey,
        packSize: badHead!.size,
        limiter: makeLimiter(),
        countSubrequest: () => {},
        log,
      })
    ).rejects.toThrow(/size mismatch/);
  });

  it("rejects a pack with corrupted checksum", async () => {
    const tree = await makeTree();
    const packBytes = await buildPack([{ type: "tree", payload: tree.payload }]);
    const corrupted = new Uint8Array(packBytes);
    corrupted[corrupted.length - 1] ^= 0xff;

    const packKey = "test/scan-corrupt.pack";
    await env.REPO_BUCKET.put(packKey, corrupted);
    const head = await env.REPO_BUCKET.head(packKey);

    await expect(
      scanPack({
        env,
        packKey,
        packSize: head!.size,
        limiter: makeLimiter(),
        countSubrequest: () => {},
        log,
      })
    ).rejects.toThrow(/checksum mismatch/);
  });

  it("rejects reserved pack type codes during header parsing", async () => {
    const blobPayload = new TextEncoder().encode("tiny");
    const packBytes = await buildPack([{ type: "blob", payload: blobPayload }]);
    const mutated = await rewritePackChecksum(packBytes, (next) => {
      setSingleBytePackHeaderSize(next, 12, 5, blobPayload.length);
    });

    const packKey = "test/scan-reserved-type.pack";
    await env.REPO_BUCKET.put(packKey, mutated);

    await expect(
      scanPack({
        env,
        packKey,
        packSize: mutated.byteLength,
        limiter: makeLimiter(),
        countSubrequest: () => {},
        log,
      })
    ).rejects.toThrow(/invalid reserved pack type/);
  });

  it("rejects delta result sizes that exceed the supported 32-bit range", async () => {
    const baseBlobPayload = new TextEncoder().encode("base\n");
    const overflowDelta = new Uint8Array([
      baseBlobPayload.length,
      0x80,
      0x80,
      0x80,
      0x80,
      0x10,
      0x00,
    ]);

    const packBytes = await buildPack([
      { type: "blob", payload: baseBlobPayload },
      { type: "ofs-delta", baseIndex: 0, delta: overflowDelta },
    ]);

    const packKey = "test/scan-overflow-delta-size.pack";
    await env.REPO_BUCKET.put(packKey, packBytes);
    const head = await env.REPO_BUCKET.head(packKey);

    await expect(
      scanPack({
        env,
        packKey,
        packSize: head!.size,
        limiter: makeLimiter(),
        countSubrequest: () => {},
        log,
      })
    ).rejects.toThrow(/32-bit size range/);
  });

  it("rejects a corrupted Adler-32 trailer even when the pack checksum is recomputed", async () => {
    const blobPayload = new TextEncoder().encode("adler validation\n");
    const packBytes = await buildPack([{ type: "blob", payload: blobPayload }]);

    const goodPackKey = "test/scan-good-adler.pack";
    await env.REPO_BUCKET.put(goodPackKey, packBytes);
    const goodHead = await env.REPO_BUCKET.head(goodPackKey);
    const goodScan = await scanPack({
      env,
      packKey: goodPackKey,
      packSize: goodHead!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
    });

    const payloadStart = goodScan.table.offsets[0] + goodScan.table.headerLens[0];
    const payloadEnd = goodScan.table.spanEnds[0];
    const mutated = await rewritePackChecksum(packBytes, (next) => {
      for (let i = payloadEnd - 4; i < payloadEnd; i++) {
        next[i] ^= 0xff;
      }
    });

    const packKey = "test/scan-bad-adler.pack";
    await env.REPO_BUCKET.put(packKey, mutated);

    await expect(
      scanPack({
        env,
        packKey,
        packSize: payloadEnd + 20,
        limiter: makeLimiter(),
        countSubrequest: () => {},
        log,
      })
    ).rejects.toThrow(/Adler-32 mismatch/);
    expect(payloadStart).toBeGreaterThan(0);
  });

  it("rejects zlib preset-dictionary headers before inflating payload bytes", async () => {
    const blobPayload = new TextEncoder().encode("fdict validation\n");
    const packBytes = await buildPack([{ type: "blob", payload: blobPayload }]);

    const goodPackKey = "test/scan-good-fdict.pack";
    await env.REPO_BUCKET.put(goodPackKey, packBytes);
    const goodHead = await env.REPO_BUCKET.head(goodPackKey);
    const goodScan = await scanPack({
      env,
      packKey: goodPackKey,
      packSize: goodHead!.size,
      limiter: makeLimiter(),
      countSubrequest: () => {},
      log,
    });

    const payloadStart = goodScan.table.offsets[0] + goodScan.table.headerLens[0];
    const mutated = await rewritePackChecksum(packBytes, (next) => {
      const cmf = next[payloadStart];
      const flgBase = (next[payloadStart + 1] | 0x20) & 0xe0;
      const check = (31 - ((cmf * 256 + flgBase) % 31)) % 31;
      next[payloadStart + 1] = flgBase | check;
    });

    const packKey = "test/scan-fdict.pack";
    await env.REPO_BUCKET.put(packKey, mutated);

    await expect(
      scanPack({
        env,
        packKey,
        packSize: mutated.byteLength,
        limiter: makeLimiter(),
        countSubrequest: () => {},
        log,
      })
    ).rejects.toThrow(/preset dictionaries/);
  });

  it("rejects undeclared bytes between the last object and the pack trailer", async () => {
    const blobPayload = new TextEncoder().encode("trailing junk validation\n");
    const packBytes = await buildPack([{ type: "blob", payload: blobPayload }]);
    const junk = new Uint8Array([0xde, 0xad, 0xbe, 0xef]);
    const mutated = new Uint8Array(packBytes.length + junk.length);
    const trailerStart = packBytes.length - 20;

    mutated.set(packBytes.subarray(0, trailerStart), 0);
    mutated.set(junk, trailerStart);
    // Keep the original trailer hash in place. The scanner must reject the pack
    // because the final object no longer ends where the trailer begins.
    mutated.set(packBytes.subarray(trailerStart), trailerStart + junk.length);

    const packKey = "test/scan-trailing-junk.pack";
    await env.REPO_BUCKET.put(packKey, mutated);
    const head = await env.REPO_BUCKET.head(packKey);

    await expect(
      scanPack({
        env,
        packKey,
        packSize: head!.size,
        limiter: makeLimiter(),
        countSubrequest: () => {},
        log,
      })
    ).rejects.toThrow(/expected indexed entries to end/);
  });
});
