/**
 * Fixture-based validation for the streaming pack indexer.
 *
 * Validates the indexer against the real 42 MiB fixture pack in
 * uncommitted-fixture/ when present. Skipped when the fixture is not available.
 *
 * Fixture files are read from disk via the FIXTURE_READER service binding
 * (defined in vitest.config.ts), which runs in Node.js and returns binary data.
 * This avoids the Vite SSR externalization issue with node:fs.
 *
 * The fixture pack is uploaded to R2 via FixedLengthStream because the local
 * harness needs a known-length stream for R2.put(). Real client uploads may
 * still arrive chunked without Content-Length.
 */

import { describe, expect, it } from "vitest";
import { env } from "cloudflare:test";

import { scanPack, resolveDeltasAndWriteIdx } from "@/git/pack/indexer/index.ts";
import { SubrequestLimiter, DEFAULT_SUBREQUEST_BUDGET } from "@/git/operations/limits.ts";
import { createLogger } from "@/common/logger.ts";
import { packIndexKey } from "@/keys.ts";
import { buildPack, buildAppendOnlyDelta, makeCommit, makeTree } from "./util/git-pack.ts";
import { computeOid } from "@/git/core/objects.ts";
import { bytesToHex } from "@/common/hex.ts";

const log = createLogger("debug", { service: "PackIndexerFixture" });
const fixtureEnv = env as unknown as {
  FIXTURE_READER: { fetch(req: Request): Promise<Response> };
  PACK_INDEXER_FIXTURE?: string;
};

const FIXTURE_PACK_NAME = "pack-395a180893e59dad8ef9d7fa135ecd8b1b399bb1";
const FIXTURE_PACK_KEY = `test/fixture/${FIXTURE_PACK_NAME}.pack`;

/**
 * Read a fixture file from disk via the FIXTURE_READER service binding.
 * Returns null if the file doesn't exist.
 */
async function readFixtureFile(relativePath: string): Promise<Uint8Array | null> {
  const resp = await fixtureEnv.FIXTURE_READER.fetch(
    new Request(`http://fixture/${encodeURIComponent(relativePath)}`)
  );
  if (!resp.ok) return null;
  return new Uint8Array(await resp.arrayBuffer());
}

describe("pack indexer fixture validation", () => {
  it("indexes a multi-object pack with mixed types and deltas", { timeout: 30_000 }, async () => {
    const blob1 = new TextEncoder().encode("file one content version 1\n");
    const blob2 = new TextEncoder().encode("file two content version 1\n");
    const blob1v2Suffix = new TextEncoder().encode("modified line\n");
    const delta1 = buildAppendOnlyDelta(blob1, blob1v2Suffix);

    const tree = await makeTree();
    const commit1 = await makeCommit(tree.oid, "first commit");
    const commit2Payload = new TextEncoder().encode(
      `tree ${tree.oid}\nparent ${commit1.oid}\nauthor You <you@example.com> 0 +0000\ncommitter You <you@example.com> 0 +0000\n\nsecond commit\n`
    );

    const packBytes = await buildPack([
      { type: "blob", payload: blob1 },
      { type: "blob", payload: blob2 },
      { type: "ofs-delta", baseIndex: 0, delta: delta1 },
      { type: "tree", payload: tree.payload },
      { type: "commit", payload: commit1.payload },
      { type: "commit", payload: commit2Payload },
    ]);

    const packKey = "test/multi-object.pack";
    await env.REPO_BUCKET.put(packKey, packBytes);
    const head = await env.REPO_BUCKET.head(packKey);

    const counter = { count: 0 };
    const limiter = new SubrequestLimiter(6);

    const scanResult = await scanPack({
      env,
      packKey,
      packSize: head!.size,
      limiter,
      countSubrequest: (n = 1) => {
        counter.count += n;
      },
      log,
    });

    expect(scanResult.objectCount).toBe(6);
    expect(scanResult.table.resolved.reduce((a: number, b: number) => a + b, 0)).toBe(5);

    const resolveResult = await resolveDeltasAndWriteIdx({
      env,
      packKey,
      packSize: head!.size,
      limiter,
      countSubrequest: (n = 1) => {
        counter.count += n;
      },
      log,
      scanResult,
      repoId: "test/multi",
    });

    expect(scanResult.table.resolved.reduce((a: number, b: number) => a + b, 0)).toBe(6);
    expect(resolveResult.objectCount).toBe(6);

    const expectedPayload = new Uint8Array(blob1.length + blob1v2Suffix.length);
    expectedPayload.set(blob1, 0);
    expectedPayload.set(blob1v2Suffix, blob1.length);
    const expectedOid = await computeOid("blob", expectedPayload);
    const deltaOid = bytesToHex(scanResult.table.oids.subarray(2 * 20, 3 * 20));
    expect(deltaOid).toBe(expectedOid);

    const idxObj = await env.REPO_BUCKET.get(packIndexKey(packKey));
    expect(idxObj).not.toBeNull();
    expect(counter.count).toBeLessThan(DEFAULT_SUBREQUEST_BUDGET);
  });

  it("generates idx matching fixture byte-for-byte", { timeout: 120_000 }, async () => {
    if (fixtureEnv.PACK_INDEXER_FIXTURE !== "1") {
      console.log("SKIP: set PACK_INDEXER_FIXTURE=1 to run the 42 MiB fixture validation");
      return;
    }

    // Read fixture files via the FIXTURE_READER service binding.
    const fixturePack = await readFixtureFile(`uncommitted-fixture/${FIXTURE_PACK_NAME}.pack`);
    const fixtureIdx = await readFixtureFile(`uncommitted-fixture/${FIXTURE_PACK_NAME}.idx`);
    if (!fixturePack || !fixtureIdx) {
      console.log("SKIP: fixture not found in uncommitted-fixture/");
      return;
    }

    log.info("fixture:loaded", {
      packBytes: fixturePack.byteLength,
      idxBytes: fixtureIdx.byteLength,
    });

    // Upload the fixture through FixedLengthStream because the local harness
    // needs a known-length stream for R2.put(). The integrated receive path
    // still needs to handle real chunked client uploads separately.
    const fls = new FixedLengthStream(fixturePack.byteLength);
    const flsWriter = fls.writable.getWriter();
    const UPLOAD_CHUNK = 512 * 1024;
    const writePromise = (async () => {
      for (let offset = 0; offset < fixturePack.byteLength; offset += UPLOAD_CHUNK) {
        const end = Math.min(offset + UPLOAD_CHUNK, fixturePack.byteLength);
        await flsWriter.write(fixturePack.subarray(offset, end));
      }
      await flsWriter.close();
    })();
    await Promise.all([env.REPO_BUCKET.put(FIXTURE_PACK_KEY, fls.readable), writePromise]);

    const head = await env.REPO_BUCKET.head(FIXTURE_PACK_KEY);
    expect(head).not.toBeNull();
    expect(head!.size).toBe(fixturePack.byteLength);

    // Run the indexer.
    const counter = { count: 0 };
    const limiter = new SubrequestLimiter(6);
    const countSub = (n = 1) => {
      counter.count += n;
    };

    const scanStart = Date.now();
    const scanResult = await scanPack({
      env,
      packKey: FIXTURE_PACK_KEY,
      packSize: head!.size,
      limiter,
      countSubrequest: countSub,
      log,
    });
    const scanMs = Date.now() - scanStart;

    const resolveStart = Date.now();
    const resolveResult = await resolveDeltasAndWriteIdx({
      env,
      packKey: FIXTURE_PACK_KEY,
      packSize: head!.size,
      limiter,
      countSubrequest: countSub,
      log,
      scanResult,
      repoId: "fixture/test",
      // 48 MiB: larger packs need more LRU headroom to avoid excessive R2
      // re-reads. The array-backed payload cache keeps that extra headroom
      // within the worker memory budget for the representative fixture.
      lruBudget: 48 * 1024 * 1024,
    });
    const resolveMs = Date.now() - resolveStart;

    // Compare idx byte-for-byte.
    const idxObj = await env.REPO_BUCKET.get(packIndexKey(FIXTURE_PACK_KEY));
    expect(idxObj).not.toBeNull();
    const generatedIdx = new Uint8Array(await idxObj!.arrayBuffer());

    expect(generatedIdx.byteLength).toBe(fixtureIdx.byteLength);

    let firstMismatch = -1;
    for (let i = 0; i < fixtureIdx.byteLength; i++) {
      if (generatedIdx[i] !== fixtureIdx[i]) {
        firstMismatch = i;
        break;
      }
    }
    expect(firstMismatch).toBe(-1);

    // The platform hard cap is 10,000 subrequests. Budget 5,000 for the
    // indexer, leaving headroom for connectivity checks and other ops.
    expect(counter.count).toBeLessThan(5_000);

    log.info("fixture:stats", {
      objectCount: scanResult.objectCount,
      scanMs,
      resolveMs,
      totalMs: scanMs + resolveMs,
      subrequests: counter.count,
      idxBytes: resolveResult.idxBytes,
    });
  });
});
