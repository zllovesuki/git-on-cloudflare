#!/usr/bin/env -S npx tsx
/**
 * Validation script for the streaming pack indexer against the real fixture.
 *
 * Tests:
 * - Generated idx matches Git's own output byte-for-byte
 * - Subrequest count stays within the receive-path indexer budget (5,000)
 * - Reports timing and subrequest stats
 *
 * Usage:
 *   npx tsx test/scripts/validate-fixture-indexer.ts
 *
 * This script creates a mock R2 bucket backed by local files so the indexer
 * can be tested outside the Cloudflare Workers runtime.
 */

import * as fs from "node:fs";
import * as path from "node:path";
import * as nodeCrypto from "node:crypto";

// Polyfill crypto.DigestStream for Node.js (Cloudflare Workers-specific API).
// This creates a WritableStream that computes a hash digest of all written data.
if (!(globalThis.crypto as unknown as Record<string, unknown>).DigestStream) {
  class DigestStreamPolyfill extends WritableStream<Uint8Array> {
    digest: Promise<ArrayBuffer>;
    constructor(algorithm: string) {
      const alg = algorithm.replace("-", "").toLowerCase(); // "SHA-1" -> "sha1"
      const hash = nodeCrypto.createHash(alg);
      let resolveDigest: (value: ArrayBuffer) => void;
      const digestPromise = new Promise<ArrayBuffer>((resolve) => {
        resolveDigest = resolve;
      });
      super({
        write(chunk) {
          hash.update(chunk);
        },
        close() {
          const buf = hash.digest();
          resolveDigest!(buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength));
        },
      });
      this.digest = digestPromise;
    }
  }
  (globalThis.crypto as unknown as Record<string, unknown>).DigestStream = DigestStreamPolyfill;
}

// Dynamic import to pick up the project's path aliases via tsx
const { scanPack, resolveDeltasAndWriteIdx } = await import("../../src/git/pack/indexer/index.ts");
const { SubrequestLimiter } = await import("../../src/git/operations/limits.ts");
const { createLogger } = await import("../../src/common/logger.ts");

const FIXTURE_DIR = path.resolve(import.meta.dirname ?? ".", "../../uncommitted-fixture");
const PACK_NAME = "pack-395a180893e59dad8ef9d7fa135ecd8b1b399bb1";
const PACK_PATH = path.join(FIXTURE_DIR, `${PACK_NAME}.pack`);
const IDX_PATH = path.join(FIXTURE_DIR, `${PACK_NAME}.idx`);

const R2_PACK_KEY = `test/fixture/${PACK_NAME}.pack`;
// The platform hard cap is 10,000 subrequests per Worker invocation. The old
// 900 "soft budget" was set when reads went through DO RPCs + loose objects.
// For the streaming receive path (which reads directly from R2), the indexer
// gets a larger share. We budget 5,000 for the indexer so the remaining ~5,000
// is available for connectivity checks, catalog loads, and other receive-path
// operations.
const SUBREQUEST_BUDGET = 5_000;

// ---------------------------------------------------------------------------
// Mock R2 bucket backed by local file system
// ---------------------------------------------------------------------------

class MockR2Object {
  key: string;
  private data: Uint8Array;
  size: number;
  constructor(key: string, data: Uint8Array, size: number) {
    this.key = key;
    this.data = data;
    this.size = size;
  }

  async arrayBuffer(): Promise<ArrayBuffer> {
    return this.data.buffer.slice(
      this.data.byteOffset,
      this.data.byteOffset + this.data.byteLength
    ) as ArrayBuffer;
  }
}

function createMockBucket(files: Map<string, Uint8Array>) {
  return {
    async get(
      key: string,
      opts?: { range?: { offset: number; length: number } }
    ): Promise<MockR2Object | null> {
      const data = files.get(key);
      if (!data) return null;
      if (opts?.range) {
        const { offset, length } = opts.range;
        const slice = data.subarray(offset, offset + length);
        return new MockR2Object(key, new Uint8Array(slice), slice.length);
      }
      return new MockR2Object(key, data, data.length);
    },
    async head(key: string): Promise<{ size: number } | null> {
      const data = files.get(key);
      return data ? { size: data.length } : null;
    },
    async put(key: string, data: Uint8Array | ArrayBuffer): Promise<void> {
      const bytes = data instanceof Uint8Array ? data : new Uint8Array(data);
      files.set(key, bytes);
    },
    async delete(_key: string): Promise<void> {},
  };
}

type MemorySnapshot = {
  heapUsed: number;
  rss: number;
};

function takeMemorySnapshot(): MemorySnapshot {
  const usage = process.memoryUsage();
  return {
    heapUsed: usage.heapUsed,
    rss: usage.rss,
  };
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  if (!fs.existsSync(PACK_PATH) || !fs.existsSync(IDX_PATH)) {
    console.error(`Fixture files not found in ${FIXTURE_DIR}`);
    process.exit(1);
  }

  const packData = new Uint8Array(fs.readFileSync(PACK_PATH));
  const expectedIdx = new Uint8Array(fs.readFileSync(IDX_PATH));

  console.log(
    `Pack: ${packData.byteLength} bytes (${(packData.byteLength / 1024 / 1024).toFixed(1)} MiB)`
  );
  console.log(
    `Idx:  ${expectedIdx.byteLength} bytes (${(expectedIdx.byteLength / 1024 / 1024).toFixed(1)} MiB)`
  );

  // Create mock R2 bucket with the fixture pack.
  const files = new Map<string, Uint8Array>();
  files.set(R2_PACK_KEY, packData);

  const env = { REPO_BUCKET: createMockBucket(files) } as unknown as Env;
  const log = createLogger("info", { service: "FixtureValidator" });
  const limiter = new SubrequestLimiter(6);
  const counter = { count: 0 };
  const countSub = (n = 1) => {
    counter.count += n;
  };

  // ---- Memory baseline (after loading fixture, before indexer) ----
  // The fixture pack and idx are intentionally loaded before this snapshot so
  // the delta isolates the indexer itself rather than fixture setup cost.
  global.gc?.(); // --expose-gc makes this available
  const before = takeMemorySnapshot();

  // ---- Scan ----
  console.log("\nScanning pack...");
  const scanStart = Date.now();
  const scanResult = await scanPack({
    env,
    packKey: R2_PACK_KEY,
    packSize: packData.byteLength,
    limiter,
    countSubrequest: countSub,
    log,
  });
  const scanMs = Date.now() - scanStart;
  console.log(`  Objects: ${scanResult.objectCount}`);
  console.log(`  Scan time: ${scanMs}ms`);
  console.log(`  Subrequests so far: ${counter.count}`);

  // ---- Resolve + write idx ----
  console.log("\nResolving deltas and writing idx...");
  const resolveStart = Date.now();
  const resolveResult = await resolveDeltasAndWriteIdx({
    env,
    packKey: R2_PACK_KEY,
    packSize: packData.byteLength,
    limiter,
    countSubrequest: countSub,
    log,
    scanResult,
    repoId: "fixture/test",
    lruBudget: 48 * 1024 * 1024, // 48 MiB — safe with the array-backed payload cache
  });
  const resolveMs = Date.now() - resolveStart;
  const totalMs = scanMs + resolveMs;

  // ---- Memory deltas: measure what remained resident after indexer work ----
  // heapUsed best approximates the memory pressure we care about on Workers.
  // RSS is still useful to inspect Node process growth, but it also includes
  // V8 and runtime pages that do not map cleanly to Workers.
  global.gc?.();
  const after = takeMemorySnapshot();
  const heapDelta = after.heapUsed - before.heapUsed;
  const rssDelta = after.rss - before.rss;

  // ---- Read generated idx ----
  const idxKey = R2_PACK_KEY.replace(/\.pack$/, ".idx");
  const generatedIdx = files.get(idxKey);
  if (!generatedIdx) {
    console.error("ERROR: No generated idx found in mock R2");
    process.exit(1);
  }

  // ---- Compare byte-for-byte ----
  console.log("\nComparing idx...");
  if (generatedIdx.byteLength !== expectedIdx.byteLength) {
    console.error(
      `ERROR: Size mismatch: generated ${generatedIdx.byteLength}, expected ${expectedIdx.byteLength}`
    );
    process.exit(1);
  }

  let firstMismatch = -1;
  for (let i = 0; i < expectedIdx.byteLength; i++) {
    if (generatedIdx[i] !== expectedIdx[i]) {
      firstMismatch = i;
      break;
    }
  }

  if (firstMismatch !== -1) {
    console.error(`ERROR: Idx mismatch at byte offset ${firstMismatch}`);
    const ctx = 16;
    const start = Math.max(0, firstMismatch - ctx);
    const end = Math.min(expectedIdx.byteLength, firstMismatch + ctx);
    console.error(
      `  Expected: ${Array.from(expectedIdx.subarray(start, end))
        .map((b) => b.toString(16).padStart(2, "0"))
        .join(" ")}`
    );
    console.error(
      `  Got:      ${Array.from(generatedIdx.subarray(start, end))
        .map((b) => b.toString(16).padStart(2, "0"))
        .join(" ")}`
    );
    process.exit(1);
  }

  // ---- Report ----
  // Compute the indexer's typed-array footprint (the part we control).
  const entryTableBytes =
    scanResult.table.offsets.byteLength +
    scanResult.table.types.byteLength +
    scanResult.table.headerLens.byteLength +
    scanResult.table.spanEnds.byteLength +
    scanResult.table.crc32s.byteLength +
    scanResult.table.oids.byteLength +
    scanResult.table.decompressedSizes.byteLength +
    scanResult.table.ofsBaseOffsets.byteLength +
    scanResult.table.resolved.byteLength;

  const scanAuxBytes = scanResult.refBaseOids.byteLength;

  const idxViewBytes =
    resolveResult.idxView.fanout.byteLength +
    resolveResult.idxView.rawNames.byteLength +
    resolveResult.idxView.offsets.byteLength +
    resolveResult.idxView.nextOffsetByIndex.byteLength +
    resolveResult.idxView.sortedOffsets.byteLength +
    resolveResult.idxView.sortedOffsetIndices.byteLength;

  // V8 heap gives the best approximation of what the indexer actually uses.
  // RSS includes V8 engine, JIT code, and Node.js internals that don't exist
  // on Cloudflare Workers, so it overstates the indexer's cost.
  // heapUsed is the closest proxy for the Workers runtime memory footprint.
  const mem = process.memoryUsage();
  const heapUsedMiB = mem.heapUsed / 1024 / 1024;
  const rssMiB = mem.rss / 1024 / 1024;

  console.log("\n=== Results ===");
  console.log(`Objects:         ${resolveResult.objectCount}`);
  console.log(`Scan time:       ${scanMs}ms`);
  console.log(`Resolve time:    ${resolveMs}ms`);
  console.log(`Total time:      ${totalMs}ms`);
  console.log(`Subrequests:     ${counter.count} / ${SUBREQUEST_BUDGET} budget`);
  console.log(`Idx size:        ${resolveResult.idxBytes} bytes`);
  console.log(
    `Entry table:     ${entryTableBytes} bytes (${(entryTableBytes / 1024 / 1024).toFixed(2)} MiB)`
  );
  console.log(
    `Scan aux:        ${scanAuxBytes} bytes (${(scanAuxBytes / 1024 / 1024).toFixed(2)} MiB)`
  );
  console.log(
    `IdxView:         ${idxViewBytes} bytes (${(idxViewBytes / 1024 / 1024).toFixed(2)} MiB)`
  );
  console.log(
    `Typed arrays:    ${((entryTableBytes + scanAuxBytes + idxViewBytes) / 1024 / 1024).toFixed(2)} MiB (indexer-controlled)`
  );
  console.log(
    `Heap delta:      ${(heapDelta / 1024 / 1024).toFixed(1)} MiB (indexer allocation, run with --expose-gc for accuracy)`
  );
  console.log(
    `RSS delta:       ${(rssDelta / 1024 / 1024).toFixed(1)} MiB (Node resident-set growth, includes runtime overhead)`
  );
  console.log(`Heap used:       ${heapUsedMiB.toFixed(1)} MiB (best proxy for Workers memory)`);
  console.log(
    `RSS:             ${rssMiB.toFixed(1)} MiB (includes V8/Node overhead, not meaningful for Workers)`
  );
  console.log(`Idx match:       PASS`);

  if (counter.count >= SUBREQUEST_BUDGET) {
    console.error(`FAIL: Subrequest count ${counter.count} exceeds budget ${SUBREQUEST_BUDGET}`);
    process.exit(1);
  }
  console.log(`Budget:          PASS (${counter.count} < ${SUBREQUEST_BUDGET})`);
  console.log("\nAll validations passed.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
