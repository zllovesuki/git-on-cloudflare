import { describe, expect, it } from "vitest";
import { env } from "cloudflare:test";

import { concatChunks } from "@/git";
import { bytesToHex, createLogger } from "@/common/index.ts";
import { computeOid } from "@/git/core/objects.ts";
import { rewritePack } from "@/git/pack/rewrite.ts";
import { buildOutputOrder, buildSelection } from "@/git/pack/rewrite/plan.ts";
import type { PackCatalogRow } from "@/do/repo/db/schema.ts";
import { buildAppendOnlyDelta, buildPack } from "./util/test-helpers.ts";
import { indexTestPack } from "./util/test-indexer.ts";

function encodeDeltaVarint(value: number): Uint8Array {
  const out: number[] = [];
  do {
    let byte = value & 0x7f;
    value >>>= 7;
    if (value > 0) byte |= 0x80;
    out.push(byte);
  } while (value > 0);
  return Uint8Array.from(out);
}

function buildCopyPrefixDelta(base: Uint8Array, prefixLength: number): Uint8Array {
  return Uint8Array.from([
    ...encodeDeltaVarint(base.length),
    ...encodeDeltaVarint(prefixLength),
    0x90,
    prefixLength,
  ]);
}

async function readStreamBytes(stream: ReadableStream<Uint8Array>): Promise<Uint8Array> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    if (value) chunks.push(value);
  }
  return concatChunks(chunks);
}

describe("pack rewrite cycles", () => {
  it("skips a cyclic selected REF_DELTA base duplicate and chooses an older alternate", async () => {
    const seedPayload = new TextEncoder().encode("seed\n");
    const aSuffix = new TextEncoder().encode("a\n");
    const aPayload = new Uint8Array(seedPayload.length + aSuffix.length);
    aPayload.set(seedPayload, 0);
    aPayload.set(aSuffix, seedPayload.length);

    const bSuffix = new TextEncoder().encode("b\n");
    const bPayload = new Uint8Array(aPayload.length + bSuffix.length);
    bPayload.set(aPayload, 0);
    bPayload.set(bSuffix, aPayload.length);

    const aOid = await computeOid("blob", aPayload);
    const bOid = await computeOid("blob", bPayload);

    const olderPackBytes = await buildPack([
      { type: "blob", payload: seedPayload },
      { type: "ofs-delta", baseIndex: 0, delta: buildAppendOnlyDelta(seedPayload, aSuffix) },
    ]);
    const middlePackBytes = await buildPack([
      {
        type: "ref-delta",
        baseOid: aOid,
        delta: buildAppendOnlyDelta(aPayload, bSuffix),
      },
    ]);
    const newerPackBytes = await buildPack([
      {
        type: "ref-delta",
        baseOid: bOid,
        delta: buildCopyPrefixDelta(bPayload, aPayload.length),
      },
    ]);

    const olderKey = `test/rewrite-ref-cycle-older-${Date.now()}.pack`;
    const middleKey = `test/rewrite-ref-cycle-middle-${Date.now()}.pack`;
    const newerKey = `test/rewrite-ref-cycle-newer-${Date.now()}.pack`;
    await env.REPO_BUCKET.put(olderKey, olderPackBytes);
    await env.REPO_BUCKET.put(middleKey, middlePackBytes);
    await env.REPO_BUCKET.put(newerKey, newerPackBytes);

    const olderResolve = await indexTestPack(env, olderKey, olderPackBytes.byteLength);
    const olderRow: PackCatalogRow = {
      packKey: olderKey,
      kind: "receive",
      state: "active",
      tier: 0,
      seqLo: 1,
      seqHi: 1,
      objectCount: olderResolve.objectCount,
      packBytes: olderPackBytes.byteLength,
      idxBytes: olderResolve.idxBytes,
      createdAt: Date.now(),
      supersededBy: null,
    };
    const middleResolve = await indexTestPack(env, middleKey, middlePackBytes.byteLength, [
      olderRow,
    ]);
    const middleRow: PackCatalogRow = {
      packKey: middleKey,
      kind: "receive",
      state: "active",
      tier: 0,
      seqLo: 2,
      seqHi: 2,
      objectCount: middleResolve.objectCount,
      packBytes: middlePackBytes.byteLength,
      idxBytes: middleResolve.idxBytes,
      createdAt: Date.now(),
      supersededBy: null,
    };
    const newerResolve = await indexTestPack(env, newerKey, newerPackBytes.byteLength, [
      middleRow,
      olderRow,
    ]);

    const snapshot = {
      packs: [
        { packKey: newerKey, packBytes: newerPackBytes.byteLength, idx: newerResolve.idxView },
        { packKey: middleKey, packBytes: middlePackBytes.byteLength, idx: middleResolve.idxView },
        { packKey: olderKey, packBytes: olderPackBytes.byteLength, idx: olderResolve.idxView },
      ],
    };
    const rewriteOptions = {
      limiter: { run: async <T>(_label: string, fn: () => Promise<T>) => await fn() },
      countSubrequest: () => {},
    };

    const selection = await buildSelection(
      env,
      snapshot,
      [aOid],
      createLogger("error", { service: "test" }),
      new Set(),
      rewriteOptions
    );

    expect(selection).toBeDefined();
    const table = selection!.table;

    let aSel = -1;
    let aCount = 0;
    let bSel = -1;
    let seedSel = -1;
    for (let sel = 0; sel < table.count; sel++) {
      const oid = bytesToHex(table.oidsRaw.subarray(sel * 20, sel * 20 + 20));
      if (oid === aOid) {
        aSel = sel;
        aCount++;
      }
      if (oid === bOid) bSel = sel;
      if (table.typeCodes[sel] < 6 && oid !== aOid && oid !== bOid) seedSel = sel;
    }

    expect(aCount).toBe(1);
    expect(aSel).toBeGreaterThanOrEqual(0);
    expect(bSel).toBeGreaterThanOrEqual(0);
    expect(seedSel).toBeGreaterThanOrEqual(0);
    expect(table.baseSlots[aSel]).toBe(seedSel);
    expect(table.baseSlots[bSel]).toBe(aSel);
    expect(buildOutputOrder(table, createLogger("error", { service: "test" }))).toBe(true);

    const stream = await rewritePack(env, snapshot, [aOid], rewriteOptions);
    expect(stream).toBeDefined();
    const rewrittenPack = await readStreamBytes(stream!);

    const verifyKey = `test/rewrite-ref-cycle-verify-${Date.now()}.pack`;
    await env.REPO_BUCKET.put(verifyKey, rewrittenPack);
    const verify = await indexTestPack(env, verifyKey, rewrittenPack.byteLength);

    const oidSet = new Set<string>();
    for (let i = 0; i < verify.idxView.count; i++) {
      const oidBytes = verify.idxView.rawNames.subarray(i * 20, (i + 1) * 20);
      oidSet.add(bytesToHex(oidBytes));
    }
    expect(oidSet.size).toBe(verify.idxView.count);
  });

  it("materializes a selected REF_DELTA row when a later base edge closes a cycle", async () => {
    const seedPayload = new TextEncoder().encode("seed\n");
    const bSuffix = new TextEncoder().encode("b\n");
    const bPayload = new Uint8Array(seedPayload.length + bSuffix.length);
    bPayload.set(seedPayload, 0);
    bPayload.set(bSuffix, seedPayload.length);

    const aSuffix = new TextEncoder().encode("a\n");
    const aPayload = new Uint8Array(bPayload.length + aSuffix.length);
    aPayload.set(bPayload, 0);
    aPayload.set(aSuffix, bPayload.length);

    const bOid = await computeOid("blob", bPayload);
    const aOid = await computeOid("blob", aPayload);

    const fallbackPackBytes = await buildPack([
      { type: "blob", payload: seedPayload },
      { type: "ofs-delta", baseIndex: 0, delta: buildAppendOnlyDelta(seedPayload, bSuffix) },
    ]);
    const sourceAPackBytes = await buildPack([
      {
        type: "ref-delta",
        baseOid: bOid,
        delta: buildAppendOnlyDelta(bPayload, aSuffix),
      },
    ]);
    const sourceBPackBytes = await buildPack([
      {
        type: "ref-delta",
        baseOid: aOid,
        delta: buildCopyPrefixDelta(aPayload, bPayload.length),
      },
    ]);

    const fallbackKey = `test/rewrite-latent-cycle-fallback-${Date.now()}.pack`;
    const sourceAKey = `test/rewrite-latent-cycle-a-${Date.now()}.pack`;
    const sourceBKey = `test/rewrite-latent-cycle-b-${Date.now()}.pack`;
    await env.REPO_BUCKET.put(fallbackKey, fallbackPackBytes);
    await env.REPO_BUCKET.put(sourceAKey, sourceAPackBytes);
    await env.REPO_BUCKET.put(sourceBKey, sourceBPackBytes);

    const fallbackResolve = await indexTestPack(env, fallbackKey, fallbackPackBytes.byteLength);
    const fallbackRow: PackCatalogRow = {
      packKey: fallbackKey,
      kind: "compact",
      state: "active",
      tier: 1,
      seqLo: 1,
      seqHi: 1,
      objectCount: fallbackResolve.objectCount,
      packBytes: fallbackPackBytes.byteLength,
      idxBytes: fallbackResolve.idxBytes,
      createdAt: Date.now(),
      supersededBy: null,
    };
    const sourceAResolve = await indexTestPack(env, sourceAKey, sourceAPackBytes.byteLength, [
      fallbackRow,
    ]);
    const sourceARow: PackCatalogRow = {
      packKey: sourceAKey,
      kind: "receive",
      state: "active",
      tier: 0,
      seqLo: 2,
      seqHi: 2,
      objectCount: sourceAResolve.objectCount,
      packBytes: sourceAPackBytes.byteLength,
      idxBytes: sourceAResolve.idxBytes,
      createdAt: Date.now(),
      supersededBy: null,
    };
    const sourceBResolve = await indexTestPack(env, sourceBKey, sourceBPackBytes.byteLength, [
      sourceARow,
      fallbackRow,
    ]);

    const snapshot = {
      packs: [
        {
          packKey: sourceAKey,
          packBytes: sourceAPackBytes.byteLength,
          idx: sourceAResolve.idxView,
        },
        {
          packKey: sourceBKey,
          packBytes: sourceBPackBytes.byteLength,
          idx: sourceBResolve.idxView,
        },
        {
          packKey: fallbackKey,
          packBytes: fallbackPackBytes.byteLength,
          idx: fallbackResolve.idxView,
        },
      ],
    };
    const rewriteOptions = {
      limiter: { run: async <T>(_label: string, fn: () => Promise<T>) => await fn() },
      countSubrequest: () => {},
    };

    const selection = await buildSelection(
      env,
      snapshot,
      [aOid, bOid],
      createLogger("error", { service: "test" }),
      new Set(),
      rewriteOptions
    );

    expect(selection).toBeDefined();
    const table = selection!.table;

    let syntheticCount = 0;
    for (let sel = 0; sel < table.count; sel++) {
      if (table.syntheticPayloads[sel]) syntheticCount++;
    }
    expect(syntheticCount).toBe(1);
    expect(buildOutputOrder(table, createLogger("error", { service: "test" }))).toBe(true);

    const stream = await rewritePack(env, snapshot, [aOid, bOid], rewriteOptions);
    expect(stream).toBeDefined();
    const rewrittenPack = await readStreamBytes(stream!);

    const verifyKey = `test/rewrite-latent-cycle-verify-${Date.now()}.pack`;
    await env.REPO_BUCKET.put(verifyKey, rewrittenPack);
    const verify = await indexTestPack(env, verifyKey, rewrittenPack.byteLength);

    const oidSet = new Set<string>();
    for (let i = 0; i < verify.idxView.count; i++) {
      const oidBytes = verify.idxView.rawNames.subarray(i * 20, (i + 1) * 20);
      oidSet.add(bytesToHex(oidBytes));
    }
    expect(oidSet.has(aOid)).toBe(true);
    expect(oidSet.has(bOid)).toBe(true);
    expect(oidSet.size).toBe(verify.idxView.count);
  });

  it("does not create an artificial cycle when an older OFS base has a newer duplicate OID", async () => {
    const basePayload = new TextEncoder().encode("shared\n");
    const suffix = new TextEncoder().encode("extra\n");
    const expandedPayload = new Uint8Array(basePayload.length + suffix.length);
    expandedPayload.set(basePayload, 0);
    expandedPayload.set(suffix, basePayload.length);

    const expandedOid = await computeOid("blob", expandedPayload);
    const baseOid = await computeOid("blob", basePayload);

    const olderPackBytes = await buildPack([
      { type: "blob", payload: basePayload },
      { type: "ofs-delta", baseIndex: 0, delta: buildAppendOnlyDelta(basePayload, suffix) },
    ]);
    const newerPackBytes = await buildPack([
      {
        type: "ref-delta",
        baseOid: expandedOid,
        delta: buildCopyPrefixDelta(expandedPayload, basePayload.length),
      },
    ]);

    const olderKey = `test/rewrite-cycle-older-${Date.now()}.pack`;
    const newerKey = `test/rewrite-cycle-newer-${Date.now()}.pack`;
    await env.REPO_BUCKET.put(olderKey, olderPackBytes);
    await env.REPO_BUCKET.put(newerKey, newerPackBytes);

    const olderResolve = await indexTestPack(env, olderKey, olderPackBytes.byteLength);
    const olderRow: PackCatalogRow = {
      packKey: olderKey,
      kind: "receive",
      state: "active",
      tier: 0,
      seqLo: 1,
      seqHi: 1,
      objectCount: olderResolve.objectCount,
      packBytes: olderPackBytes.byteLength,
      idxBytes: olderResolve.idxBytes,
      createdAt: Date.now(),
      supersededBy: null,
    };
    const newerResolve = await indexTestPack(env, newerKey, newerPackBytes.byteLength, [olderRow]);

    const selection = await buildSelection(
      env,
      {
        packs: [
          { packKey: newerKey, packBytes: newerPackBytes.byteLength, idx: newerResolve.idxView },
          { packKey: olderKey, packBytes: olderPackBytes.byteLength, idx: olderResolve.idxView },
        ],
      },
      [expandedOid, baseOid],
      createLogger("error", { service: "test" }),
      new Set(),
      {
        limiter: { run: async (_label, fn) => await fn() },
        countSubrequest: () => {},
      }
    );

    expect(selection).toBeDefined();
    const table = selection!.table;
    expect(Array.from(table.baseSlots.subarray(0, table.count))).toEqual([1, -1]);
    expect(buildOutputOrder(table, createLogger("error", { service: "test" }))).toBe(true);
  });

  it("redirects duplicate full-object bases to the existing OID owner", async () => {
    const basePayload = new TextEncoder().encode("shared base\n");
    const suffix = new TextEncoder().encode("child\n");
    const childPayload = new Uint8Array(basePayload.length + suffix.length);
    childPayload.set(basePayload, 0);
    childPayload.set(suffix, basePayload.length);

    const baseOid = await computeOid("blob", basePayload);
    const childOid = await computeOid("blob", childPayload);

    const olderPackBytes = await buildPack([{ type: "blob", payload: basePayload }]);
    const newerPackBytes = await buildPack([
      { type: "blob", payload: basePayload },
      { type: "ofs-delta", baseIndex: 0, delta: buildAppendOnlyDelta(basePayload, suffix) },
    ]);

    const olderKey = `test/rewrite-dup-full-older-${Date.now()}.pack`;
    const newerKey = `test/rewrite-dup-full-newer-${Date.now()}.pack`;
    await env.REPO_BUCKET.put(olderKey, olderPackBytes);
    await env.REPO_BUCKET.put(newerKey, newerPackBytes);

    const olderResolve = await indexTestPack(env, olderKey, olderPackBytes.byteLength);
    const newerResolve = await indexTestPack(env, newerKey, newerPackBytes.byteLength);

    const selection = await buildSelection(
      env,
      {
        packs: [
          { packKey: olderKey, packBytes: olderPackBytes.byteLength, idx: olderResolve.idxView },
          { packKey: newerKey, packBytes: newerPackBytes.byteLength, idx: newerResolve.idxView },
        ],
      },
      [baseOid, childOid],
      createLogger("error", { service: "test" }),
      new Set(),
      {
        limiter: { run: async (_label, fn) => await fn() },
        countSubrequest: () => {},
      }
    );

    expect(selection).toBeDefined();
    const table = selection!.table;
    expect(table.count).toBe(2);
    expect(Array.from(table.baseSlots.subarray(0, table.count))).toEqual([-1, 0]);
    expect(buildOutputOrder(table, createLogger("error", { service: "test" }))).toBe(true);
  });

  it("keeps an OFS base row live when a newer duplicate delta already owns the OID", async () => {
    const seedPayload = new TextEncoder().encode("seed\n");
    const xSuffix = new TextEncoder().encode("x\n");
    const xPayload = new Uint8Array(seedPayload.length + xSuffix.length);
    xPayload.set(seedPayload, 0);
    xPayload.set(xSuffix, seedPayload.length);

    const ySuffix = new TextEncoder().encode("y\n");
    const yPayload = new Uint8Array(xPayload.length + ySuffix.length);
    yPayload.set(xPayload, 0);
    yPayload.set(ySuffix, xPayload.length);

    const seedOid = await computeOid("blob", seedPayload);
    const xOid = await computeOid("blob", xPayload);
    const yOid = await computeOid("blob", yPayload);

    const olderPackBytes = await buildPack([
      { type: "blob", payload: seedPayload },
      { type: "ofs-delta", baseIndex: 0, delta: buildAppendOnlyDelta(seedPayload, xSuffix) },
      { type: "ofs-delta", baseIndex: 1, delta: buildAppendOnlyDelta(xPayload, ySuffix) },
    ]);
    const newerPackBytes = await buildPack([
      {
        type: "ref-delta",
        baseOid: yOid,
        delta: buildCopyPrefixDelta(yPayload, xPayload.length),
      },
    ]);

    const olderKey = `test/rewrite-ofs-base-owner-older-${Date.now()}.pack`;
    const newerKey = `test/rewrite-ofs-base-owner-newer-${Date.now()}.pack`;
    await env.REPO_BUCKET.put(olderKey, olderPackBytes);
    await env.REPO_BUCKET.put(newerKey, newerPackBytes);

    const olderResolve = await indexTestPack(env, olderKey, olderPackBytes.byteLength);
    const olderRow: PackCatalogRow = {
      packKey: olderKey,
      kind: "receive",
      state: "active",
      tier: 0,
      seqLo: 1,
      seqHi: 1,
      objectCount: olderResolve.objectCount,
      packBytes: olderPackBytes.byteLength,
      idxBytes: olderResolve.idxBytes,
      createdAt: Date.now(),
      supersededBy: null,
    };
    const newerResolve = await indexTestPack(env, newerKey, newerPackBytes.byteLength, [olderRow]);

    const selection = await buildSelection(
      env,
      {
        packs: [
          { packKey: newerKey, packBytes: newerPackBytes.byteLength, idx: newerResolve.idxView },
          { packKey: olderKey, packBytes: olderPackBytes.byteLength, idx: olderResolve.idxView },
        ],
      },
      [xOid, yOid],
      createLogger("error", { service: "test" }),
      new Set(),
      {
        limiter: { run: async (_label, fn) => await fn() },
        countSubrequest: () => {},
      }
    );

    expect(selection).toBeDefined();
    const table = selection!.table;

    let olderXSel = -1;
    let olderYSel = -1;
    let olderSeedSel = -1;
    for (let sel = 0; sel < table.count; sel++) {
      const oid = bytesToHex(table.oidsRaw.subarray(sel * 20, sel * 20 + 20));
      if (table.packSlots[sel] === 1 && oid === xOid) olderXSel = sel;
      if (table.packSlots[sel] === 1 && oid === seedOid) olderSeedSel = sel;
      if (oid === yOid) olderYSel = sel;
    }

    // The older OFS chain must keep its exact base row so pack-local topology
    // stays acyclic even when a newer duplicate delta advertises the same OID.
    expect(olderSeedSel).toBeGreaterThanOrEqual(0);
    expect(olderXSel).toBeGreaterThanOrEqual(0);
    expect(olderYSel).toBeGreaterThanOrEqual(0);
    expect(table.baseSlots[olderXSel]).toBe(olderSeedSel);
    expect(table.baseSlots[olderYSel]).toBe(olderXSel);
    expect(buildOutputOrder(table, createLogger("error", { service: "test" }))).toBe(true);
  });

  it("collapses pinned duplicate rows back into one live owner slot", async () => {
    const basePayload = new TextEncoder().encode("shared base\n");
    const childSuffix = new TextEncoder().encode("child\n");
    const childPayload = new Uint8Array(basePayload.length + childSuffix.length);
    childPayload.set(basePayload, 0);
    childPayload.set(childSuffix, basePayload.length);

    const baseOid = await computeOid("blob", basePayload);
    const childOid = await computeOid("blob", childPayload);

    const newerPackBytes = await buildPack([{ type: "blob", payload: basePayload }]);
    const olderPackBytes = await buildPack([
      {
        type: "ref-delta",
        baseOid,
        delta: buildCopyPrefixDelta(basePayload, basePayload.length),
      },
      { type: "ofs-delta", baseIndex: 0, delta: buildAppendOnlyDelta(basePayload, childSuffix) },
    ]);

    const newerKey = `test/rewrite-pinned-self-cycle-newer-${Date.now()}.pack`;
    const olderKey = `test/rewrite-pinned-self-cycle-older-${Date.now()}.pack`;
    await env.REPO_BUCKET.put(newerKey, newerPackBytes);
    await env.REPO_BUCKET.put(olderKey, olderPackBytes);

    const newerResolve = await indexTestPack(env, newerKey, newerPackBytes.byteLength);
    const newerRow: PackCatalogRow = {
      packKey: newerKey,
      kind: "receive",
      state: "active",
      tier: 0,
      seqLo: 2,
      seqHi: 2,
      objectCount: newerResolve.objectCount,
      packBytes: newerPackBytes.byteLength,
      idxBytes: newerResolve.idxBytes,
      createdAt: Date.now(),
      supersededBy: null,
    };
    const olderResolve = await indexTestPack(env, olderKey, olderPackBytes.byteLength, [newerRow]);

    const selection = await buildSelection(
      env,
      {
        packs: [
          { packKey: newerKey, packBytes: newerPackBytes.byteLength, idx: newerResolve.idxView },
          { packKey: olderKey, packBytes: olderPackBytes.byteLength, idx: olderResolve.idxView },
        ],
      },
      [baseOid, childOid],
      createLogger("error", { service: "test" }),
      new Set(),
      {
        limiter: { run: async (_label, fn) => await fn() },
        countSubrequest: () => {},
      }
    );

    expect(selection).toBeDefined();
    const table = selection!.table;

    let baseSel = -1;
    let baseCount = 0;
    let childSel = -1;
    for (let sel = 0; sel < table.count; sel++) {
      const oid = bytesToHex(table.oidsRaw.subarray(sel * 20, sel * 20 + 20));
      if (oid === baseOid) {
        baseSel = sel;
        baseCount++;
      }
      if (oid === childOid) childSel = sel;
    }

    // The planner now rewrites the live owner slot to the safe full-object
    // encoding instead of keeping both same-OID rows alive in the output pack.
    expect(baseSel).toBeGreaterThanOrEqual(0);
    expect(baseCount).toBe(1);
    expect(childSel).toBeGreaterThanOrEqual(0);
    expect(table.baseSlots[baseSel]).toBe(-1);
    expect(table.baseSlots[childSel]).toBe(baseSel);
    expect(buildOutputOrder(table, createLogger("error", { service: "test" }))).toBe(true);
  });

  it("repairs redirected duplicate delta bases before collapsing them away", async () => {
    const seedPayload = new TextEncoder().encode("seed\n");
    const xSuffix = new TextEncoder().encode("x\n");
    const xPayload = new Uint8Array(seedPayload.length + xSuffix.length);
    xPayload.set(seedPayload, 0);
    xPayload.set(xSuffix, seedPayload.length);

    const childSuffix = new TextEncoder().encode("child\n");
    const childPayload = new Uint8Array(xPayload.length + childSuffix.length);
    childPayload.set(xPayload, 0);
    childPayload.set(childSuffix, xPayload.length);

    const xOid = await computeOid("blob", xPayload);
    const childOid = await computeOid("blob", childPayload);

    // Layout:
    //   0: full seed
    //   1: OFS delta seed -> x
    //   2: OFS identity delta x -> x   (selected owner for x)
    //   3: OFS identity delta x -> x   (redirected duplicate for x)
    //   4: OFS delta x -> child        (depends on the selected owner)
    //
    // The selected x row depends on the duplicate x row, so dead-slot pruning
    // must keep that duplicate live to avoid folding the owner back onto
    // itself. Before the fix, row 3 survived compaction with `baseSlots = -1`
    // because redirected rows returned before resolving their own base edge.
    const packBytes = await buildPack([
      { type: "blob", payload: seedPayload },
      { type: "ofs-delta", baseIndex: 0, delta: buildAppendOnlyDelta(seedPayload, xSuffix) },
      { type: "ofs-delta", baseIndex: 1, delta: buildCopyPrefixDelta(xPayload, xPayload.length) },
      { type: "ofs-delta", baseIndex: 2, delta: buildCopyPrefixDelta(xPayload, xPayload.length) },
      { type: "ofs-delta", baseIndex: 2, delta: buildAppendOnlyDelta(xPayload, childSuffix) },
    ]);

    const packKey = `test/rewrite-retained-redirect-${Date.now()}.pack`;
    await env.REPO_BUCKET.put(packKey, packBytes);
    const resolve = await indexTestPack(env, packKey, packBytes.byteLength);

    const selection = await buildSelection(
      env,
      {
        packs: [{ packKey, packBytes: packBytes.byteLength, idx: resolve.idxView }],
      },
      [xOid, childOid],
      createLogger("error", { service: "test" }),
      new Set(),
      {
        limiter: { run: async (_label, fn) => await fn() },
        countSubrequest: () => {},
      }
    );

    expect(selection).toBeDefined();
    const table = selection!.table;

    let xSel = -1;
    let xCount = 0;
    let childSel = -1;
    for (let sel = 0; sel < table.count; sel++) {
      const oid = bytesToHex(table.oidsRaw.subarray(sel * 20, sel * 20 + 20));
      if (oid === xOid) {
        xSel = sel;
        xCount++;
      }
      if (oid === childOid) childSel = sel;
    }

    const selectedOwnerSel = childSel >= 0 ? table.baseSlots[childSel] : -1;
    const rootBaseSel = selectedOwnerSel >= 0 ? table.baseSlots[selectedOwnerSel] : -1;

    // The retained duplicate still needs its base chain repaired first, but
    // the final selection should collapse back to one live x row.
    expect(xCount).toBe(1);
    expect(xSel).toBeGreaterThanOrEqual(0);
    expect(childSel).toBeGreaterThanOrEqual(0);
    expect(selectedOwnerSel).toBe(xSel);
    expect(table.baseSlots[childSel]).toBe(xSel);
    expect(rootBaseSel).toBeGreaterThanOrEqual(0);
    expect(table.baseSlots[xSel]).toBe(rootBaseSel);
    expect(table.baseSlots[rootBaseSel]).toBe(-1);
    expect(table.typeCodes[rootBaseSel]).toBeLessThan(6);
    expect(buildOutputOrder(table, createLogger("error", { service: "test" }))).toBe(true);
  });

  it("collapses retained redirected ref-delta rows into the owner slot", async () => {
    const seedPayload = new TextEncoder().encode("seed\n");
    const xSuffix = new TextEncoder().encode("x\n");
    const xPayload = new Uint8Array(seedPayload.length + xSuffix.length);
    xPayload.set(seedPayload, 0);
    xPayload.set(xSuffix, seedPayload.length);

    const childSuffix = new TextEncoder().encode("child\n");
    const childPayload = new Uint8Array(xPayload.length + childSuffix.length);
    childPayload.set(xPayload, 0);
    childPayload.set(childSuffix, xPayload.length);

    const seedOid = await computeOid("blob", seedPayload);
    const childOid = await computeOid("blob", childPayload);

    // Layout:
    //   0: full seed
    //   1: REF delta seed -> x         (redirected duplicate for x)
    //   2: OFS identity delta x -> x   (selected owner for x)
    //   3: OFS delta x -> child        (depends on the selected owner)
    //
    // Row 1 is a retained redirected REF_DELTA. Before the retained-redirect
    // repair pass, this row stayed live with `baseSlots = -1`.
    const packBytes = await buildPack([
      { type: "blob", payload: seedPayload },
      {
        type: "ref-delta",
        baseOid: seedOid,
        delta: buildAppendOnlyDelta(seedPayload, xSuffix),
      },
      { type: "ofs-delta", baseIndex: 1, delta: buildCopyPrefixDelta(xPayload, xPayload.length) },
      { type: "ofs-delta", baseIndex: 2, delta: buildAppendOnlyDelta(xPayload, childSuffix) },
    ]);

    const packKey = `test/rewrite-retained-ref-delta-${Date.now()}.pack`;
    await env.REPO_BUCKET.put(packKey, packBytes);
    const resolve = await indexTestPack(env, packKey, packBytes.byteLength);

    const selection = await buildSelection(
      env,
      {
        packs: [{ packKey, packBytes: packBytes.byteLength, idx: resolve.idxView }],
      },
      [childOid],
      createLogger("error", { service: "test" }),
      new Set(),
      {
        limiter: { run: async (_label, fn) => await fn() },
        countSubrequest: () => {},
      }
    );

    expect(selection).toBeDefined();
    const table = selection!.table;

    let childSel = -1;
    for (let sel = 0; sel < table.count; sel++) {
      const oid = bytesToHex(table.oidsRaw.subarray(sel * 20, sel * 20 + 20));
      if (oid === childOid) childSel = sel;
    }

    const selectedOwnerSel = childSel >= 0 ? table.baseSlots[childSel] : -1;
    const rootBaseSel = selectedOwnerSel >= 0 ? table.baseSlots[selectedOwnerSel] : -1;

    expect(selectedOwnerSel).toBeGreaterThanOrEqual(0);
    expect(childSel).toBeGreaterThanOrEqual(0);
    expect(table.typeCodes[selectedOwnerSel]).toBe(7);
    expect(table.baseSlots[childSel]).toBe(selectedOwnerSel);
    expect(rootBaseSel).toBeGreaterThanOrEqual(0);
    expect(table.baseSlots[selectedOwnerSel]).toBe(rootBaseSel);
    expect(table.baseSlots[rootBaseSel]).toBe(-1);
    expect(table.typeCodes[rootBaseSel]).toBeLessThan(6);
    expect(buildOutputOrder(table, createLogger("error", { service: "test" }))).toBe(true);
  });

  it("collapses multi-hop retained redirect chains back into one live owner row", async () => {
    const seedPayload = new TextEncoder().encode("seed\n");
    const xSuffix = new TextEncoder().encode("x\n");
    const xPayload = new Uint8Array(seedPayload.length + xSuffix.length);
    xPayload.set(seedPayload, 0);
    xPayload.set(xSuffix, seedPayload.length);

    const childSuffix = new TextEncoder().encode("child\n");
    const childPayload = new Uint8Array(xPayload.length + childSuffix.length);
    childPayload.set(xPayload, 0);
    childPayload.set(childSuffix, xPayload.length);

    const seedOid = await computeOid("blob", seedPayload);
    const childOid = await computeOid("blob", childPayload);

    // Layout:
    //   0: full seed
    //   1: REF delta seed -> x         (retained redirect discovered on pass 2)
    //   2: OFS identity delta x -> x   (retained redirect discovered on pass 1)
    //   3: OFS identity delta x -> x   (selected owner for x)
    //   4: OFS delta x -> child        (depends on the selected owner)
    //
    // The owner row depends on row 2, and row 2 depends on row 1. This forces
    // the retained-redirect repair logic to run more than once.
    const packBytes = await buildPack([
      { type: "blob", payload: seedPayload },
      {
        type: "ref-delta",
        baseOid: seedOid,
        delta: buildAppendOnlyDelta(seedPayload, xSuffix),
      },
      { type: "ofs-delta", baseIndex: 1, delta: buildCopyPrefixDelta(xPayload, xPayload.length) },
      { type: "ofs-delta", baseIndex: 2, delta: buildCopyPrefixDelta(xPayload, xPayload.length) },
      { type: "ofs-delta", baseIndex: 3, delta: buildAppendOnlyDelta(xPayload, childSuffix) },
    ]);

    const packKey = `test/rewrite-retained-ref-chain-${Date.now()}.pack`;
    await env.REPO_BUCKET.put(packKey, packBytes);
    const resolve = await indexTestPack(env, packKey, packBytes.byteLength);

    const selection = await buildSelection(
      env,
      {
        packs: [{ packKey, packBytes: packBytes.byteLength, idx: resolve.idxView }],
      },
      [childOid],
      createLogger("error", { service: "test" }),
      new Set(),
      {
        limiter: { run: async (_label, fn) => await fn() },
        countSubrequest: () => {},
      }
    );

    expect(selection).toBeDefined();
    const table = selection!.table;

    let childSel = -1;
    for (let sel = 0; sel < table.count; sel++) {
      const oid = bytesToHex(table.oidsRaw.subarray(sel * 20, sel * 20 + 20));
      if (oid === childOid) childSel = sel;
    }

    const selectedOwnerSel = childSel >= 0 ? table.baseSlots[childSel] : -1;
    const rootBaseSel = selectedOwnerSel >= 0 ? table.baseSlots[selectedOwnerSel] : -1;

    expect(selectedOwnerSel).toBeGreaterThanOrEqual(0);
    expect(rootBaseSel).toBeGreaterThanOrEqual(0);
    expect(table.typeCodes[selectedOwnerSel]).toBe(7);
    expect(table.baseSlots[childSel]).toBe(selectedOwnerSel);
    expect(table.baseSlots[selectedOwnerSel]).toBe(rootBaseSel);
    expect(table.baseSlots[rootBaseSel]).toBe(-1);
    expect(table.typeCodes[rootBaseSel]).toBeLessThan(6);
    expect(buildOutputOrder(table, createLogger("error", { service: "test" }))).toBe(true);
  });

  it("streams an indexable pack for a retained redirected ref-delta shape", async () => {
    const seedPayload = new TextEncoder().encode("seed\n");
    const xSuffix = new TextEncoder().encode("x\n");
    const xPayload = new Uint8Array(seedPayload.length + xSuffix.length);
    xPayload.set(seedPayload, 0);
    xPayload.set(xSuffix, seedPayload.length);

    const childSuffix = new TextEncoder().encode("child\n");
    const childPayload = new Uint8Array(xPayload.length + childSuffix.length);
    childPayload.set(xPayload, 0);
    childPayload.set(childSuffix, xPayload.length);

    const seedOid = await computeOid("blob", seedPayload);
    const childOid = await computeOid("blob", childPayload);

    const packBytes = await buildPack([
      { type: "blob", payload: seedPayload },
      {
        type: "ref-delta",
        baseOid: seedOid,
        delta: buildAppendOnlyDelta(seedPayload, xSuffix),
      },
      { type: "ofs-delta", baseIndex: 1, delta: buildCopyPrefixDelta(xPayload, xPayload.length) },
      { type: "ofs-delta", baseIndex: 2, delta: buildAppendOnlyDelta(xPayload, childSuffix) },
    ]);

    const packKey = `test/rewrite-retained-ref-stream-${Date.now()}.pack`;
    await env.REPO_BUCKET.put(packKey, packBytes);
    const resolve = await indexTestPack(env, packKey, packBytes.byteLength);

    const stream = await rewritePack(
      env,
      {
        packs: [{ packKey, packBytes: packBytes.byteLength, idx: resolve.idxView }],
      },
      [childOid],
      {
        limiter: { run: async (_label, fn) => await fn() },
        countSubrequest: () => {},
      }
    );

    expect(stream).toBeDefined();
    const rewrittenPack = await readStreamBytes(stream!);
    expect(new TextDecoder().decode(rewrittenPack.subarray(0, 4))).toBe("PACK");

    const verifyKey = `test/rewrite-retained-ref-stream-verify-${Date.now()}.pack`;
    await env.REPO_BUCKET.put(verifyKey, rewrittenPack);
    const verify = await indexTestPack(env, verifyKey, rewrittenPack.byteLength);
    expect(verify.idxView.count).toBe(3);

    const oidSet = new Set<string>();
    for (let i = 0; i < verify.idxView.count; i++) {
      const oidBytes = verify.idxView.rawNames.subarray(i * 20, (i + 1) * 20);
      oidSet.add(bytesToHex(oidBytes));
    }
    expect(oidSet.size).toBe(verify.idxView.count);
  });
});
