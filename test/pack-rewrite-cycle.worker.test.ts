import { describe, expect, it } from "vitest";
import { env } from "cloudflare:test";

import { bytesToHex, createLogger } from "@/common/index.ts";
import { computeOid } from "@/git/core/objects.ts";
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

describe("pack rewrite cycles", () => {
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
});
