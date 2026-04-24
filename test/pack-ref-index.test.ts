import test from "ava";

import { hexToBytes } from "@/common/index.ts";
import { objTypeCode, type GitObjectType } from "@/git/core/index.ts";
import type { IdxView } from "@/git/object-store/types.ts";
import {
  getPackRefObjectType,
  getPackRefRefsAt,
  PackRefsBuilder,
  parsePackRefView,
  parseTreeClosureRefs,
  type PackRefSnapshotEntry,
} from "@/git/pack/refIndex.ts";
import { allocateEntryTable, type PackEntryTable } from "@/git/pack/indexer/index.ts";
import { computeNeededFromPackRefs } from "@/git/operations/fetch/refClosure.ts";

const HEADER_BYTES = 60;

function oid(prefix: string): string {
  return prefix.padEnd(40, "0");
}

function oidFromNumber(value: number): string {
  return value.toString(16).padStart(40, "0");
}

function makeIdxView(args: {
  packKey: string;
  count: number;
  packSize: number;
  packChecksum: Uint8Array;
  idxChecksum: Uint8Array;
  oids?: string[];
}): IdxView {
  const rawNames = new Uint8Array(args.count * 20);
  const fanout = new Uint32Array(256);
  const sortedOids = args.oids || Array.from({ length: args.count }, () => oid("00"));
  if (sortedOids.length !== args.count) {
    throw new Error("test idx view oid count mismatch");
  }

  const firstByteCounts = new Uint32Array(256);
  for (let index = 0; index < sortedOids.length; index++) {
    const oidBytes = hexToBytes(sortedOids[index]!);
    rawNames.set(oidBytes, index * 20);
    firstByteCounts[oidBytes[0] || 0]++;
  }

  let cumulative = 0;
  for (let index = 0; index < fanout.length; index++) {
    cumulative += firstByteCounts[index] || 0;
    fanout[index] = cumulative;
  }

  return {
    packKey: args.packKey,
    count: args.count,
    fanout,
    rawNames,
    offsets: new Float64Array(args.count),
    nextOffsetByIndex: new Float64Array(args.count),
    sortedOffsets: new Float64Array(args.count),
    sortedOffsetIndices: new Uint32Array(args.count),
    packSize: args.packSize,
    packChecksum: args.packChecksum,
    idxChecksum: args.idxChecksum,
  };
}

function recordObject(args: {
  table: PackEntryTable;
  builder: PackRefsBuilder;
  index: number;
  oid: string;
  type: GitObjectType;
  payload: Uint8Array;
}): void {
  args.table.oids.set(hexToBytes(args.oid), args.index * 20);
  args.table.objectTypes[args.index] = objTypeCode(args.type);
  args.builder.recordObject(args.index, args.type, args.payload);
}

function buildPackRefSnapshotEntry(args: {
  packKey: string;
  objects: Array<{
    oid: string;
    type: GitObjectType;
    payload: Uint8Array;
  }>;
}): PackRefSnapshotEntry {
  const packChecksum = hexToBytes(oid("aa"));
  const idxChecksum = hexToBytes(oid("bb"));
  const packSize = Math.max(256, args.objects.length * 64);
  const table = allocateEntryTable(args.objects.length);
  const builder = new PackRefsBuilder(args.objects.length);

  for (let index = 0; index < args.objects.length; index++) {
    const object = args.objects[index]!;
    recordObject({
      table,
      builder,
      index,
      oid: object.oid,
      type: object.type,
      payload: object.payload,
    });
  }

  const sortedOids = args.objects.map((object) => object.oid).sort();
  const idx = makeIdxView({
    packKey: args.packKey,
    count: args.objects.length,
    packSize,
    packChecksum,
    idxChecksum,
    oids: sortedOids,
  });
  const built = builder.build({
    table,
    objectCount: args.objects.length,
    packBytes: packSize,
    packChecksum,
    idxChecksum,
  });
  const parsed = parsePackRefView(args.packKey, built.bytes, idx);
  if (parsed.type !== "Ready") {
    throw new Error(`test sidecar failed to parse: ${parsed.type}`);
  }

  return {
    packKey: args.packKey,
    packBytes: packSize,
    idx,
    refs: parsed.view,
  };
}

function treePayload(entries: Array<{ mode: string; name: string; oid: string }>): Uint8Array {
  const encoder = new TextEncoder();
  const parts: Uint8Array[] = [];
  let total = 0;
  for (const entry of entries) {
    const header = encoder.encode(`${entry.mode} ${entry.name}`);
    const oidBytes = hexToBytes(entry.oid);
    parts.push(header, Uint8Array.from([0]), oidBytes);
    total += header.byteLength + 1 + oidBytes.byteLength;
  }
  const out = new Uint8Array(total);
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.byteLength;
  }
  return out;
}

function buildSampleRefIndex() {
  const packKey = "do/test/objects/pack/sample.pack";
  const packChecksum = hexToBytes(oid("aa"));
  const idxChecksum = hexToBytes(oid("bb"));
  const packSize = 1234;
  const table = allocateEntryTable(4);
  const builder = new PackRefsBuilder(4);
  const encoder = new TextEncoder();

  const blobOid = oid("40");
  const treeOid = oid("20");
  const parentOid = oid("50");
  const tagTargetOid = oid("60");
  const fileOid = oid("70");
  const gitlinkOid = oid("80");

  recordObject({
    table,
    builder,
    index: 0,
    oid: oid("10"),
    type: "commit",
    payload: encoder.encode(`tree ${treeOid}\nparent ${parentOid}\n\nmessage\n`),
  });
  recordObject({
    table,
    builder,
    index: 1,
    oid: treeOid,
    type: "tree",
    payload: treePayload([
      { mode: "100644", name: "file.txt", oid: fileOid },
      { mode: "160000", name: "submodule", oid: gitlinkOid },
    ]),
  });
  recordObject({
    table,
    builder,
    index: 2,
    oid: oid("30"),
    type: "tag",
    payload: encoder.encode(`object ${tagTargetOid}\ntype commit\ntag v1\n\nmessage\n`),
  });
  recordObject({
    table,
    builder,
    index: 3,
    oid: blobOid,
    type: "blob",
    payload: encoder.encode("blob\n"),
  });

  const idx = makeIdxView({
    packKey,
    count: 4,
    packSize,
    packChecksum,
    idxChecksum,
  });
  const built = builder.build({
    table,
    objectCount: 4,
    packBytes: packSize,
    packChecksum,
    idxChecksum,
  });

  return { built, idx, refs: { treeOid, parentOid, fileOid, tagTargetOid } };
}

test("pack ref sidecar encodes and parses logical object refs", (t) => {
  const { built, idx, refs } = buildSampleRefIndex();
  const parsed = parsePackRefView(idx.packKey, built.bytes, idx);

  t.is(parsed.type, "Ready");
  if (parsed.type !== "Ready") return;

  t.is(parsed.view.objectCount, 4);
  t.is(parsed.view.refStartsBytes.byteLength, 5 * 4);
  t.is(getPackRefObjectType(parsed.view, 0), "commit");
  t.deepEqual(getPackRefRefsAt(parsed.view, 0), [refs.treeOid, refs.parentOid]);
  t.is(getPackRefObjectType(parsed.view, 1), "tree");
  t.deepEqual(getPackRefRefsAt(parsed.view, 1), [refs.fileOid]);
  t.is(getPackRefObjectType(parsed.view, 2), "tag");
  t.deepEqual(getPackRefRefsAt(parsed.view, 2), [refs.tagTargetOid]);
  t.is(getPackRefObjectType(parsed.view, 3), "blob");
  t.deepEqual(getPackRefRefsAt(parsed.view, 3), []);
});

test("tree closure parser excludes gitlinks", (t) => {
  const fileOid = oid("11");
  const gitlinkOid = oid("22");
  const refs = parseTreeClosureRefs(
    treePayload([
      { mode: "100644", name: "file.txt", oid: fileOid },
      { mode: "160000", name: "submodule", oid: gitlinkOid },
      { mode: "40000", name: "dir", oid: oid("33") },
    ])
  );

  t.deepEqual(refs, [fileOid, oid("33")]);
});

test("pack ref sidecar keeps duplicate OID ordering deterministic", (t) => {
  const packKey = "do/test/objects/pack/dupe.pack";
  const packChecksum = hexToBytes(oid("aa"));
  const idxChecksum = hexToBytes(oid("bb"));
  const table = allocateEntryTable(2);
  const builder = new PackRefsBuilder(2);
  const encoder = new TextEncoder();
  const duplicateOid = oid("10");
  const treeA = oid("20");
  const treeB = oid("30");

  recordObject({
    table,
    builder,
    index: 0,
    oid: duplicateOid,
    type: "commit",
    payload: encoder.encode(`tree ${treeA}\n\nfirst\n`),
  });
  recordObject({
    table,
    builder,
    index: 1,
    oid: duplicateOid,
    type: "commit",
    payload: encoder.encode(`tree ${treeB}\n\nsecond\n`),
  });

  const idx = makeIdxView({
    packKey,
    count: 2,
    packSize: 200,
    packChecksum,
    idxChecksum,
  });
  const built = builder.build({
    table,
    objectCount: 2,
    packBytes: 200,
    packChecksum,
    idxChecksum,
  });
  const parsed = parsePackRefView(packKey, built.bytes, idx);

  t.is(parsed.type, "Ready");
  if (parsed.type !== "Ready") return;
  t.deepEqual(getPackRefRefsAt(parsed.view, 0), [treeA]);
  t.deepEqual(getPackRefRefsAt(parsed.view, 1), [treeB]);
});

test("sidecar closure walks a wide graph with a cursor queue", async (t) => {
  const packKey = "do/test/objects/pack/wide.pack";
  const packChecksum = hexToBytes(oid("aa"));
  const idxChecksum = hexToBytes(oid("bb"));
  const blobCount = 4096;
  const objectCount = blobCount + 2;
  const packSize = 200_000;
  const table = allocateEntryTable(objectCount);
  const builder = new PackRefsBuilder(objectCount);
  const encoder = new TextEncoder();

  const commitOid = oidFromNumber(1);
  const treeOid = oidFromNumber(2);
  const blobOids = Array.from({ length: blobCount }, (_value, index) => oidFromNumber(index + 3));

  recordObject({
    table,
    builder,
    index: 0,
    oid: commitOid,
    type: "commit",
    payload: encoder.encode(`tree ${treeOid}\n\nwide\n`),
  });
  recordObject({
    table,
    builder,
    index: 1,
    oid: treeOid,
    type: "tree",
    payload: treePayload(
      blobOids.map((entryOid, index) => ({
        mode: "100644",
        name: `file-${index}.txt`,
        oid: entryOid,
      }))
    ),
  });

  const emptyBlob = new Uint8Array(0);
  for (let index = 0; index < blobOids.length; index++) {
    recordObject({
      table,
      builder,
      index: index + 2,
      oid: blobOids[index]!,
      type: "blob",
      payload: emptyBlob,
    });
  }

  const sortedOids = [commitOid, treeOid, ...blobOids].sort();
  const idx = makeIdxView({
    packKey,
    count: objectCount,
    packSize,
    packChecksum,
    idxChecksum,
    oids: sortedOids,
  });
  const built = builder.build({
    table,
    objectCount,
    packBytes: packSize,
    packChecksum,
    idxChecksum,
  });
  const parsed = parsePackRefView(packKey, built.bytes, idx);
  t.is(parsed.type, "Ready");
  if (parsed.type !== "Ready") return;

  const closure = await computeNeededFromPackRefs({
    repoId: "test/wide",
    packs: [{ packKey, packBytes: packSize, idx, refs: parsed.view }],
    wants: [commitOid, commitOid],
    haves: [],
  });

  t.is(closure.type, "Ready");
  if (closure.type !== "Ready") return;
  t.is(closure.neededOids.length, objectCount);
  t.is(new Set(closure.neededOids).size, objectCount);
  t.true(closure.neededOids.includes(commitOid));
  t.true(closure.neededOids.includes(treeOid));
  t.true(closure.neededOids.includes(blobOids[blobOids.length - 1]!));
});

test("sidecar closure queues duplicate wants once", async (t) => {
  const blobOid = oid("11");
  const pack = buildPackRefSnapshotEntry({
    packKey: "do/test/objects/pack/duplicate-wants.pack",
    objects: [
      {
        oid: blobOid,
        type: "blob",
        payload: new TextEncoder().encode("content\n"),
      },
    ],
  });

  const closure = await computeNeededFromPackRefs({
    repoId: "test/duplicate-wants",
    packs: [pack],
    wants: [blobOid, blobOid],
    haves: [],
  });

  t.is(closure.type, "Ready");
  if (closure.type !== "Ready") return;
  t.deepEqual(closure.neededOids, [blobOid]);
  t.is(closure.stats.indexedObjects, 1);
  t.is(closure.stats.queued, 1);
  t.is(closure.stats.seen, 1);
  t.is(closure.stats.needed, 1);
  t.is(closure.stats.duplicateQueueSkips, 1);
});

test("sidecar closure does not grow the queue for duplicate tree edges", async (t) => {
  const encoder = new TextEncoder();
  const commitOid = oid("10");
  const treeOid = oid("20");
  const blobOid = oid("30");
  const duplicateEntries = 4096;
  const pack = buildPackRefSnapshotEntry({
    packKey: "do/test/objects/pack/duplicate-tree-edges.pack",
    objects: [
      {
        oid: commitOid,
        type: "commit",
        payload: encoder.encode(`tree ${treeOid}\n\nmessage\n`),
      },
      {
        oid: treeOid,
        type: "tree",
        payload: treePayload(
          Array.from({ length: duplicateEntries }, (_value, index) => ({
            mode: "100644",
            name: `file-${index}.txt`,
            oid: blobOid,
          }))
        ),
      },
      {
        oid: blobOid,
        type: "blob",
        payload: encoder.encode("same blob\n"),
      },
    ],
  });

  const closure = await computeNeededFromPackRefs({
    repoId: "test/duplicate-tree-edges",
    packs: [pack],
    wants: [commitOid],
    haves: [],
  });

  t.is(closure.type, "Ready");
  if (closure.type !== "Ready") return;
  t.is(closure.neededOids.length, 3);
  t.is(closure.stats.queued, 3);
  t.is(closure.stats.seen, 3);
  t.is(closure.stats.edgeVisits, duplicateEntries + 1);
  t.is(closure.stats.duplicateQueueSkips, duplicateEntries - 1);
});

test("sidecar closure canonicalizes cross-pack duplicate OIDs by snapshot order", async (t) => {
  const encoder = new TextEncoder();
  const duplicateCommitOid = oid("10");
  const newerTreeOid = oid("20");
  const olderTreeOid = oid("30");
  const newerPack = buildPackRefSnapshotEntry({
    packKey: "do/test/objects/pack/newer.pack",
    objects: [
      {
        oid: duplicateCommitOid,
        type: "commit",
        payload: encoder.encode(`tree ${newerTreeOid}\n\nnew\n`),
      },
      {
        oid: newerTreeOid,
        type: "tree",
        payload: treePayload([]),
      },
    ],
  });
  const olderPack = buildPackRefSnapshotEntry({
    packKey: "do/test/objects/pack/older.pack",
    objects: [
      {
        oid: duplicateCommitOid,
        type: "commit",
        payload: encoder.encode(`tree ${olderTreeOid}\n\nold\n`),
      },
      {
        oid: olderTreeOid,
        type: "tree",
        payload: treePayload([]),
      },
    ],
  });

  const closure = await computeNeededFromPackRefs({
    repoId: "test/cross-pack-duplicate",
    packs: [newerPack, olderPack],
    wants: [duplicateCommitOid],
    haves: [],
  });

  t.is(closure.type, "Ready");
  if (closure.type !== "Ready") return;
  t.true(closure.neededOids.includes(duplicateCommitOid));
  t.true(closure.neededOids.includes(newerTreeOid));
  t.false(closure.neededOids.includes(olderTreeOid));
  t.is(closure.stats.queued, 2);
  t.is(closure.stats.needed, 2);
});

test("sidecar closure keeps force-push overlap bounded by canonical active objects", async (t) => {
  const encoder = new TextEncoder();
  const commitOid = oid("10");
  const treeOid = oid("20");
  const blobOid = oid("30");
  const newerPack = buildPackRefSnapshotEntry({
    packKey: "do/test/objects/pack/force-newer.pack",
    objects: [
      {
        oid: commitOid,
        type: "commit",
        payload: encoder.encode(`tree ${treeOid}\n\nforce push\n`),
      },
      {
        oid: treeOid,
        type: "tree",
        payload: treePayload([{ mode: "100644", name: "file.txt", oid: blobOid }]),
      },
      {
        oid: blobOid,
        type: "blob",
        payload: encoder.encode("overlap\n"),
      },
    ],
  });
  const olderPack = buildPackRefSnapshotEntry({
    packKey: "do/test/objects/pack/force-older.pack",
    objects: [
      {
        oid: blobOid,
        type: "blob",
        payload: encoder.encode("overlap\n"),
      },
    ],
  });

  const closure = await computeNeededFromPackRefs({
    repoId: "test/force-overlap",
    packs: [newerPack, olderPack],
    wants: [commitOid],
    haves: [oid("99")],
  });

  t.is(closure.type, "Ready");
  if (closure.type !== "Ready") return;
  t.deepEqual(new Set(closure.neededOids), new Set([commitOid, treeOid, blobOid]));
  t.is(closure.stats.indexedObjects, 4);
  t.is(closure.stats.queued, 3);
  t.is(closure.stats.seen, 3);
  t.is(closure.stats.needed, 3);
});

test("sidecar closure returns a retryable budget result at the missing ref cap", async (t) => {
  const treeOid = oid("20");
  const missingEntries = 1025;
  const pack = buildPackRefSnapshotEntry({
    packKey: "do/test/objects/pack/missing-ref-budget.pack",
    objects: [
      {
        oid: treeOid,
        type: "tree",
        payload: treePayload(
          Array.from({ length: missingEntries }, (_value, index) => ({
            mode: "100644",
            name: `missing-${index}.txt`,
            oid: oidFromNumber(index + 1),
          }))
        ),
      },
    ],
  });

  const closure = await computeNeededFromPackRefs({
    repoId: "test/missing-ref-budget",
    packs: [pack],
    wants: [treeOid],
    haves: [],
  });

  t.is(closure.type, "BudgetExceeded");
  if (closure.type !== "BudgetExceeded") return;
  t.is(closure.reason, "missing-ref-budget");
  t.is(closure.stats.queued, 1);
  t.is(closure.stats.seen, 1);
  t.is(closure.stats.missing, 1024);
});

test("pack ref sidecar parser rejects invalid artifacts", (t) => {
  const { built, idx } = buildSampleRefIndex();
  const cases: Array<{ name: string; mutate: (bytes: Uint8Array) => Uint8Array; reason: string }> =
    [
      {
        name: "bad magic",
        mutate(bytes) {
          bytes[0] = 0;
          return bytes;
        },
        reason: "bad-magic",
      },
      {
        name: "bad version",
        mutate(bytes) {
          new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).setUint32(4, 2, false);
          return bytes;
        },
        reason: "bad-version",
      },
      {
        name: "count mismatch",
        mutate(bytes) {
          new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).setUint32(8, 99, false);
          return bytes;
        },
        reason: "object-count-mismatch",
      },
      {
        name: "pack bytes mismatch",
        mutate(bytes) {
          new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).setUint32(16, 999, false);
          return bytes;
        },
        reason: "pack-bytes-mismatch",
      },
      {
        name: "pack checksum mismatch",
        mutate(bytes) {
          bytes[20] ^= 0xff;
          return bytes;
        },
        reason: "pack-checksum-mismatch",
      },
      {
        name: "idx checksum mismatch",
        mutate(bytes) {
          bytes[40] ^= 0xff;
          return bytes;
        },
        reason: "idx-checksum-mismatch",
      },
      {
        name: "invalid type code",
        mutate(bytes) {
          bytes[HEADER_BYTES] = 9;
          return bytes;
        },
        reason: "invalid-type-code",
      },
      {
        name: "truncated type codes",
        mutate(bytes) {
          return bytes.slice(0, HEADER_BYTES + 1);
        },
        reason: "truncated-type-codes",
      },
      {
        name: "non-monotonic starts",
        mutate(bytes) {
          const dv = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
          const startsOffset = HEADER_BYTES + idx.count;
          dv.setUint32(startsOffset, 2, false);
          dv.setUint32(startsOffset + 4, 1, false);
          return bytes;
        },
        reason: "non-monotonic-ref-starts",
      },
      {
        name: "bad final offset",
        mutate(bytes) {
          const dv = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
          const finalStartOffset = HEADER_BYTES + idx.count + idx.count * 4;
          dv.setUint32(finalStartOffset, 999, false);
          return bytes;
        },
        reason: "invalid-final-ref-offset",
      },
    ];

  for (const entry of cases) {
    const mutated = entry.mutate(new Uint8Array(built.bytes));
    const parsed = parsePackRefView(idx.packKey, mutated, idx);
    t.is(parsed.type, "Invalid", entry.name);
    if (parsed.type === "Invalid") {
      t.is(parsed.reason, entry.reason, entry.name);
    }
  }
});
