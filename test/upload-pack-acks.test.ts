import test from "ava";
import type { PktItem } from "@/git/core/index.ts";
import { concatChunks, decodePktLines, flushPkt } from "@/git/index.ts";
import { buildAckSection } from "@/git/operations/fetch/protocol.ts";
import { createSidebandPacketChunks } from "@/git/operations/fetch/sideband.ts";

function findLine(items: PktItem[], text: string): number {
  return items.findIndex((item) => item.type === "line" && item.text === text);
}

function expectLine(item: PktItem | undefined) {
  if (!item || item.type !== "line") {
    throw new Error("expected pkt line");
  }
  return item;
}

function buildPacketizedResponse(
  packfile: Uint8Array,
  done: boolean,
  ackOids: string[]
): Uint8Array {
  return concatChunks([
    ...buildAckSection(ackOids, done),
    ...createSidebandPacketChunks(1, packfile),
    flushPkt(),
  ]);
}

test("packetized response emits NAK when no common haves and done=false", (t) => {
  const items = decodePktLines(
    buildPacketizedResponse(new Uint8Array([0x50, 0x41, 0x43, 0x4b]), false, [])
  );

  const ackIndex = findLine(items, "acknowledgments\n");
  t.true(ackIndex >= 0);
  t.is(expectLine(items[ackIndex + 1]).text, "NAK\n");
  t.is(items[ackIndex + 2]?.type, "delim");
  t.is(expectLine(items[ackIndex + 3]).text, "packfile\n");
  const bandLine = expectLine(items[ackIndex + 4]);
  t.true((bandLine.raw?.length || 0) >= 1);
  t.is(bandLine.raw?.[0], 0x01);
});

test("packetized response emits ACK common lines and the final ready line", (t) => {
  const firstOid = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
  const secondOid = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
  const items = decodePktLines(
    buildPacketizedResponse(new Uint8Array([0xaa]), false, [firstOid, secondOid])
  );

  const ackIndex = findLine(items, "acknowledgments\n");
  t.true(ackIndex >= 0);
  t.is(expectLine(items[ackIndex + 1]).text, `ACK ${firstOid} common\n`);
  t.is(expectLine(items[ackIndex + 2]).text, `ACK ${secondOid} ready\n`);
  t.is(items[ackIndex + 3]?.type, "delim");
});

test("packetized response omits acknowledgments when done=true", (t) => {
  const items = decodePktLines(buildPacketizedResponse(new Uint8Array([0xff, 0x00]), true, []));
  t.true(findLine(items, "packfile\n") >= 0);
  t.is(findLine(items, "acknowledgments\n"), -1);
});
