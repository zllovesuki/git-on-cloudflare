import { asBufferSource, deflate, hexToBytes, zeroOid } from "@/common/index.ts";
import {
  concatChunks,
  encodeObjHeader,
  encodeOfsDeltaDistance,
  GitObjectType,
  objTypeCode,
} from "@/git/index.ts";

type PackObjectEntry = {
  type: GitObjectType;
  payload: Uint8Array;
};

type OfsDeltaEntry = {
  type: "ofs-delta";
  baseIndex: number;
  delta: Uint8Array;
};

type RefDeltaEntry = {
  type: "ref-delta";
  baseOid: string;
  delta: Uint8Array;
};

type PackBuildEntry = PackObjectEntry | OfsDeltaEntry | RefDeltaEntry;

function encodeDeltaVarint(value: number): Uint8Array {
  if (!Number.isInteger(value) || value < 0) {
    throw new Error(`invalid delta varint value: ${value}`);
  }
  const out: number[] = [];
  do {
    let byte = value & 0x7f;
    value >>>= 7;
    if (value > 0) byte |= 0x80;
    out.push(byte);
  } while (value > 0);
  return Uint8Array.from(out);
}

function encodeDeltaCopy(offset: number, size: number): Uint8Array {
  if (!Number.isInteger(offset) || offset < 0) {
    throw new Error(`invalid delta copy offset: ${offset}`);
  }
  if (!Number.isInteger(size) || size <= 0 || size > 0x10000) {
    throw new Error(`invalid delta copy size: ${size}`);
  }

  let opcode = 0x80;
  const bytes: number[] = [];

  if (offset & 0xff) {
    opcode |= 0x01;
    bytes.push(offset & 0xff);
  }
  if (offset & 0xff00) {
    opcode |= 0x02;
    bytes.push((offset >>> 8) & 0xff);
  }
  if (offset & 0xff0000) {
    opcode |= 0x04;
    bytes.push((offset >>> 16) & 0xff);
  }
  if (offset & 0xff000000) {
    opcode |= 0x08;
    bytes.push((offset >>> 24) & 0xff);
  }

  if (size !== 0x10000) {
    if (size & 0xff) {
      opcode |= 0x10;
      bytes.push(size & 0xff);
    }
    if (size & 0xff00) {
      opcode |= 0x20;
      bytes.push((size >>> 8) & 0xff);
    }
    if (size & 0xff0000) {
      opcode |= 0x40;
      bytes.push((size >>> 16) & 0xff);
    }
  }

  return Uint8Array.from([opcode, ...bytes]);
}

/**
 * Build a simple append-only git delta. The result is the full base payload
 * followed by the provided suffix bytes.
 */
export function buildAppendOnlyDelta(base: Uint8Array, suffix: Uint8Array): Uint8Array {
  const parts: Uint8Array[] = [
    encodeDeltaVarint(base.length),
    encodeDeltaVarint(base.length + suffix.length),
  ];

  if (base.length > 0) parts.push(encodeDeltaCopy(0, base.length));

  for (let offset = 0; offset < suffix.length; offset += 0x7f) {
    const chunk = suffix.subarray(offset, offset + 0x7f);
    parts.push(Uint8Array.from([chunk.length]));
    parts.push(chunk);
  }

  return concatChunks(parts);
}

/**
 * Build a delta whose result is the leading `prefixLength` bytes of `base`.
 */
export function buildCopyPrefixDelta(base: Uint8Array, prefixLength: number): Uint8Array {
  if (!Number.isInteger(prefixLength) || prefixLength < 0 || prefixLength > base.length) {
    throw new Error(`invalid delta prefix length: ${prefixLength}`);
  }

  const parts: Uint8Array[] = [encodeDeltaVarint(base.length), encodeDeltaVarint(prefixLength)];
  if (prefixLength > 0) parts.push(encodeDeltaCopy(0, prefixLength));
  return concatChunks(parts);
}

/**
 * Build a Git pack file from objects, including delta entries used by pack-only tests.
 */
export async function buildPack(objects: PackBuildEntry[]): Promise<Uint8Array> {
  const header = new Uint8Array(12);
  header.set(new TextEncoder().encode("PACK"), 0);
  const view = new DataView(header.buffer);
  view.setUint32(4, 2);
  view.setUint32(8, objects.length);

  const parts: Uint8Array[] = [header];
  const entryOffsets: number[] = [];
  let bodyLength = header.byteLength;

  for (const object of objects) {
    const entryOffset = bodyLength;
    let objectHeader: Uint8Array;
    let objectPayload: Uint8Array;

    if (object.type === "ofs-delta") {
      const baseOffset = entryOffsets[object.baseIndex];
      if (baseOffset === undefined) {
        throw new Error(`missing OFS_DELTA base index: ${object.baseIndex}`);
      }
      objectHeader = concatChunks([
        encodeObjHeader(6, object.delta.byteLength),
        encodeOfsDeltaDistance(entryOffset - baseOffset),
      ]);
      objectPayload = await deflate(object.delta);
    } else if (object.type === "ref-delta") {
      objectHeader = concatChunks([
        encodeObjHeader(7, object.delta.byteLength),
        hexToBytes(object.baseOid),
      ]);
      objectPayload = await deflate(object.delta);
    } else {
      objectHeader = encodeObjHeader(objTypeCode(object.type), object.payload.byteLength);
      objectPayload = await deflate(object.payload);
    }

    parts.push(objectHeader);
    parts.push(objectPayload);
    entryOffsets.push(entryOffset);
    bodyLength += objectHeader.byteLength + objectPayload.byteLength;
  }

  const body = concatChunks(parts);
  const sha = new Uint8Array(await crypto.subtle.digest("SHA-1", asBufferSource(body)));
  const out = new Uint8Array(body.byteLength + 20);
  out.set(body, 0);
  out.set(sha, body.byteLength);
  return out;
}

export async function makeCommit(treeOid: string, msg: string) {
  const author = `You <you@example.com> 0 +0000`;
  const payload = new TextEncoder().encode(
    `tree ${treeOid}\n` + `author ${author}\n` + `committer ${author}\n\n${msg}`
  );
  const header = new TextEncoder().encode(`commit ${payload.byteLength}\0`);
  const raw = new Uint8Array(header.length + payload.length);
  raw.set(header, 0);
  raw.set(payload, header.length);
  const hash = await crypto.subtle.digest("SHA-1", raw);
  const oid = Array.from(new Uint8Array(hash))
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
  return { oid, payload };
}

export async function makeTree(): Promise<{ oid: string; payload: Uint8Array }> {
  const payload = new Uint8Array(0);
  const header = new TextEncoder().encode(`tree ${payload.byteLength}\0`);
  const raw = new Uint8Array(header.length + payload.length);
  raw.set(header, 0);
  raw.set(payload, header.length);
  const oid = Array.from(new Uint8Array(await crypto.subtle.digest("SHA-1", raw)))
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
  return { oid, payload };
}

export function zero40(): string {
  return zeroOid();
}

export { encodeObjHeader } from "@/git/index.ts";
