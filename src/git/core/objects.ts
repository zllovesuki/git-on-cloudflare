/**
 * Git object encoding/decoding utilities
 */

import { deflate } from "@/common/compression.ts";
import { bytesToHex } from "@/common/hex.ts";
import { asBufferSource, createDigestStream } from "@/common/webtypes.ts";

/**
 * Git object types
 */
export type GitObjectType = "blob" | "tree" | "commit" | "tag";

const textEncoder = new TextEncoder();

/**
 * Map Git object type to pack type code
 */
export function objTypeCode(type: GitObjectType): number {
  switch (type) {
    case "commit":
      return 1;
    case "tree":
      return 2;
    case "blob":
      return 3;
    case "tag":
      return 4;
    default:
      throw new Error(`Unknown object type: ${type}`);
  }
}

/**
 * Encode Git pack object header
 * @param type - Object type code (1=commit, 2=tree, 3=blob, 4=tag)
 * @param size - Object size in bytes
 * @returns Encoded header bytes
 */
export function encodeObjHeader(type: number, size: number): Uint8Array {
  let first = (type << 4) | (size & 0x0f);
  size >>= 4;
  const bytes: number[] = [];
  if (size > 0) first |= 0x80;
  bytes.push(first);
  while (size > 0) {
    let b = size & 0x7f;
    size >>= 7;
    if (size > 0) b |= 0x80;
    bytes.push(b);
  }
  return new Uint8Array(bytes);
}

function buildRawGitObject(type: GitObjectType, payload: Uint8Array): Uint8Array {
  const header = encodeGitObjectHeader(type, payload.byteLength);
  const raw = new Uint8Array(header.byteLength + payload.byteLength);
  raw.set(header, 0);
  raw.set(payload, header.byteLength);
  return raw;
}

function encodeGitObjectHeader(type: GitObjectType, payloadLength: number): Uint8Array {
  return textEncoder.encode(`${type} ${payloadLength}\0`);
}

/**
 * Compute the raw 20-byte SHA-1 digest for a Git object payload.
 * This avoids a hex roundtrip on hot paths that already store OIDs in binary.
 */
export async function computeOidBytes(
  type: GitObjectType,
  payload: Uint8Array
): Promise<Uint8Array> {
  const digestStream = createDigestStream("SHA-1");
  const digestWriter = digestStream.getWriter();
  await digestWriter.write(encodeGitObjectHeader(type, payload.byteLength));
  await digestWriter.write(payload);
  await digestWriter.close();
  return new Uint8Array(await digestStream.digest);
}

async function computeOidBytesFromRaw(raw: Uint8Array): Promise<Uint8Array> {
  return new Uint8Array(await crypto.subtle.digest("SHA-1", asBufferSource(raw)));
}

/**
 * Create a Git object with header and compute its OID
 * @param type - Git object type
 * @param payload - Object content
 * @returns Object ID and raw bytes
 */
export async function createGitObject(
  type: GitObjectType,
  payload: Uint8Array
): Promise<{ oid: string; raw: Uint8Array }> {
  const raw = buildRawGitObject(type, payload);
  const oid = bytesToHex(await computeOidBytesFromRaw(raw));
  return { oid, raw };
}

/**
 * Create and compress a Git object
 * @param type - Git object type
 * @param payload - Object content
 * @returns Object ID, raw bytes, and compressed bytes
 */
export async function encodeGitObject(
  type: GitObjectType,
  payload: Uint8Array
): Promise<{ oid: string; raw: Uint8Array; zdata: Uint8Array }> {
  const { oid, raw } = await createGitObject(type, payload);
  const zdata = await deflate(raw);
  return { oid, raw, zdata };
}

/**
 * Parse a Git object to extract type and payload
 * @param raw - Raw Git object bytes (with header)
 * @returns Object type (GitObjectType) and payload
 */
export function parseGitObject(raw: Uint8Array): { type: GitObjectType; payload: Uint8Array } {
  // Find the null byte that separates header from content
  let nullIndex = -1;
  for (let i = 0; i < raw.length; i++) {
    if (raw[i] === 0) {
      nullIndex = i;
      break;
    }
  }

  if (nullIndex === -1) {
    throw new Error("Invalid Git object: no null byte found");
  }

  const header = new TextDecoder().decode(raw.subarray(0, nullIndex));
  const [typeStr] = header.split(" ");
  const payload = raw.subarray(nullIndex + 1);

  if (typeStr !== "commit" && typeStr !== "tree" && typeStr !== "blob" && typeStr !== "tag") {
    throw new Error(`Invalid Git object type: ${typeStr}`);
  }
  const type = typeStr as GitObjectType;

  return { type, payload };
}

/**
 * Compute the OID of a Git object from its type and payload
 * @param type - Git object type
 * @param payload - Object content
 * @returns SHA-1 hash as hex string
 */
export async function computeOid(type: GitObjectType, payload: Uint8Array): Promise<string> {
  return bytesToHex(await computeOidBytes(type, payload));
}
