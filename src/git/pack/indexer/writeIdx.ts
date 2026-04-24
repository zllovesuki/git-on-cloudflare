/**
 * Binary writer for Git pack-index v2.
 *
 * Produces a standard `.idx` file that can be verified byte-for-byte against
 * `git index-pack` output. The format is fully deterministic given the same
 * pack contents.
 *
 * Layout (all multi-byte integers are big-endian):
 *   [4]  magic         0xff744f63
 *   [4]  version       0x00000002
 *  [1024] fanout       256 x uint32 cumulative OID counts
 *  [N*20] oid table    sorted raw 20-byte SHA-1 hashes
 *  [N*4]  crc32 table  per-object CRC-32 in OID-sorted order
 *  [N*4]  offset table uint32, MSB set -> index into 64-bit table
 *  [var]  64-bit table 8-byte entries for offsets >= 0x80000000
 *  [20]   pack SHA-1   trailing checksum copied from the pack
 *  [20]   idx SHA-1    SHA-1 of everything above
 */

import type { PackEntryTable } from "./types.ts";
import { asBufferSource } from "@/common/webtypes.ts";

/** Compare two raw 20-byte OIDs located at positions a and b in a flat buffer. */
function compareOids(oids: Uint8Array, a: number, b: number): number {
  for (let i = 0; i < 20; i++) {
    const diff = oids[a + i] - oids[b + i];
    if (diff !== 0) return diff;
  }
  return 0;
}

export function buildOidSortedEntryIndices(
  table: PackEntryTable,
  objectCount: number
): Uint32Array {
  const sortedIndices = new Uint32Array(objectCount);
  for (let i = 0; i < objectCount; i++) sortedIndices[i] = i;
  // Duplicate OIDs are invalid in normal packs but can appear in defensive
  // tests and malformed inputs. Tie-break by pack entry index so every derived
  // artifact writes duplicate rows in the same deterministic order.
  sortedIndices.sort((a, b) => compareOids(table.oids, a * 20, b * 20) || a - b);
  return sortedIndices;
}

export async function writeIdxV2(
  table: PackEntryTable,
  objectCount: number,
  packChecksum: Uint8Array
): Promise<Uint8Array> {
  const N = objectCount;

  // 1. Build sorted index by OID (raw 20-byte comparison).
  const sortedIndices = buildOidSortedEntryIndices(table, N);

  // 2. Count large offsets (>= 0x80000000).
  let largeCount = 0;
  for (let i = 0; i < N; i++) {
    if (table.offsets[i] >= 0x80000000) largeCount++;
  }

  // 3. Calculate total size.
  const totalSize =
    4 + // magic
    4 + // version
    256 * 4 + // fanout
    N * 20 + // OID table
    N * 4 + // CRC-32 table
    N * 4 + // 32-bit offset table
    largeCount * 8 + // 64-bit offset table
    20 + // pack checksum
    20; // idx checksum

  const buf = new Uint8Array(totalSize);
  const dv = new DataView(buf.buffer);
  let pos = 0;

  // 4. Magic + version.
  buf[pos++] = 0xff;
  buf[pos++] = 0x74;
  buf[pos++] = 0x4f;
  buf[pos++] = 0x63;
  dv.setUint32(pos, 2, false);
  pos += 4;

  // 5. Fanout table.
  const fanoutStart = pos;
  // Fill with zeros first; we'll accumulate below.
  pos += 256 * 4;

  // Compute fanout: for each first-byte bucket, count how many sorted OIDs
  // have a first byte <= that value.
  const buckets = new Uint32Array(256);
  for (let i = 0; i < N; i++) {
    const idx = sortedIndices[i];
    const firstByte = table.oids[idx * 20];
    buckets[firstByte]++;
  }
  let cumulative = 0;
  for (let b = 0; b < 256; b++) {
    cumulative += buckets[b];
    dv.setUint32(fanoutStart + b * 4, cumulative, false);
  }

  // 6. OID table (sorted).
  for (let i = 0; i < N; i++) {
    const idx = sortedIndices[i];
    buf.set(table.oids.subarray(idx * 20, idx * 20 + 20), pos);
    pos += 20;
  }

  // 7. CRC-32 table (in OID-sorted order).
  for (let i = 0; i < N; i++) {
    const idx = sortedIndices[i];
    dv.setUint32(pos, table.crc32s[idx], false);
    pos += 4;
  }

  // 8. 32-bit offset table.
  const offsets32Start = pos;
  pos += N * 4;

  // 9. 64-bit offset table (only for offsets >= 0x80000000).
  const offsets64Start = pos;
  let largeIdx = 0;
  for (let i = 0; i < N; i++) {
    const idx = sortedIndices[i];
    const off = table.offsets[idx];
    if (off >= 0x80000000) {
      // Mark MSB in the 32-bit table entry to point into the 64-bit table.
      dv.setUint32(offsets32Start + i * 4, 0x80000000 | largeIdx, false);
      // Write 64-bit offset as two uint32.
      dv.setUint32(offsets64Start + largeIdx * 8, 0, false); // high 32 bits (always 0 for < 4GB)
      dv.setUint32(offsets64Start + largeIdx * 8 + 4, off, false);
      largeIdx++;
    } else {
      dv.setUint32(offsets32Start + i * 4, off, false);
    }
  }
  pos = offsets64Start + largeCount * 8;

  // 10. Pack checksum.
  buf.set(packChecksum, pos);
  pos += 20;

  // 11. Idx checksum: SHA-1 of everything before this field.
  const idxHash = new Uint8Array(
    await crypto.subtle.digest("SHA-1", asBufferSource(buf.subarray(0, pos)))
  );
  buf.set(idxHash, pos);

  return buf;
}
