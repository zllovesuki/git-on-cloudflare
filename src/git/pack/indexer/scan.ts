/**
 * Streaming pack scanner (Pass 1).
 *
 * Reads a .pack file from R2 in sequential chunks, parses every entry header,
 * inflates compressed data to determine span boundaries, computes OIDs for
 * non-delta objects, and records metadata for delta objects. The entire pack is
 * never buffered in memory; at most one object's inflated payload is held at a
 * time before being discarded.
 */

import { bytesToHex } from "@/common/hex.ts";
import { createDigestStream } from "@/common/webtypes.ts";
import { computeOidBytes } from "@/git/core/objects.ts";
import { readPackRange } from "@/git/pack/packMeta.ts";
import { typeCodeToObjectType } from "@/git/object-store/support.ts";

import { InflateCursor, CRC32_INIT, crc32Update, crc32Finish } from "./inflateCursor.ts";
import { allocateEntryTable } from "./types.ts";
import type { IndexerOptions, ScanResult, RefBaseOids } from "./types.ts";

const DEFAULT_CHUNK_SIZE = 1_048_576; // 1 MiB
const DELTA_HEADER_CAPTURE_LIMIT = 16;
const PACK_HEADER_BYTES = 12;
const PACK_TRAILER_BYTES = 20;
const MIN_PACK_BYTES = PACK_HEADER_BYTES + PACK_TRAILER_BYTES;
// Smallest possible packed entry:
// - 1 byte Git pack object header
// - 8 bytes for the smallest valid zlib-wrapped empty payload
//
// Delta entries are always larger than this, so using the constant as a
// lower bound can only reject impossible object counts, never valid packs.
const MIN_PACKED_ENTRY_BYTES = 9;
// The scanner stores one typed-array row per object. Keep a hard ceiling so a
// malformed pack header cannot force unbounded metadata allocation in the
// shared worker isolate before any object bytes are validated.
const MAX_INDEXABLE_OBJECT_COUNT = 250_000;
const MAX_PACK_OFFSET = 0xffffffff;
const MAX_PACK_OBJECT_SIZE = 0xffffffff;
const SCAN_PROGRESS_STEPS = 20;

function emitScanProgress(
  onProgress: IndexerOptions["onProgress"],
  processed: number,
  total: number
): void {
  if (!onProgress || total <= 0) return;
  const percent = Math.round((processed / total) * 100);
  if (processed >= total) {
    onProgress(`Scanning pack objects: 100% (${total}/${total}), done.\n`);
    return;
  }
  onProgress(`Scanning pack objects: ${percent}% (${processed}/${total})\r`);
}

function isReservedPackType(type: number): boolean {
  return type === 0 || type === 5;
}

function readDeltaSizeVarint(
  data: Uint8Array,
  pos: number,
  fieldName: string
): { value: number; nextPos: number } {
  let value = 0;
  let factor = 1;
  let cursor = pos;

  while (cursor < data.length) {
    const b = data[cursor++];
    value += (b & 0x7f) * factor;
    if (value > MAX_PACK_OBJECT_SIZE) {
      throw new Error(`scan: ${fieldName} exceeds supported 32-bit size range`);
    }
    if (!(b & 0x80)) {
      return { value, nextPos: cursor };
    }
    if (factor > Number.MAX_SAFE_INTEGER / 128) {
      throw new Error(`scan: ${fieldName} is too large to decode safely`);
    }
    factor *= 128;
  }

  throw new Error(`scan: truncated ${fieldName} header`);
}

// ---------------------------------------------------------------------------
// Pack header varint parsing helpers (inline to avoid extra R2 reads)
// ---------------------------------------------------------------------------

/**
 * Parse a pack entry header from an in-memory buffer at the given position.
 * Returns the type code, decompressed size, header length, and delta metadata.
 *
 * The logic mirrors `readPackHeaderExFromBuf` in packMeta.ts but returns the
 * decoded size directly and operates on a position-based cursor so the scanner
 * can work from its sliding buffer without copying.
 */
function parseEntryHeader(
  buf: Uint8Array,
  pos: number
): {
  type: number;
  size: number;
  headerLen: number;
  baseOidBytes?: Uint8Array;
  baseRel?: number;
} | null {
  if (pos >= buf.length) return null;

  const start = pos;
  let c = buf[pos++];
  const type = (c >> 4) & 0x07;
  if (isReservedPackType(type)) {
    throw new Error(`scan: invalid reserved pack type ${type} at offset ${start}`);
  }
  let size = c & 0x0f;
  let factor = 16;

  while (c & 0x80) {
    if (pos >= buf.length) return null;
    c = buf[pos++];
    size += (c & 0x7f) * factor;
    if (size > MAX_PACK_OBJECT_SIZE) {
      throw new Error(`scan: entry size exceeds supported 32-bit range at offset ${start}`);
    }
    if (c & 0x80) {
      // The indexer stores sizes in Uint32Array-backed tables. Reject absurdly
      // long varints here instead of letting arithmetic drift past safe integer
      // precision and then mislabeling the failure later in the scan.
      if (factor > Number.MAX_SAFE_INTEGER / 128) {
        throw new Error(`scan: entry size is too large to decode safely at offset ${start}`);
      }
      factor *= 128;
    }
  }

  const sizeVarLen = pos - start;

  if (type === 7) {
    // REF_DELTA: 20-byte base OID follows the size varint
    if (pos + 20 > buf.length) return null;
    // Borrow a zero-copy view here and copy it into the flat ref-base table
    // immediately after header parsing. The backing scan buffer is not stable
    // once the reader advances to the next range window.
    const baseOidBytes = buf.subarray(pos, pos + 20);
    return { type, size, headerLen: sizeVarLen + 20, baseOidBytes };
  }

  if (type === 6) {
    // OFS_DELTA: variable-length negative offset follows the size varint
    if (pos >= buf.length) return null;
    let b = buf[pos++];
    let x = b & 0x7f;
    while (b & 0x80) {
      if (pos >= buf.length) return null;
      b = buf[pos++];
      x = (x + 1) * 128 + (b & 0x7f);
      if (x > MAX_PACK_OFFSET) {
        throw new Error(`scan: OFS_DELTA base distance exceeds 32-bit range at offset ${start}`);
      }
    }
    return { type, size, headerLen: pos - start, baseRel: x };
  }

  return { type, size, headerLen: sizeVarLen };
}

/**
 * Read the base_size and result_size varints from the start of a delta
 * instruction stream. These are the first two varints in the inflated delta
 * payload (before any copy/insert opcodes).
 */
function readDeltaResultSize(data: Uint8Array): number {
  const baseSize = readDeltaSizeVarint(data, 0, "delta base-size");
  const resultSize = readDeltaSizeVarint(data, baseSize.nextPos, "delta result-size");
  return resultSize.value;
}

function validatePackObjectCount(packSize: number, objectCount: number): void {
  if (objectCount > MAX_INDEXABLE_OBJECT_COUNT) {
    throw new Error(
      `scan: object count ${objectCount} exceeds safe isolate limit ${MAX_INDEXABLE_OBJECT_COUNT}`
    );
  }

  const bytesAvailableForEntries = packSize - PACK_HEADER_BYTES - PACK_TRAILER_BYTES;
  const maxPossibleObjects = Math.floor(bytesAvailableForEntries / MIN_PACKED_ENTRY_BYTES);
  if (objectCount > maxPossibleObjects) {
    throw new Error(
      `scan: object count ${objectCount} cannot fit in ${packSize} bytes of pack data`
    );
  }
}

// ---------------------------------------------------------------------------
// Buffered reader – manages a sliding window over sequential R2 range reads
// ---------------------------------------------------------------------------

class BufferedPackReader {
  private env: Env;
  private packKey: string;
  private packSize: number;
  private chunkSize: number;
  private limiter: IndexerOptions["limiter"];
  private countSub: IndexerOptions["countSubrequest"];
  private signal?: AbortSignal;

  /** Current in-memory buffer. */
  buf: Uint8Array<ArrayBufferLike> = new Uint8Array(0);
  /** Current read position within `buf`. */
  pos = 0;
  /** Absolute pack offset corresponding to buf[0]. */
  bufAbsStart = 0;

  constructor(opts: IndexerOptions) {
    this.env = opts.env;
    this.packKey = opts.packKey;
    this.packSize = opts.packSize;
    this.chunkSize = opts.chunkSize ?? DEFAULT_CHUNK_SIZE;
    this.limiter = opts.limiter;
    this.countSub = opts.countSubrequest;
    this.signal = opts.signal;
  }

  /** Absolute offset of the current read position in the pack. */
  get absPos(): number {
    return this.bufAbsStart + this.pos;
  }

  /** Number of unread bytes remaining in the current buffer. */
  get remaining(): number {
    return this.buf.length - this.pos;
  }

  /**
   * Ensure the buffer has at least `minBytes` available from the current
   * position. Reads a new chunk from R2 if necessary, concatenating with
   * any leftover bytes from the current buffer.
   */
  async ensure(minBytes: number): Promise<void> {
    while (this.remaining < minBytes) {
      // `buf` always represents one contiguous window: unread bytes from the
      // previous chunk followed by freshly fetched bytes. Reading from
      // `bufAbsStart + buf.length` therefore continues exactly where the
      // current window ends.
      const nextAbsOffset = this.bufAbsStart + this.buf.length;
      const bytesLeft = this.packSize - nextAbsOffset;
      if (bytesLeft <= 0) return;
      const readLen = Math.min(this.chunkSize, bytesLeft);

      const chunk = await readPackRange(this.env, this.packKey, nextAbsOffset, readLen, {
        limiter: this.limiter,
        countSubrequest: this.countSub,
        signal: this.signal,
      });
      if (!chunk) throw new Error("scan: unexpected R2 read failure");

      const leftover = this.buf.subarray(this.pos);
      if (leftover.length === 0) {
        this.bufAbsStart = nextAbsOffset;
        this.buf = chunk;
        this.pos = 0;
        continue;
      }

      const newBuf = new Uint8Array(leftover.length + chunk.length);
      newBuf.set(leftover, 0);
      newBuf.set(chunk, leftover.length);

      this.bufAbsStart += this.pos;
      this.buf = newBuf;
      this.pos = 0;
    }
  }

  /** Consume `n` bytes from the buffer and advance the cursor. */
  consume(n: number): Uint8Array {
    const slice = this.buf.subarray(this.pos, this.pos + n);
    this.pos += n;
    return slice;
  }
}

// ---------------------------------------------------------------------------
// scanPack
// ---------------------------------------------------------------------------

export async function scanPack(opts: IndexerOptions): Promise<ScanResult> {
  const { env, packKey, packSize, log } = opts;
  if (!Number.isSafeInteger(packSize) || packSize < MIN_PACK_BYTES) {
    throw new Error(`scan: pack size ${packSize} is smaller than the minimum valid pack size`);
  }
  const reader = new BufferedPackReader(opts);
  if (packSize - PACK_TRAILER_BYTES > MAX_PACK_OFFSET) {
    throw new Error(
      `scan: pack offsets above ${MAX_PACK_OFFSET} bytes are not supported by this indexer yet`
    );
  }

  // ---- 1. Read and validate the 12-byte pack header ----
  await reader.ensure(PACK_HEADER_BYTES);
  const headerBuf = reader.consume(PACK_HEADER_BYTES);

  const magic =
    String.fromCharCode(headerBuf[0]) +
    String.fromCharCode(headerBuf[1]) +
    String.fromCharCode(headerBuf[2]) +
    String.fromCharCode(headerBuf[3]);
  if (magic !== "PACK") throw new Error("scan: invalid pack magic");

  const hdv = new DataView(headerBuf.buffer, headerBuf.byteOffset, 12);
  const version = hdv.getUint32(4, false);
  if (version !== 2) throw new Error(`scan: unsupported pack version ${version}`);

  const objectCount = hdv.getUint32(8, false);
  validatePackObjectCount(packSize, objectCount);
  log.info("scan:start", { packKey, packSize, objectCount });
  opts.onProgress?.(`Scanning pack objects: 0% (0/${objectCount})\r`);

  // ---- 2. Allocate entry table ----
  const table = allocateEntryTable(objectCount);
  const refBaseOids: RefBaseOids = new Uint8Array(objectCount * 20);
  let refDeltaCount = 0;
  let resolvedCount = 0;

  // ---- 3. Streaming SHA-1 digest (covers everything except the trailing 20 bytes) ----
  const digestStream = createDigestStream("SHA-1");
  const digestWriter = digestStream.getWriter();
  // Feed the 12-byte header to the digest.
  await digestWriter.write(headerBuf);

  const inflator = new InflateCursor();
  const progressInterval = Math.max(1, Math.floor(objectCount / SCAN_PROGRESS_STEPS));

  // ---- 4. Sequential scan of all entries ----
  for (let i = 0; i < objectCount; i++) {
    const entryStart = reader.absPos;

    // Make sure we have enough bytes to parse the header (up to ~30 bytes for
    // type varint + OFS_DELTA distance or REF_DELTA OID).
    await reader.ensure(Math.min(64, packSize - entryStart));

    const header = parseEntryHeader(reader.buf, reader.pos);
    if (!header) throw new Error(`scan: failed to parse header at offset ${entryStart}`);

    // Record basic metadata.
    table.offsets[i] = entryStart;
    table.types[i] = header.type;
    table.headerLens[i] = header.headerLen;
    table.decompressedSizes[i] = header.size; // overwritten for deltas below

    if (header.type === 6 && header.baseRel !== undefined) {
      if (header.baseRel > entryStart) {
        throw new Error(
          `scan: OFS_DELTA at offset ${entryStart} points before the start of the pack`
        );
      }
      table.ofsBaseOffsets[i] = entryStart - header.baseRel;
    }
    if (header.type === 7 && header.baseOidBytes) {
      refBaseOids.set(header.baseOidBytes, i * 20);
      refDeltaCount++;
    }

    // CRC-32 accumulation starts with the header bytes.
    const headerBytes = reader.buf.subarray(reader.pos, reader.pos + header.headerLen);
    let crc = crc32Update(CRC32_INIT, headerBytes, 0, headerBytes.length);

    // Feed header bytes to the SHA-1 digest.
    await digestWriter.write(headerBytes);

    // Advance past the header.
    reader.pos += header.headerLen;

    // ---- Inflate the compressed payload ----
    const captureLimit = typeCodeToObjectType(header.type)
      ? header.size
      : DELTA_HEADER_CAPTURE_LIMIT;
    inflator.reset({ captureLimit });
    let firstInflatePush = true;
    while (!inflator.finished) {
      // Ensure we have data to feed.
      // The first push must include the full 2-byte zlib header. A 1-byte
      // first chunk used to mis-parse valid packs at range boundaries.
      const minBytes = firstInflatePush ? 2 : 1;
      if (reader.remaining < minBytes) {
        await reader.ensure(minBytes);
      }
      if (reader.remaining < minBytes) {
        throw new Error(`scan: unexpected EOF while inflating entry at offset ${entryStart}`);
      }
      const available = reader.buf.subarray(reader.pos, reader.pos + reader.remaining);
      inflator.push(available);
      firstInflatePush = false;

      const consumed = inflator.consumedInputBytes;
      if (consumed <= 0 && !inflator.finished) {
        throw new Error(`scan: inflate stalled at offset ${reader.absPos}`);
      }

      // Feed consumed compressed bytes to CRC and digest.
      const compressedSlice = reader.buf.subarray(reader.pos, reader.pos + consumed);
      crc = crc32Update(crc, compressedSlice, 0, compressedSlice.length);
      await digestWriter.write(compressedSlice);

      reader.pos += consumed;
    }

    // Record span end and CRC.
    table.spanEnds[i] = reader.absPos;
    table.crc32s[i] = crc32Finish(crc);

    // ---- Compute OID for non-delta objects ----
    const baseType = typeCodeToObjectType(header.type);
    if (baseType) {
      const inflated = inflator.output;
      if (inflated.length !== header.size) {
        throw new Error(
          `scan: inflated ${baseType} size mismatch at offset ${entryStart} (expected ${header.size}, got ${inflated.length})`
        );
      }
      // Non-delta: hash "<type> <size>\0<payload>" to get the OID.
      table.oids.set(await computeOidBytes(baseType, inflated), i * 20);
      table.resolved[i] = 1;
      table.decompressedSizes[i] = inflated.length;
      resolvedCount++;
    } else {
      if (inflator.outputLength !== header.size) {
        throw new Error(
          `scan: inflated delta size mismatch at offset ${entryStart} (expected ${header.size}, got ${inflator.outputLength})`
        );
      }
      // Pack headers store the size of the inflated delta *program*, not the
      // final post-apply object size. The apply step needs the latter, so we
      // read the delta's declared result size here and stash it for resolve().
      const resultSize = readDeltaResultSize(inflator.capturedOutput);
      table.decompressedSizes[i] = resultSize;
      table.resolved[i] = 0;
    }

    // Log progress periodically.
    if ((i + 1) % 10000 === 0 || i + 1 === objectCount) {
      log.debug("scan:progress", { processed: i + 1, total: objectCount });
    }
    if ((i + 1) % progressInterval === 0 || i + 1 === objectCount) {
      emitScanProgress(opts.onProgress, i + 1, objectCount);
    }
  }

  // ---- 5. Validate trailing SHA-1 checksum ----
  // A valid pack has no slack bytes between the final entry and the trailing
  // 20-byte checksum. The digest covers exactly bytes [0, packSize - 20), so
  // accepting extra bytes here would let a malformed pack smuggle undeclared
  // data past the scanner while still reusing the original trailer hash.
  const trailingOffset = packSize - PACK_TRAILER_BYTES;
  if (reader.absPos !== trailingOffset) {
    throw new Error(
      `scan: expected indexed entries to end at ${trailingOffset}, got ${reader.absPos}`
    );
  }

  await digestWriter.close();
  const computedHash = new Uint8Array(await digestStream.digest);

  // Read the trailing 20 bytes.
  const trailer = await readPackRange(env, packKey, trailingOffset, PACK_TRAILER_BYTES, {
    limiter: opts.limiter,
    countSubrequest: opts.countSubrequest,
    signal: opts.signal,
  });
  if (!trailer || trailer.length !== PACK_TRAILER_BYTES) {
    throw new Error("scan: failed to read pack trailer checksum");
  }

  // Compare.
  for (let i = 0; i < 20; i++) {
    if (computedHash[i] !== trailer[i]) {
      throw new Error(
        `scan: pack checksum mismatch (computed ${bytesToHex(computedHash)} != trailing ${bytesToHex(trailer)})`
      );
    }
  }

  log.info("scan:done", {
    objectCount,
    resolved: resolvedCount,
    deltas: objectCount - resolvedCount,
  });

  return {
    table,
    refBaseOids,
    refDeltaCount,
    resolvedCount,
    objectCount,
    packChecksum: trailer,
  };
}
