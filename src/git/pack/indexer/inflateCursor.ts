/**
 * Byte-accounting inflate wrapper and CRC-32 helpers for the streaming pack indexer.
 *
 * Why pako instead of the Web DecompressionStream:
 * Git pack entries are concatenated zlib streams. The indexer must know exactly how
 * many compressed bytes each entry consumed so it can compute span boundaries and
 * CRC-32 values. The Web DecompressionStream API does not expose byte-position
 * accounting; pako's Inflate class in raw mode exposes `strm.total_in` which gives
 * us the precise count.
 *
 * Implementation note: pako's zlib-mode Inflate has a known limitation with
 * trailing bytes after Z_STREAM_END. We work around this by using raw mode
 * (`raw: true`) and manually accounting for the 2-byte zlib header and 4-byte
 * Adler-32 checksum that wrap the raw deflate data in each pack entry.
 */

import { Inflate } from "pako";

// ---------------------------------------------------------------------------
// CRC-32  (ISO 3309 / ITU-T V.42, polynomial 0xEDB88320)
// ---------------------------------------------------------------------------

const CRC32_TABLE = new Uint32Array(256);
for (let i = 0; i < 256; i++) {
  let c = i;
  for (let j = 0; j < 8; j++) {
    c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
  }
  CRC32_TABLE[i] = c;
}

/** Start value for a fresh CRC-32 accumulation. */
export const CRC32_INIT = 0xffffffff;

/** Feed a byte range into a running CRC-32 value. */
export function crc32Update(crc: number, data: Uint8Array, start = 0, end = data.length): number {
  for (let i = start; i < end; i++) {
    crc = CRC32_TABLE[(crc ^ data[i]) & 0xff] ^ (crc >>> 8);
  }
  return crc;
}

/** Finalize a running CRC-32 value to its unsigned 32-bit result. */
export function crc32Finish(crc: number): number {
  return (crc ^ 0xffffffff) >>> 0;
}

// ---------------------------------------------------------------------------
// Adler-32 (RFC 1950 zlib wrapper trailer)
// ---------------------------------------------------------------------------

const ADLER32_MOD = 65521;
const ADLER32_INIT = 1;

function adler32Update(adler: number, data: Uint8Array, start = 0, end = data.length): number {
  let a = adler & 0xffff;
  let b = (adler >>> 16) & 0xffff;
  for (let i = start; i < end; i++) {
    a += data[i];
    if (a >= ADLER32_MOD) a -= ADLER32_MOD;
    b += a;
    if (b >= ADLER32_MOD) b -= ADLER32_MOD;
  }
  return ((b & 0xffff) << 16) | (a & 0xffff);
}

// ---------------------------------------------------------------------------
// Zlib header helpers
// ---------------------------------------------------------------------------

/** Size of the zlib (RFC 1950) header: CMF + FLG bytes. */
const ZLIB_HEADER_SIZE = 2;

/** Size of the zlib (RFC 1950) Adler-32 trailer. */
const ZLIB_TRAILER_SIZE = 4;

/**
 * Validate a zlib header (CMF + FLG). Returns true if valid.
 * Per RFC 1950: (CMF * 256 + FLG) % 31 === 0, CM must be 8 (deflate).
 */
function isValidZlibHeader(cmf: number, flg: number): boolean {
  if ((cmf & 0x0f) !== 8) return false; // CM must be deflate
  return (cmf * 256 + flg) % 31 === 0;
}

// ---------------------------------------------------------------------------
// InflateCursor – pako wrapper with byte accounting
// ---------------------------------------------------------------------------

/**
 * InflateCursor wraps a pako Inflate instance in raw mode and tracks the exact
 * number of compressed bytes consumed from the input (including the zlib
 * header and trailer). After each `push()` call the caller can inspect the
 * confirmed consumed-byte count and carry-forward tail for the current pack
 * entry.
 *
 * The zlib header (2 bytes) is consumed on the first push. The raw deflate
 * data is processed by pako. The Adler-32 trailer (4 bytes) is accounted
 * for after the deflate stream ends.
 */
export class InflateCursor {
  private inf!: Inflate;
  private chunks: Uint8Array[] = [];
  private totalOutputBytes = 0;
  private capturedOutputBytes = 0;
  private captureLimit = Number.MAX_SAFE_INTEGER;
  private outputTruncated = false;
  private rawDeflateDone = false;
  private entryDone = false;
  private adler32 = ADLER32_INIT;
  private trailerBytes = new Uint8Array(ZLIB_TRAILER_SIZE);

  /** Whether the 2-byte zlib header has been consumed from the input. */
  private headerConsumed = false;
  /** How many Adler-32 trailer bytes have been consumed so far. */
  private trailerBytesConsumed = 0;
  /** Bytes confirmed consumed from the most recent push(). */
  private lastBytesConsumed = 0;
  /** Tail from the most recent push() that belongs to the next pack entry. */
  private lastUnconsumedInput: Uint8Array<ArrayBufferLike> = new Uint8Array(0);

  constructor() {
    this.createInflate();
  }

  private createInflate(): void {
    // Use raw mode to avoid pako's zlib-mode trailing-byte bug.
    // We handle the zlib header/trailer manually so the scanner can both
    // account for exact consumed bytes and validate the zlib wrapper itself.
    const inf = new Inflate({ raw: true });
    inf.onData = (chunk: Uint8Array) => {
      this.adler32 = adler32Update(this.adler32, chunk, 0, chunk.length);
      this.totalOutputBytes += chunk.length;
      const captureRemaining = this.captureLimit - this.capturedOutputBytes;
      if (captureRemaining <= 0) {
        this.outputTruncated = this.outputTruncated || chunk.length > 0;
        return;
      }
      if (chunk.length <= captureRemaining) {
        this.chunks.push(chunk);
        this.capturedOutputBytes += chunk.length;
        return;
      }
      this.chunks.push(chunk.slice(0, captureRemaining));
      this.capturedOutputBytes += captureRemaining;
      this.outputTruncated = true;
    };
    inf.onEnd = (_status: number) => {
      // Handled via err check in push().
    };
    this.inf = inf;
  }

  /**
   * Feed compressed bytes into the inflate engine.
   * After this call, check `finished` and `consumedInputBytes`.
   *
   * The first call must include at least the 2-byte zlib header. The cursor
   * strips the header and feeds only the raw deflate data to pako.
   */
  push(data: Uint8Array): void {
    if (this.entryDone) {
      this.lastBytesConsumed = 0;
      this.lastUnconsumedInput = data;
      return;
    }

    let pos = 0;
    let consumed = 0;

    // On the first push, skip the 2-byte zlib header.
    if (!this.headerConsumed) {
      if (data.length < ZLIB_HEADER_SIZE) {
        throw new Error("inflate: buffer too small for zlib header");
      }
      if (!isValidZlibHeader(data[0], data[1])) {
        throw new Error(
          `inflate: invalid zlib header (CMF=0x${data[0].toString(16)}, FLG=0x${data[1].toString(16)})`
        );
      }
      if (data[1] & 0x20) {
        // Git packs never use preset dictionaries. In raw mode pako would
        // otherwise treat the FDICT bit as just more wrapper bytes, so we
        // reject it explicitly here before any payload bytes are accepted.
        throw new Error("inflate: zlib preset dictionaries are not supported");
      }
      pos = ZLIB_HEADER_SIZE;
      consumed = ZLIB_HEADER_SIZE;
      this.headerConsumed = true;
    }

    if (!this.rawDeflateDone && pos < data.length) {
      // Push only the raw deflate bytes to pako. We track the trailing Adler
      // bytes separately because pako reports raw-stream completion before the
      // zlib wrapper has necessarily been fully consumed from this chunk.
      // `ended === true` here only means "raw deflate is done", not "the pack
      // entry is done". The scanner must keep feeding bytes until the wrapper's
      // 4-byte trailer has been accounted for too.
      const rawInput = data.subarray(pos);
      // pako does not expose stable public byte-accounting APIs for this use
      // case, so the scanner intentionally reaches into `strm.total_in` and
      // `ended`. Keep that footgun documented here because a pako upgrade could
      // break the indexer even if TypeScript still compiles.
      const strm = (this.inf as unknown as { strm: { total_in: number } }).strm;
      const beforeTotalIn = strm.total_in;
      this.inf.push(rawInput, false);
      if ((this.inf.err as number) !== 0) {
        throw new Error(`inflate error ${this.inf.err}: ${this.inf.msg}`);
      }
      const rawConsumed = strm.total_in - beforeTotalIn;
      pos += rawConsumed;
      consumed += rawConsumed;
      if ((this.inf as unknown as { ended: boolean }).ended) {
        this.rawDeflateDone = true;
      }
    }

    if (this.rawDeflateDone && pos < data.length) {
      const trailerNeeded = ZLIB_TRAILER_SIZE - this.trailerBytesConsumed;
      const trailerBytes = Math.min(trailerNeeded, data.length - pos);
      this.trailerBytes.set(data.subarray(pos, pos + trailerBytes), this.trailerBytesConsumed);
      this.trailerBytesConsumed += trailerBytes;
      pos += trailerBytes;
      consumed += trailerBytes;
      if (this.trailerBytesConsumed === ZLIB_TRAILER_SIZE) {
        const dv = new DataView(
          this.trailerBytes.buffer,
          this.trailerBytes.byteOffset,
          this.trailerBytes.byteLength
        );
        const expectedAdler32 = dv.getUint32(0, false);
        if (expectedAdler32 !== this.adler32 >>> 0) {
          throw new Error(
            `inflate: Adler-32 mismatch (expected 0x${expectedAdler32.toString(16)}, got 0x${(this.adler32 >>> 0).toString(16)})`
          );
        }
        this.entryDone = true;
      }
    }

    this.lastBytesConsumed = consumed;
    // Anything left here belongs to the next pack entry. We only expose that
    // tail after consuming the current entry's trailer bytes.
    this.lastUnconsumedInput = pos >= data.length ? new Uint8Array(0) : data.subarray(pos);
  }

  /**
   * Total compressed input bytes consumed by this zlib stream, including the
   * 2-byte zlib header and 4-byte Adler-32 trailer.
   */
  get bytesConsumed(): number {
    const strm = (this.inf as unknown as { strm: { total_in: number } }).strm;
    return (this.headerConsumed ? ZLIB_HEADER_SIZE : 0) + strm.total_in + this.trailerBytesConsumed;
  }

  /** Bytes confirmed consumed from the most recent push(). */
  get consumedInputBytes(): number {
    return this.lastBytesConsumed;
  }

  /** Whether the zlib stream has fully completed (deflate done + trailer accounted). */
  get finished(): boolean {
    return this.entryDone;
  }

  /** The concatenated decompressed output. Collapses intermediate chunks on first access. */
  get output(): Uint8Array {
    if (this.outputTruncated) {
      throw new Error("inflate: full output was not retained for this entry");
    }
    if (this.chunks.length === 1) return this.chunks[0];
    const out = new Uint8Array(this.totalOutputBytes);
    let pos = 0;
    for (const c of this.chunks) {
      out.set(c, pos);
      pos += c.length;
    }
    // Replace chunks with single concatenated result to release intermediate references.
    this.chunks = [out];
    return out;
  }

  /**
   * Returns the retained output prefix. This may be shorter than the full
   * inflated payload when the caller only needs the delta header prefix.
   */
  get capturedOutput(): Uint8Array {
    if (this.chunks.length === 0) return new Uint8Array(0);
    if (this.chunks.length === 1) return this.chunks[0];
    const out = new Uint8Array(this.capturedOutputBytes);
    let pos = 0;
    for (const c of this.chunks) {
      out.set(c, pos);
      pos += c.length;
    }
    this.chunks = [out];
    return out;
  }

  /** Total decompressed output bytes seen for the current entry. */
  get outputLength(): number {
    return this.totalOutputBytes;
  }

  /**
   * Returns the bytes from the last push() that were not consumed by the
   * inflate engine (because they belong to the *next* pack entry). The
   * caller should carry these forward as the prefix of the next entry.
   *
   * When the inflate is finished, this accounts for the 4-byte Adler-32
   * trailer that follows the raw deflate data.
   */
  get unconsumedInput(): Uint8Array<ArrayBufferLike> {
    return this.lastUnconsumedInput;
  }

  /** Reset for the next pack entry. Releases the previous pako instance. */
  reset(options?: { captureLimit?: number }): void {
    // Release the old pako Inflate instance's internal state before creating a new one.
    (this.inf as unknown) = null;
    this.createInflate();
    this.chunks.length = 0;
    this.totalOutputBytes = 0;
    this.capturedOutputBytes = 0;
    this.captureLimit = options?.captureLimit ?? Number.MAX_SAFE_INTEGER;
    this.outputTruncated = false;
    this.rawDeflateDone = false;
    this.entryDone = false;
    this.adler32 = ADLER32_INIT;
    this.trailerBytes.fill(0);
    this.headerConsumed = false;
    this.trailerBytesConsumed = 0;
    this.lastBytesConsumed = 0;
    this.lastUnconsumedInput = new Uint8Array(0);
  }
}
