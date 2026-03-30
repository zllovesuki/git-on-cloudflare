import { readPackRange } from "@/git/pack/packMeta.ts";

import { InflateCursor } from "../inflateCursor.ts";
import type { PackEntryTable, ResolveOptions } from "../types.ts";
import { throwIfAborted } from "./errors.ts";

export class SequentialReader {
  private buf: Uint8Array<ArrayBufferLike> = new Uint8Array(0);
  private bufAbsStart = 0;
  private env: Env;
  private packKey: string;
  private packSize: number;
  private chunkSize: number;
  private limiter: ResolveOptions["limiter"];
  private countSub: ResolveOptions["countSubrequest"];
  private log: ResolveOptions["log"];
  private signal?: AbortSignal;

  constructor(
    env: Env,
    packKey: string,
    packSize: number,
    chunkSize: number,
    limiter: ResolveOptions["limiter"],
    countSub: ResolveOptions["countSubrequest"],
    log: ResolveOptions["log"],
    signal?: AbortSignal
  ) {
    this.env = env;
    this.packKey = packKey;
    this.packSize = packSize;
    this.chunkSize = chunkSize;
    this.limiter = limiter;
    this.countSub = countSub;
    this.log = log;
    this.signal = signal;
  }

  throwIfAborted(stage: string): void {
    throwIfAborted(this.signal, this.log, stage);
  }

  /**
   * Read a byte range from the pack. If the range falls within the current
   * buffered chunk, return a subarray. Otherwise preload a new chunk starting
   * at the requested offset so nearby follow-on reads stay coalesced.
   */
  async readRange(offset: number, length: number): Promise<Uint8Array> {
    this.throwIfAborted("reader:read-range");
    const bufEnd = this.bufAbsStart + this.buf.length;
    if (offset >= this.bufAbsStart && offset + length <= bufEnd) {
      const localStart = offset - this.bufAbsStart;
      return this.buf.subarray(localStart, localStart + length);
    }
    if (length > this.chunkSize) {
      const data = await readPackRange(this.env, this.packKey, offset, length, {
        limiter: this.limiter,
        countSubrequest: this.countSub,
        signal: this.signal,
      });
      if (!data) {
        this.throwIfAborted("reader:read-range");
        throw new Error("resolve: R2 read failure");
      }
      return data;
    }

    await this.preload(offset);
    const preloadEnd = this.bufAbsStart + this.buf.length;
    if (offset + length <= preloadEnd) {
      const localStart = offset - this.bufAbsStart;
      return this.buf.subarray(localStart, localStart + length);
    }

    const data = await readPackRange(this.env, this.packKey, offset, length, {
      limiter: this.limiter,
      countSubrequest: this.countSub,
      signal: this.signal,
    });
    if (!data) {
      this.throwIfAborted("reader:read-range");
      throw new Error("resolve: R2 read failure");
    }
    return data;
  }

  /**
   * Return the largest already-buffered window starting at `offset`, preloading
   * a new chunk when needed. Unlike `readRange()`, this intentionally does not
   * stitch together the full requested span, so pass-2 inflate can stream large
   * entries without double-buffering their compressed bytes.
   */
  async readWindow(offset: number, maxLength: number): Promise<Uint8Array> {
    this.throwIfAborted("reader:read-window");
    if (maxLength <= 0 || offset >= this.packSize) return new Uint8Array(0);

    const bufEnd = this.bufAbsStart + this.buf.length;
    if (!(offset >= this.bufAbsStart && offset < bufEnd)) {
      await this.preload(offset);
    }

    const windowEnd = this.bufAbsStart + this.buf.length;
    if (offset < this.bufAbsStart || offset >= windowEnd) {
      const data = await readPackRange(this.env, this.packKey, offset, Math.min(maxLength, 1), {
        limiter: this.limiter,
        countSubrequest: this.countSub,
        signal: this.signal,
      });
      if (!data) {
        this.throwIfAborted("reader:read-window");
        throw new Error("resolve: R2 read failure");
      }
      return data;
    }

    const localStart = offset - this.bufAbsStart;
    const localLength = Math.min(maxLength, windowEnd - offset);
    return this.buf.subarray(localStart, localStart + localLength);
  }

  /** Preload a large sequential chunk starting at the given offset. */
  async preload(offset: number): Promise<void> {
    this.throwIfAborted("reader:preload");
    const bytesLeft = this.packSize - offset;
    if (bytesLeft <= 0) return;
    const readLen = Math.min(this.chunkSize, bytesLeft);
    const chunk = await readPackRange(this.env, this.packKey, offset, readLen, {
      limiter: this.limiter,
      countSubrequest: this.countSub,
      signal: this.signal,
    });
    if (!chunk) {
      this.throwIfAborted("reader:preload");
      throw new Error("resolve: R2 preload failure");
    }
    this.buf = chunk;
    this.bufAbsStart = offset;
  }
}

/** Inflate a pack entry's compressed payload using a buffered pack reader. */
export async function inflateFromReader(
  reader: SequentialReader,
  table: PackEntryTable,
  index: number
): Promise<Uint8Array> {
  reader.throwIfAborted("reader:inflate-entry");
  const payloadStart = table.offsets[index] + table.headerLens[index];
  const cursor = new InflateCursor();
  let nextOffset = payloadStart;
  let firstPush = true;

  while (!cursor.finished) {
    reader.throwIfAborted("reader:inflate-entry");
    const bytesLeft = table.spanEnds[index] - nextOffset;
    if (bytesLeft <= 0) {
      throw new Error(`resolve: incomplete inflate for entry at offset ${table.offsets[index]}`);
    }

    const minBytes = firstPush ? 2 : 1;
    let window = await reader.readWindow(nextOffset, bytesLeft);
    if (window.length < minBytes) {
      // A chunk size of 1 is valid in tests and can split the zlib wrapper at
      // arbitrary boundaries. Stitch just the minimum prefix needed for the
      // inflate cursor to make forward progress.
      window = await reader.readRange(nextOffset, Math.min(bytesLeft, minBytes));
    }
    if (window.length < minBytes) {
      throw new Error(
        `resolve: unexpected EOF while inflating entry at offset ${table.offsets[index]}`
      );
    }

    cursor.push(window);
    firstPush = false;

    const consumed = cursor.consumedInputBytes;
    if (consumed <= 0 && !cursor.finished) {
      throw new Error(`resolve: inflate stalled at offset ${nextOffset}`);
    }
    nextOffset += consumed;
  }

  if (nextOffset !== table.spanEnds[index]) {
    throw new Error(
      `resolve: inflate span mismatch at offset ${table.offsets[index]} (expected end ${table.spanEnds[index]}, got ${nextOffset})`
    );
  }
  return cursor.output;
}
