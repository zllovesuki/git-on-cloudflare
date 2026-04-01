import { pktLine, flushPkt } from "@/git/core/index.ts";
import type { Logger } from "@/common/logger.ts";

export const SIDEBAND_PAYLOAD_MAX_BYTES = 65_515;

type SidebandEnqueueController = {
  enqueue(chunk: Uint8Array): void;
};

export function createSidebandPacketChunks(
  band: 1 | 2 | 3,
  payload: Uint8Array,
  maxChunk: number = SIDEBAND_PAYLOAD_MAX_BYTES
): Uint8Array[] {
  const chunks: Uint8Array[] = [];
  for (let off = 0; off < payload.byteLength; off += maxChunk) {
    const slice = payload.subarray(off, Math.min(off + maxChunk, payload.byteLength));
    const banded = new Uint8Array(1 + slice.byteLength);
    banded[0] = band;
    banded.set(slice, 1);
    chunks.push(pktLine(banded));
  }
  if (payload.byteLength === 0) {
    const banded = new Uint8Array(1);
    banded[0] = band;
    chunks.push(pktLine(banded));
  }
  return chunks;
}

export function enqueueSidebandPayload(
  controller: SidebandEnqueueController,
  band: 1 | 2 | 3,
  payload: Uint8Array,
  maxChunk?: number
): void {
  for (const chunk of createSidebandPacketChunks(band, payload, maxChunk)) {
    controller.enqueue(chunk);
  }
}

export class SidebandProgressMux {
  private progressMessages: string[] = [];
  private progressIdx = 0;
  private lastProgressTime = 0;
  private inProgress = false;
  private resolveFirstProgress?: () => void;
  private firstProgressPromise: Promise<void>;
  private readonly intervalMs: number;

  constructor(intervalMs = 100) {
    this.intervalMs = intervalMs;
    this.firstProgressPromise = new Promise<void>((resolve) => {
      this.resolveFirstProgress = resolve;
    });
  }

  push(msg: string): void {
    this.progressMessages.push(msg);
    if (this.resolveFirstProgress) {
      this.resolveFirstProgress();
      this.resolveFirstProgress = undefined;
    }
  }

  async waitForFirst(timeoutMs = 20): Promise<void> {
    await Promise.race([this.firstProgressPromise, new Promise((r) => setTimeout(r, timeoutMs))]);
  }

  shouldSendProgress(): boolean {
    const now = Date.now();
    return (
      now - this.lastProgressTime >= this.intervalMs &&
      !this.inProgress &&
      this.progressIdx < this.progressMessages.length
    );
  }

  async sendPending(emitFn: (msg: string) => void): Promise<void> {
    if (this.shouldSendProgress()) {
      this.inProgress = true;
      while (this.progressIdx < this.progressMessages.length) {
        emitFn(this.progressMessages[this.progressIdx++]);
      }
      this.lastProgressTime = Date.now();
      this.inProgress = false;
    }
  }

  sendRemaining(emitFn: (msg: string) => void): void {
    while (this.progressIdx < this.progressMessages.length) {
      emitFn(this.progressMessages[this.progressIdx++]);
    }
  }
}

export function createSidebandTransform(options?: {
  onProgress?: (msg: string) => void;
  signal?: AbortSignal;
}): TransformStream<Uint8Array, Uint8Array> {
  return new TransformStream<Uint8Array, Uint8Array>({
    async transform(chunk, controller) {
      if (options?.signal?.aborted) {
        controller.terminate();
        return;
      }

      enqueueSidebandPayload(controller, 1, chunk);
    },

    flush(controller) {},
  });
}

export function emitProgress(
  controller: ReadableStreamDefaultController<Uint8Array>,
  message: string
) {
  enqueueSidebandPayload(controller, 2, new TextEncoder().encode(message));
}

export function emitFatal(
  controller: ReadableStreamDefaultController<Uint8Array>,
  message: string
) {
  enqueueSidebandPayload(controller, 3, new TextEncoder().encode(`fatal: ${message}\n`));
}

export async function pipePackWithSideband(
  packStream: ReadableStream<Uint8Array>,
  controller: ReadableStreamDefaultController<Uint8Array>,
  options: {
    signal?: AbortSignal;
    progressMux: SidebandProgressMux;
    log: Logger;
  }
): Promise<void> {
  const { signal, progressMux, log } = options;

  try {
    const sidebandTransform = createSidebandTransform({ signal });
    const reader = packStream.pipeThrough(sidebandTransform).getReader();

    await progressMux.waitForFirst();
    progressMux.sendRemaining((msg) => emitProgress(controller, msg));

    while (true) {
      if (signal?.aborted) {
        log.debug("pipe:aborted");
        reader.cancel();
        break;
      }

      const { done, value } = await reader.read();
      if (done) break;

      await progressMux.sendPending((msg) => emitProgress(controller, msg));
      controller.enqueue(value);
    }

    progressMux.sendRemaining((msg) => emitProgress(controller, msg));
    controller.enqueue(flushPkt());
  } catch (error) {
    log.error("pipe:error", { error: String(error) });
    try {
      emitFatal(controller, String(error));
    } catch {}
    throw error;
  }
}
