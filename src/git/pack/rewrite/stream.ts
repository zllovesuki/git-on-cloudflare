import type {
  OrderedPackSnapshot,
  OrderedPackSnapshotEntry,
} from "@/git/operations/fetch/types.ts";
import type { Logger } from "@/common/logger.ts";

import { createDigestStream } from "@/common/index.ts";
import { isResolveAbortedError } from "@/git/pack/indexer/resolve/errors.ts";
import { encodeOfsDeltaDistance } from "../packMeta.ts";
import {
  WHOLE_PACK_MAX_BYTES,
  buildPackHeader,
  countRewriteSubrequest,
  type PackReadState,
  type RewriteOptions,
  type SelectionTable,
} from "./shared.ts";

// ---------------------------------------------------------------------------
// Entry header construction
// ---------------------------------------------------------------------------

function buildEntryHeaderBytes(table: SelectionTable, sel: number): Uint8Array | undefined {
  const type = table.typeCodes[sel];
  const svStart = sel * 5;
  const svLen = table.sizeVarLens[sel];
  if (svLen === 0) return undefined;

  if (type === 6) {
    const base = table.baseSlots[sel];
    if (base < 0) return undefined;
    const distance = table.outputOffsets[sel] - table.outputOffsets[base];
    const distBytes = encodeOfsDeltaDistance(distance);
    const out = new Uint8Array(svLen + distBytes.length);
    out.set(table.sizeVarBuf.subarray(svStart, svStart + svLen), 0);
    out.set(distBytes, svLen);
    return out;
  }

  if (type === 7) {
    if (!table.baseOidRaw) return undefined;
    const baseOidBytes = table.baseOidRaw.subarray(sel * 20, sel * 20 + 20);
    const out = new Uint8Array(svLen + 20);
    out.set(table.sizeVarBuf.subarray(svStart, svStart + svLen), 0);
    out.set(baseOidBytes, svLen);
    return out;
  }

  // Non-delta: just the size varint (subarray is safe — table is immutable during streaming)
  return table.sizeVarBuf.subarray(svStart, svStart + svLen);
}

// ---------------------------------------------------------------------------
// Payload emission
// ---------------------------------------------------------------------------

async function emitPackPayload(
  controller: ReadableStreamDefaultController<Uint8Array>,
  writer: WritableStreamDefaultWriter<Uint8Array>,
  table: SelectionTable,
  sel: number,
  state: PackReadState
): Promise<void> {
  const payloadStart = table.offsets[sel] + table.headerLens[sel];
  let bytesLeft = table.payloadLens[sel];
  if (bytesLeft <= 0) return;

  if (state.wholePack) {
    const payload = state.wholePack.subarray(payloadStart, payloadStart + bytesLeft);
    await writer.write(payload);
    controller.enqueue(payload);
    return;
  }

  let currentOffset = payloadStart;
  while (bytesLeft > 0) {
    let window = await state.reader.readWindow(currentOffset, bytesLeft);
    if (window.length === 0) {
      window = await state.reader.readRange(currentOffset, Math.min(bytesLeft, 1));
    }
    if (window.length === 0) {
      throw new Error(
        `rewrite: unexpected EOF while streaming pack#${table.packSlots[sel]} entry#${table.entryIndices[sel]}`
      );
    }

    await writer.write(window);
    controller.enqueue(window);
    currentOffset += window.length;
    bytesLeft -= window.length;
  }
}

// ---------------------------------------------------------------------------
// Passthrough stream (single pack, all objects selected)
// ---------------------------------------------------------------------------

export async function passthroughSinglePack(
  env: Env,
  snapshotPack: OrderedPackSnapshotEntry,
  readState: PackReadState,
  controller: ReadableStreamDefaultController<Uint8Array>,
  log: Logger,
  warnedFlags: Set<string>,
  options?: RewriteOptions
): Promise<"completed" | "aborted"> {
  const digestStream = createDigestStream("SHA-1");
  const writer = digestStream.getWriter();

  if (options?.signal?.aborted) {
    await writer.abort();
    return "aborted";
  }

  const emit = async (chunk: Uint8Array) => {
    await writer.write(chunk);
    controller.enqueue(chunk);
  };

  options?.onProgress?.(`Enumerating objects: ${snapshotPack.idx.count}, from 1 packs\n`);

  if (readState.wholePack) {
    if (readState.wholePack.length < 20) {
      throw new Error("rewrite: passthrough pack read failed");
    }
    if (options?.signal?.aborted) {
      await writer.abort();
      return "aborted";
    }
    await emit(readState.wholePack.subarray(0, readState.wholePack.length - 20));
  } else if (snapshotPack.packBytes <= WHOLE_PACK_MAX_BYTES) {
    throw new Error("rewrite: missing whole-pack preload for passthrough");
  } else {
    countRewriteSubrequest(
      log,
      warnedFlags,
      options,
      `rewrite-passthrough:${snapshotPack.packKey}`,
      { op: "r2:get-pack", packKey: snapshotPack.packKey }
    );
    await options!.limiter!.run("r2:get-pack", async () => {
      const packObject = await env.REPO_BUCKET.get(snapshotPack.packKey);
      if (!packObject?.body) {
        throw new Error("rewrite: passthrough pack stream unavailable");
      }

      const reader = packObject.body.getReader();
      let trailing = new Uint8Array(0);
      while (true) {
        if (options?.signal?.aborted) {
          await reader.cancel();
          await writer.abort();
          throw new Error("rewrite: passthrough aborted");
        }
        const { done, value } = await reader.read();
        if (done) break;
        if (!value) continue;

        const chunk = new Uint8Array(trailing.length + value.length);
        chunk.set(trailing, 0);
        chunk.set(value, trailing.length);
        if (chunk.length <= 20) {
          trailing = chunk;
          continue;
        }

        const bodyChunk = chunk.subarray(0, chunk.length - 20);
        trailing = chunk.slice(chunk.length - 20);
        if (options?.signal?.aborted) {
          await reader.cancel();
          await writer.abort();
          throw new Error("rewrite: passthrough aborted");
        }
        await emit(bodyChunk);
      }

      if (trailing.length < 20) {
        throw new Error("rewrite: truncated passthrough pack");
      }
    });
  }

  await writer.close();
  options?.onProgress?.(
    `Counting objects: 100% (${snapshotPack.idx.count}/${snapshotPack.idx.count}), done.\n`
  );
  controller.enqueue(new Uint8Array(await digestStream.digest));
  return "completed";
}

export function createPassthroughStream(args: {
  env: Env;
  snapshotPack: OrderedPackSnapshotEntry;
  readState: PackReadState;
  log: Logger;
  warnedFlags: Set<string>;
  options?: RewriteOptions;
  onComplete?: () => void;
}): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    async start(controller) {
      try {
        const status = await passthroughSinglePack(
          args.env,
          args.snapshotPack,
          args.readState,
          controller,
          args.log,
          args.warnedFlags,
          args.options
        );
        if (status === "aborted") {
          args.log.debug("rewrite:passthrough-aborted");
          controller.close();
          return;
        }
        args.onComplete?.();
        controller.close();
      } catch (error) {
        if (
          isResolveAbortedError(error) ||
          args.options?.signal?.aborted ||
          (error instanceof Error && error.message === "rewrite: passthrough aborted")
        ) {
          args.log.debug("rewrite:passthrough-aborted");
          controller.close();
          return;
        }
        args.log.error("rewrite:passthrough-error", { error: String(error) });
        controller.error(error);
      }
    },
  });
}

// ---------------------------------------------------------------------------
// Rewrite stream (multi-pack or partial selection)
// ---------------------------------------------------------------------------

export function createRewriteStream(
  table: SelectionTable,
  snapshot: OrderedPackSnapshot,
  readStates: Map<number, PackReadState>,
  log: Logger,
  options?: RewriteOptions,
  onComplete?: () => void
): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    async start(controller) {
      let writer: WritableStreamDefaultWriter<Uint8Array> | undefined;
      try {
        const digestStream = createDigestStream("SHA-1");
        const digestWriter = digestStream.getWriter();
        writer = digestWriter;

        const emit = async (chunk: Uint8Array) => {
          await digestWriter.write(chunk);
          controller.enqueue(chunk);
        };

        await emit(buildPackHeader(table.count));
        options?.onProgress?.(
          `Enumerating objects: ${table.count}, from ${readStates.size} packs\n`
        );

        const progressInterval = Math.max(1, Math.floor(table.count / 10));
        let streamed = 0;

        for (let i = 0; i < table.count; i++) {
          if (options?.signal?.aborted) {
            log.debug("rewrite:stream-aborted");
            await digestWriter.abort();
            controller.close();
            return;
          }

          const sel = table.outputOrder[i];
          const packSlot = table.packSlots[sel];
          const readState = readStates.get(packSlot);
          const headerBytes = buildEntryHeaderBytes(table, sel);
          if (!headerBytes || !readState) {
            const pack = snapshot.packs[packSlot];
            throw new Error(
              `rewrite: missing stream state for ${pack?.packKey}#${table.entryIndices[sel]}`
            );
          }

          await emit(headerBytes);
          await emitPackPayload(controller, writer, table, sel, readState);

          streamed++;
          if (streamed % progressInterval === 0 || streamed === table.count) {
            const percent = Math.round((streamed / table.count) * 100);
            if (streamed === table.count) {
              options?.onProgress?.(
                `Counting objects: 100% (${table.count}/${table.count}), done.\n`
              );
            } else {
              options?.onProgress?.(`Counting objects: ${percent}% (${streamed}/${table.count})\r`);
            }
          }
        }

        await digestWriter.close();
        controller.enqueue(new Uint8Array(await digestStream.digest));
        onComplete?.();
        controller.close();
      } catch (error) {
        if (isResolveAbortedError(error) || options?.signal?.aborted) {
          log.debug("rewrite:stream-aborted");
          try {
            await writer?.abort();
          } catch {}
          controller.close();
          return;
        }
        log.error("rewrite:stream-error", { error: String(error) });
        controller.error(error);
      }
    },
  });
}
