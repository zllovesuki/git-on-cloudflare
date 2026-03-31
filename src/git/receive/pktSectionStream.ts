import { DELIM, FLUSH, RESPONSE_END } from "@/git/core/pktline.ts";
import { appendBytes, cloneBytes } from "./bytes.ts";

const MAX_COMMAND_SECTION_BYTES = 256 * 1024;

type ParsedPktSection =
  | {
      status: "ok";
      lines: string[];
      offset: number;
    }
  | {
      status: "incomplete";
    }
  | {
      status: "invalid";
      message: string;
    };

function parsePktSectionPrefix(bytes: Uint8Array): ParsedPktSection {
  const decoder = new TextDecoder();
  const lines: string[] = [];
  let offset = 0;

  while (offset + 4 <= bytes.byteLength) {
    const header = decoder.decode(bytes.subarray(offset, offset + 4));
    offset += 4;

    if (header === FLUSH) {
      return {
        status: "ok",
        lines,
        offset,
      };
    }

    if (header === DELIM || header === RESPONSE_END) {
      return {
        status: "invalid",
        message: "Unexpected protocol delimiter in receive-pack command section.",
      };
    }

    const length = Number.parseInt(header, 16);
    if (!Number.isFinite(length) || length < 4) {
      return {
        status: "invalid",
        message: "Malformed pkt-line header in receive-pack command section.",
      };
    }

    const payloadLength = length - 4;
    if (offset + payloadLength > bytes.byteLength) {
      return { status: "incomplete" };
    }

    const payload = bytes.subarray(offset, offset + payloadLength);
    offset += payloadLength;
    lines.push(decoder.decode(payload).replace(/\r?\n$/, ""));
  }

  return { status: "incomplete" };
}

export async function readPktSectionStream(body: ReadableStream<Uint8Array>): Promise<{
  lines: string[];
  bytesConsumed: number;
  packStream: ReadableStream<Uint8Array>;
}> {
  const reader = body.getReader();
  let buffered: Uint8Array<ArrayBufferLike> = new Uint8Array(0);

  while (true) {
    const parsed = parsePktSectionPrefix(buffered);
    if (parsed.status === "ok") {
      const prefixRemainder = buffered.subarray(parsed.offset);
      const packStream = new ReadableStream<Uint8Array>({
        start(controller) {
          if (prefixRemainder.byteLength > 0) {
            controller.enqueue(prefixRemainder);
          }
        },
        async pull(controller) {
          const next = await reader.read();
          if (next.done) {
            controller.close();
            return;
          }
          controller.enqueue(next.value);
        },
        async cancel(reason) {
          await reader.cancel(reason);
        },
      });

      return {
        lines: parsed.lines,
        bytesConsumed: parsed.offset,
        packStream,
      };
    }

    if (parsed.status === "invalid") {
      await reader.cancel(parsed.message);
      throw new Error(parsed.message);
    }

    const next = await reader.read();
    if (next.done) {
      await reader.cancel("receive-pack command section ended before flush");
      throw new Error("Receive-pack command section ended before the flush packet.");
    }

    buffered = appendBytes(buffered, cloneBytes(next.value));
    if (buffered.byteLength > MAX_COMMAND_SECTION_BYTES) {
      await reader.cancel("receive-pack command section exceeded the supported size");
      throw new Error("Receive-pack command section exceeded the supported size.");
    }
  }
}
