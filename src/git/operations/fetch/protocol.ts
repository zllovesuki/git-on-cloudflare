import { asBodyInit, clientAbortedResponse } from "@/common/index.ts";
import { pktLine, delimPkt, flushPkt, concatChunks } from "@/git/core/index.ts";

/**
 * Builds acknowledgment section for git protocol v2.
 */
export function buildAckSection(ackOids: string[], done: boolean): Uint8Array[] {
  const chunks: Uint8Array[] = [];

  if (!done) {
    chunks.push(pktLine("acknowledgments\n"));
    if (ackOids && ackOids.length > 0) {
      for (let i = 0; i < ackOids.length; i++) {
        const oid = ackOids[i];
        const suffix = i === ackOids.length - 1 ? "ready" : "common";
        chunks.push(pktLine(`ACK ${oid} ${suffix}\n`));
      }
    } else {
      chunks.push(pktLine("NAK\n"));
    }
    chunks.push(delimPkt());
  }
  chunks.push(pktLine("packfile\n"));

  return chunks;
}

/**
 * Builds an ACK/NAK-only response when no packfile is needed.
 */
export function buildAckOnlyResponse(ackOids: string[]): Response {
  const chunks: Uint8Array[] = [pktLine("acknowledgments\n")];

  if (ackOids && ackOids.length > 0) {
    for (let i = 0; i < ackOids.length; i++) {
      const oid = ackOids[i];
      const suffix = i === ackOids.length - 1 ? "ready" : "common";
      chunks.push(pktLine(`ACK ${oid} ${suffix}\n`));
    }
  } else {
    chunks.push(pktLine("NAK\n"));
  }

  chunks.push(flushPkt());

  return new Response(asBodyInit(concatChunks(chunks)), {
    status: 200,
    headers: {
      "Content-Type": "application/x-git-upload-pack-result",
      "Cache-Control": "no-cache",
    },
  });
}

/**
 * Constructs a Git protocol v2 response with packetized packfile data.
 * Useful for the buffered approach.
 */
export function respondWithPacketizedPack(
  packfile: Uint8Array,
  done: boolean,
  ackOids: string[],
  signal?: AbortSignal
): Response {
  if (signal?.aborted) return clientAbortedResponse();

  const chunks: Uint8Array[] = [];

  if (!done) {
    chunks.push(pktLine("acknowledgments\n"));
    if (ackOids && ackOids.length > 0) {
      for (let i = 0; i < ackOids.length; i++) {
        const oid = ackOids[i];
        const suffix = i === ackOids.length - 1 ? "ready" : "common";
        chunks.push(pktLine(`ACK ${oid} ${suffix}\n`));
      }
    } else {
      chunks.push(pktLine("NAK\n"));
    }
    chunks.push(delimPkt());
  }
  chunks.push(pktLine("packfile\n"));

  const maxChunk = 65515;
  for (let off = 0; off < packfile.byteLength; off += maxChunk) {
    if (signal?.aborted) return clientAbortedResponse();
    const slice = packfile.subarray(off, Math.min(off + maxChunk, packfile.byteLength));
    const banded = new Uint8Array(1 + slice.byteLength);
    banded[0] = 0x01;
    banded.set(slice, 1);
    chunks.push(pktLine(banded));
  }
  chunks.push(flushPkt());

  return new Response(asBodyInit(concatChunks(chunks)), {
    status: 200,
    headers: {
      "Content-Type": "application/x-git-upload-pack-result",
      "Cache-Control": "no-cache",
    },
  });
}
