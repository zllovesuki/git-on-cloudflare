import type { PktItem } from "@/git/core/index.ts";

import { delimPkt, flushPkt, pktLine } from "@/git";
import { decodePktLines } from "@/git/core/index.ts";

export function buildFetchBody(args: {
  wants: string[];
  haves?: string[];
  done?: boolean;
  agent?: string | false;
}): Uint8Array {
  const chunks: Uint8Array[] = [];
  chunks.push(pktLine("command=fetch\n"));
  if (args.agent !== false) {
    chunks.push(pktLine(`agent=${args.agent || "test"}\n`));
  }
  chunks.push(delimPkt());
  for (const want of args.wants) chunks.push(pktLine(`want ${want}\n`));
  for (const have of args.haves || []) chunks.push(pktLine(`have ${have}\n`));
  if (args.done) chunks.push(pktLine("done\n"));
  chunks.push(flushPkt());
  const total = chunks.reduce((size, chunk) => size + chunk.byteLength, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return out;
}

function isPktLine(item: PktItem): item is Extract<PktItem, { type: "line" }> {
  return item.type === "line";
}

export function decodePktTextLines(bytes: Uint8Array): string[] {
  return decodePktLines(bytes)
    .filter(isPktLine)
    .map((item) => item.text.trim());
}

export function findBytes(haystack: Uint8Array, needle: Uint8Array): number {
  outer: for (let i = 0; i <= haystack.length - needle.length; i++) {
    for (let j = 0; j < needle.length; j++) {
      if (haystack[i + j] !== needle[j]) continue outer;
    }
    return i;
  }
  return -1;
}
