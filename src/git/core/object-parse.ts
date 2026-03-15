/**
 * Shared Git object parsing helpers
 */

import { parseGitObject, type GitObjectType } from "./objects.ts";
import { parseCommitText } from "./commitParse.ts";
import { createInflateStream, bytesToHex, createBlobFromBytes } from "@/common/index.ts";

const td = new TextDecoder();

/**
 * Inflate a zlib-compressed Git object (with header) and return its type and payload.
 * Returns null on error.
 */
export async function inflateAndParseHeader(
  zdata: Uint8Array
): Promise<{ type: GitObjectType; payload: Uint8Array } | null> {
  try {
    const stream = createBlobFromBytes(zdata).stream().pipeThrough(createInflateStream());
    const raw = new Uint8Array(await new Response(stream).arrayBuffer());
    const { type, payload } = parseGitObject(raw);
    return { type, payload };
  } catch {
    return null;
  }
}

/**
 * Parse commit payload to extract referenced OIDs: tree and parents.
 */
export function parseCommitRefs(payload: Uint8Array): { tree?: string; parents: string[] } {
  const text = td.decode(payload);
  const { tree, parents } = parseCommitText(text);
  return { tree, parents };
}

/**
 * Parse a tree payload and return child object OIDs.
 * Git tree format: "<mode> <name>\0<20-byte-oid>" repeated.
 */
export function parseTreeChildOids(payload: Uint8Array): string[] {
  const out: string[] = [];
  let i = 0;
  while (i < payload.length) {
    let sp = i;
    while (sp < payload.length && payload[sp] !== 0x20) sp++;
    if (sp >= payload.length) break;
    let nul = sp + 1;
    while (nul < payload.length && payload[nul] !== 0x00) nul++;
    if (nul + 20 > payload.length) break;
    const oidBytes = payload.subarray(nul + 1, nul + 21);
    out.push(bytesToHex(oidBytes));
    i = nul + 21;
  }
  return out;
}

/**
 * Parse annotated tag payload to extract its target OID and type.
 */
export function parseTagTarget(
  payload: Uint8Array
): { targetOid: string; targetType: GitObjectType } | null {
  const text = td.decode(payload);
  const mObj = text.match(/^object\s+([0-9a-f]{40})/m);
  const mType = text.match(/^type\s+(commit|tree|blob|tag)/m);
  if (!mObj || !mType) return null;
  return { targetOid: mObj[1].toLowerCase(), targetType: mType[1] as GitObjectType };
}
