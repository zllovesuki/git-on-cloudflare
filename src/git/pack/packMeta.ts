import { bytesToHex } from "@/common/index.ts";

export type PackReadOptions = {
  limiter?: { run<T>(label: string, fn: () => Promise<T>): Promise<T> };
  countSubrequest?: (n?: number) => boolean | void;
  signal?: AbortSignal;
};

export type PackHeaderEx = {
  type: number;
  sizeVarBytes: Uint8Array;
  headerLen: number;
  baseOid?: string;
  baseRel?: number;
};

/**
 * Read a byte range from an R2 `.pack` object.
 */
export async function readPackRange(
  env: Env,
  key: string,
  offset: number,
  length: number,
  options?: PackReadOptions
): Promise<Uint8Array | undefined> {
  if (options?.signal?.aborted) return undefined;

  const run = async () => {
    const obj = await env.REPO_BUCKET.get(key, { range: { offset, length } });
    if (!obj) return undefined;
    return new Uint8Array(await obj.arrayBuffer());
  };

  if (options?.limiter) {
    options.countSubrequest?.();
    return await options.limiter.run("r2:get-range", run);
  }
  return await run();
}

/**
 * Read and parse a pack entry header at the given offset.
 */
export async function readPackHeaderEx(
  env: Env,
  key: string,
  offset: number,
  options?: PackReadOptions
): Promise<PackHeaderEx | undefined> {
  const head = await readPackRange(env, key, offset, 128, options);
  if (!head) return undefined;
  return readPackHeaderExFromBuf(head, 0);
}

/**
 * Parse a pack entry header from an in-memory buffer.
 */
export function readPackHeaderExFromBuf(buf: Uint8Array, offset: number): PackHeaderEx | undefined {
  let p = offset;
  if (p >= buf.length) return undefined;

  const start = p;
  let c = buf[p++];
  const type = (c >> 4) & 0x07;
  while (c & 0x80) {
    if (p >= buf.length) return undefined;
    c = buf[p++];
  }

  const sizeVarBytes = buf.subarray(start, p);
  if (type === 7) {
    if (p + 20 > buf.length) return undefined;
    return {
      type,
      sizeVarBytes,
      headerLen: sizeVarBytes.length + 20,
      baseOid: bytesToHex(buf.subarray(p, p + 20)),
    };
  }

  if (type === 6) {
    const ofsStart = p;
    if (p >= buf.length) return undefined;

    let distance = 0;
    let byte = buf[p++];
    distance = byte & 0x7f;
    while (byte & 0x80) {
      if (p >= buf.length) return undefined;
      byte = buf[p++];
      distance = ((distance + 1) << 7) | (byte & 0x7f);
    }

    return {
      type,
      sizeVarBytes,
      headerLen: sizeVarBytes.length + (p - ofsStart),
      baseRel: distance,
    };
  }

  return { type, sizeVarBytes, headerLen: sizeVarBytes.length };
}

/**
 * Returns the encoded byte length of an OFS_DELTA distance without allocating.
 * Use this in convergence loops where only the length matters.
 */
export function ofsDeltaDistanceLength(rel: number): number {
  if (rel <= 0) return 1;
  let current = rel >>> 0;
  let count = 0;
  while (true) {
    count++;
    const group = current & 0x7f;
    current = ((current - group) >>> 7) - 1;
    if (current < 0) break;
  }
  return count;
}

/**
 * Encodes OFS_DELTA distance using Git's varint-with-add-one scheme.
 */
export function encodeOfsDeltaDistance(rel: number): Uint8Array {
  if (rel <= 0) return new Uint8Array([0]);

  let current = rel >>> 0;
  const groups: number[] = [];
  while (true) {
    const group = current & 0x7f;
    groups.push(group);
    current = ((current - group) >>> 7) - 1;
    if (current < 0) break;
  }

  groups.reverse();
  for (let index = 0; index < groups.length - 1; index++) {
    groups[index] |= 0x80;
  }
  return new Uint8Array(groups);
}
