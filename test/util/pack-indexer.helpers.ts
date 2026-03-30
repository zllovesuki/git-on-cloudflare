import { SubrequestLimiter } from "@/git/operations/limits.ts";
import { createLogger } from "@/common/logger.ts";
import type { PackCatalogRow } from "@/git/object-store/types.ts";

export const packIndexerLog = createLogger("debug", { service: "PackIndexerTest" });

export function makeLimiter() {
  return new SubrequestLimiter(6);
}

export function makeCountSubrequest(counter: { count: number }) {
  return (n = 1) => {
    counter.count += n;
  };
}

export function makeTracingLimiter(labels: string[]) {
  return {
    async run<T>(label: string, fn: () => Promise<T>): Promise<T> {
      labels.push(label);
      return await fn();
    },
  };
}

export function makeActiveCatalogRow(args: {
  packKey: string;
  packBytes: number;
  objectCount: number;
  idxBytes: number;
}): PackCatalogRow {
  return {
    packKey: args.packKey,
    kind: "legacy",
    state: "active",
    tier: 0,
    seqLo: 1,
    seqHi: 1,
    objectCount: args.objectCount,
    packBytes: args.packBytes,
    idxBytes: args.idxBytes,
    createdAt: Date.now(),
    supersededBy: null,
  };
}

export async function rewritePackChecksum(
  packBytes: Uint8Array,
  mutate: (next: Uint8Array) => void
): Promise<Uint8Array> {
  const next = new Uint8Array(packBytes);
  mutate(next);
  const digest = new Uint8Array(
    await crypto.subtle.digest("SHA-1", next.subarray(0, next.length - 20))
  );
  next.set(digest, next.length - 20);
  return next;
}

export function setSingleBytePackHeaderSize(
  bytes: Uint8Array,
  offset: number,
  typeCode: number,
  size: number
): void {
  if (size < 0 || size > 0x0f) {
    throw new Error(`test helper requires a single-byte pack header size, got ${size}`);
  }
  bytes[offset] = (typeCode << 4) | size;
}
