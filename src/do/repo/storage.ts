/**
 * Storage operations for Git objects in Durable Object and R2
 *
 * This module handles the storage and retrieval of Git objects,
 * maintaining a two-tier storage strategy:
 * - DO storage: Fast, consistent, limited capacity
 * - R2 storage: Cheap, scalable, eventual consistency
 */

import type { RepoStateSchema } from "./repoState.ts";

import { asTypedStorage, objKey } from "./repoState.ts";
import { r2LooseKey } from "@/keys.ts";
import { isValidOid } from "@/common/index.ts";
import { inflateAndParseHeader, parseCommitText } from "@/git/index.ts";

/**
 * Check if a loose object exists
 * @param ctx - Durable Object state context
 * @param env - Worker environment
 * @param prefix - Repository prefix
 * @param oid - Object ID to check
 * @returns true if object exists in DO or R2 storage
 */
export async function hasLoose(
  ctx: DurableObjectState,
  env: Env,
  prefix: string,
  oid: string
): Promise<boolean> {
  if (!isValidOid(oid)) return false;

  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const data = await store.get(objKey(oid));
  if (data) return true;

  try {
    const head = await env.REPO_BUCKET.head(r2LooseKey(prefix, oid));
    return !!head;
  } catch {}

  return false;
}

/**
 * Store a loose object in DO storage and mirror to R2
 * @param ctx - Durable Object state context
 * @param env - Worker environment
 * @param prefix - Repository prefix
 * @param oid - Object ID
 * @param bytes - Compressed object data
 */
export async function storeObject(
  ctx: DurableObjectState,
  env: Env,
  prefix: string,
  oid: string,
  bytes: Uint8Array
): Promise<void> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);

  // Store in DO storage
  await store.put(objKey(oid), bytes);

  // Best-effort mirror to R2
  await mirrorObjectToR2(env, prefix, oid, bytes);
}

/**
 * Mirror an object to R2 storage (best-effort)
 * @param env - Worker environment
 * @param prefix - Repository prefix
 * @param oid - Object ID
 * @param bytes - Compressed object data
 */
async function mirrorObjectToR2(
  env: Env,
  prefix: string,
  oid: string,
  bytes: Uint8Array
): Promise<void> {
  const r2key = r2LooseKey(prefix, oid);
  try {
    // Mirrors the compressed loose object to R2 for low-latency reads.
    // We do not fail the write if mirroring fails; DO storage remains the
    // source of truth and R2 will be filled by subsequent writes.
    await env.REPO_BUCKET.put(r2key, bytes);
  } catch {}
}

/**
 * Batch API: Get multiple objects at once to reduce subrequest count
 * Returns a Map of OID to compressed data (with Git header)
 * @param ctx - Durable Object state context
 * @param oids - Array of object IDs to fetch
 * @returns Map of OID to data, with null for missing objects
 */
export async function getObjectsBatch(
  ctx: DurableObjectState,
  oids: string[]
): Promise<Map<string, Uint8Array | null>> {
  const result = new Map<string, Uint8Array | null>();

  // Batch size to avoid memory issues while maximizing throughput
  const BATCH_SIZE = 256;

  for (let i = 0; i < oids.length; i += BATCH_SIZE) {
    const batch = oids.slice(i, i + BATCH_SIZE);
    // Use Durable Object storage.get(array) to fetch many keys at once
    const keys = batch.map((oid) => objKey(oid) as unknown as string);
    const fetched = (await ctx.storage.get(keys)) as Map<
      string,
      Uint8Array | ArrayBuffer | undefined
    >;
    for (const oid of batch) {
      const key = objKey(oid) as unknown as string;
      const data = fetched.get(key);
      if (data) {
        const uint8Data = data instanceof Uint8Array ? data : new Uint8Array(data);
        result.set(oid, uint8Data);
      } else {
        // Do NOT fetch from R2 here to avoid subrequest bursts inside the DO invocation.
        // Return null for DO-missing objects; the Worker will perform pack-based retrieval
        // with better reuse and per-request memoization.
        result.set(oid, null);
      }
    }
  }
  return result;
}

/**
 * Read and parse a commit object directly from DO storage (fallback to R2 if needed)
 * @param ctx - Durable Object state context
 * @param env - Worker environment
 * @param prefix - Repository prefix
 * @param oid - Commit object ID
 * @returns Parsed commit information or null if not found
 */
export async function readCommitFromStore(
  ctx: DurableObjectState,
  env: Env,
  prefix: string,
  oid: string
): Promise<{
  oid: string;
  tree: string;
  parents: string[];
  author?: { name: string; email: string; when: number; tz: string };
  committer?: { name: string; email: string; when: number; tz: string };
  message: string;
} | null> {
  // Prefer DO-stored loose object to avoid R2/HTTP hops
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  let data = await store.get(objKey(oid));

  if (!data) {
    // Fallback: try R2-stored loose copy
    try {
      const obj = await env.REPO_BUCKET.get(r2LooseKey(prefix, oid));
      if (obj) data = await obj.arrayBuffer();
    } catch {}
  }
  if (!data) return null;

  const z = data instanceof ArrayBuffer ? new Uint8Array(data) : data;
  // Decompress (zlib/deflate) and parse git header
  const parsed = await inflateAndParseHeader(z);
  if (!parsed || parsed.type !== "commit") return null;
  const text = new TextDecoder().decode(parsed.payload);

  const commit = parseCommitText(text);
  return { oid, ...commit };
}
