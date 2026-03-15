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
import { isValidOid, createInflateStream, createBlobFromBytes } from "@/common/index.ts";
import {
  inflateAndParseHeader,
  parseCommitRefs,
  parseTreeChildOids,
  parseCommitText,
} from "@/git/index.ts";
import { getDb, getPackOidsBatch } from "./db/index.ts";

/**
 * Get a single object stream by OID
 * Tries R2 first for cost efficiency, falls back to DO storage
 * @param ctx - Durable Object state context
 * @param env - Worker environment
 * @param prefix - Repository prefix (do/<id>)
 * @param oid - Object ID to retrieve
 * @returns ReadableStream of compressed object data or null if not found
 */
export async function getObjectStream(
  ctx: DurableObjectState,
  env: Env,
  prefix: string,
  oid: string
): Promise<ReadableStream | null> {
  if (!isValidOid(oid)) return null;

  // Try R2 first
  try {
    const obj = await env.REPO_BUCKET.get(r2LooseKey(prefix, oid));
    if (obj) return obj.body;
  } catch {}

  // Fallback to DO storage
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const data = await store.get(objKey(oid));
  if (!data) return null;

  // IMPORTANT: This stream contains the Git object in its zlib-compressed form
  // including the Git header. Callers that want the raw payload should pipe
  // through `createInflateStream()` and strip the header ("<type> <len>\0").
  return new ReadableStream({
    start(controller) {
      controller.enqueue(data);
      controller.close();
    },
  });
}

/**
 * Get a single object as ArrayBuffer by OID
 * @param ctx - Durable Object state context
 * @param env - Worker environment
 * @param prefix - Repository prefix
 * @param oid - Object ID to retrieve
 * @returns Compressed object data or null if not found
 */
export async function getObject(
  ctx: DurableObjectState,
  env: Env,
  prefix: string,
  oid: string
): Promise<ArrayBuffer | Uint8Array | null> {
  if (!isValidOid(oid)) return null;

  // Try R2 first
  try {
    const obj = await env.REPO_BUCKET.get(r2LooseKey(prefix, oid));
    if (obj) return new Uint8Array(await obj.arrayBuffer());
  } catch {}

  // Fallback to DO storage
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const data = await store.get(objKey(oid));
  return data || null;
}

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
 * Batch membership check for loose objects
 * Returns an array of booleans aligned with input OIDs
 * Uses small concurrency for R2 HEADs; DO storage checks are performed directly
 * @param ctx - Durable Object state context
 * @param env - Worker environment
 * @param prefix - Repository prefix
 * @param oids - Array of object IDs to check
 * @param logger - Logger instance
 * @returns Array of booleans indicating existence
 */
export async function hasLooseBatch(
  ctx: DurableObjectState,
  env: Env,
  prefix: string,
  oids: string[],
  logger?: { debug: (msg: string, data?: any) => void; warn: (msg: string, data?: any) => void }
): Promise<boolean[]> {
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const db = getDb(ctx.storage);

  // Short-circuit using recent pack membership to avoid R2 HEADs
  // Build a small set of OIDs from the newest packs we know about.
  const packSet = new Set<string>();
  try {
    const last = ((await store.get("lastPackOids")) || []) as string[];
    for (const x of last) packSet.add(x.toLowerCase());

    // Include a couple more recent packs if available, loading from SQLite
    const list = (((await store.get("packList")) || []) as string[]).slice(0, 2);
    if (list.length > 0) {
      try {
        const map = await getPackOidsBatch(db, list);
        for (const arr of map.values()) {
          for (const x of arr) packSet.add(String(x).toLowerCase());
        }
      } catch (e) {
        logger?.debug?.("hasLooseBatch:packOidsBatch-failed", { error: String(e) });
      }
    }
  } catch {}

  const checkOne = async (oid: string): Promise<boolean> => {
    if (!isValidOid(oid)) return false;

    // 1) Fast-path: known to be present in a recent pack
    if (packSet.size > 0 && packSet.has(oid.toLowerCase())) return true;

    // 2) DO state (loose) lookup
    const data = await store.get(objKey(oid));
    if (data) return true;

    try {
      // 3) R2 loose HEAD fallback
      const head = await env.REPO_BUCKET.head(r2LooseKey(prefix, oid));
      return !!head;
    } catch {
      return false;
    }
  };

  const MAX = 16;
  const out: boolean[] = [];
  for (let i = 0; i < oids.length; i += MAX) {
    const part = oids.slice(i, i + MAX);
    const res = await Promise.all(part.map((oid) => checkOne(oid)));
    out.push(...res);
  }
  return out;
}

/**
 * Get object size without loading the full object
 * @param ctx - Durable Object state context
 * @param env - Worker environment
 * @param prefix - Repository prefix
 * @param oid - Object ID
 * @returns Size in bytes or null if not found
 */
export async function getObjectSize(
  ctx: DurableObjectState,
  env: Env,
  prefix: string,
  oid: string
): Promise<number | null> {
  const r2key = r2LooseKey(prefix, oid);

  // Try R2 first
  try {
    const obj = await env.REPO_BUCKET.head(r2key);
    if (obj) return obj.size;
  } catch {}

  // Fallback to DO storage
  const store = asTypedStorage<RepoStateSchema>(ctx.storage);
  const data = await store.get(objKey(oid));

  if (!data) return null;
  return data.byteLength;
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
 * Batch API: Extract object references (for commits and trees) without full parsing
 * @param ctx - Durable Object state context
 * @param oids - Array of object IDs to get references for
 * @param logger - Logger instance
 * @returns Map of OID to array of referenced OIDs
 */
export async function getObjectRefsBatch(
  ctx: DurableObjectState,
  oids: string[],
  logger?: { debug: (msg: string, data?: any) => void }
): Promise<Map<string, string[]>> {
  const result = new Map<string, string[]>();

  const objects = await getObjectsBatch(ctx, oids);

  for (const [oid, data] of objects) {
    if (!data) {
      result.set(oid, []);
      continue;
    }

    try {
      const parsed = await inflateAndParseHeader(data);
      if (!parsed) {
        result.set(oid, []);
        continue;
      }
      const { type, payload } = parsed;
      const refs: string[] = [];
      if (type === "commit") {
        const { tree, parents } = parseCommitRefs(payload);
        if (tree) refs.push(tree);
        for (const p of parents) refs.push(p);
      } else if (type === "tree") {
        for (const child of parseTreeChildOids(payload)) refs.push(child);
      }
      result.set(oid, refs);
    } catch (e) {
      logger?.debug("getObjectRefsBatch:parse-error", { oid, error: String(e) });
      result.set(oid, []);
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
  const ds = createInflateStream();
  const stream = createBlobFromBytes(z).stream().pipeThrough(ds);
  const raw = new Uint8Array(await new Response(stream).arrayBuffer());
  // header: <type> <len>\0
  let p = 0;
  let sp = p;
  while (sp < raw.length && raw[sp] !== 0x20) sp++;
  const type = new TextDecoder().decode(raw.subarray(p, sp));
  if (type !== "commit") return null;
  let nul = sp + 1;
  while (nul < raw.length && raw[nul] !== 0x00) nul++;
  const payload = raw.subarray(nul + 1);
  const text = new TextDecoder().decode(payload);

  const parsed = parseCommitText(text);
  return { oid, ...parsed };
}
