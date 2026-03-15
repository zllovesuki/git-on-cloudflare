/**
 * Small helpers around the Cloudflare Workers Cache API for JSON payloads.
 *
 * Notes
 * - Cache lives per colo; keys should be stable and include all inputs.
 * - Always use the same origin as the incoming request when constructing keys.
 * - Only cache GET responses.
 */

import { asBodyInit } from "@/common/webtypes.ts";

const CACHE_NAME_JSON = "git-on-cf:json";
const CACHE_NAME_OBJECTS = "git-on-cf:objects";

/**
 * Optional per-request memoization store to avoid repeated upstream calls
 * (DO RPCs, R2) within a single Worker request.
 */
export interface RequestMemo {
  /** Pin the repository for this request memo to prevent cross-repo contamination */
  repoId?: string;
  /** Object results by OID (git header removed) */
  objects?: Map<string, { type: string; payload: Uint8Array } | undefined>;
  /** Parsed references for objects: commit -> [tree, parents], tree -> [entries] */
  refs?: Map<string, string[]>;
  /** Candidate pack list for the current repo (once per request) */
  packList?: string[];
  /** In-flight promise for candidate pack list to coalesce concurrent discovery */
  packListPromise?: Promise<string[]>;
  /** Pack OIDs by pack key */
  packOids?: Map<string, Set<string>>;
  /** In-memory virtual FS for pack files to reuse across OIDs (current repo only) */
  packFiles?: Map<string, Uint8Array>;
  /** Small flags set for once-per-request log throttling and guards */
  flags?: Set<string>;
  /** Remaining DO batch budget for getObjectRefsBatch (shared across both closures) */
  doBatchBudget?: number;
  /** If true, disable further DO refs batches due to errors or budgets */
  doBatchDisabled?: boolean;
  /** Count of DO-backed loose loader calls (stub.getObject) within this request */
  loaderCalls?: number;
  /** Soft cap for DO-backed loose loader calls; can be adjusted between phases (closure vs fallback) */
  loaderCap?: number;
  /** Optional per-request soft subrequest budget to degrade before hitting platform hard limits */
  subreqBudget?: number;
  /** Optional concurrency limiter for upstream calls; must provide a run(label, fn) API */
  limiter?: { run<T>(label: string, fn: () => Promise<T>): Promise<T> };
}

/**
 * Context for cacheable operations.
 * Combines request and execution context for caching and background tasks.
 * When provided, both fields are required since they typically come together.
 */
export interface CacheContext {
  req: Request;
  ctx: ExecutionContext;
  /** Optional per-request memoization */
  memo?: RequestMemo;
}

/**
 * Resolve the zone cache instance used for JSON payloads.
 *
 * We intentionally use a named cache via `caches.open(...)` instead of
 * `caches.default` so that TypeScript does not need Cloudflare-specific
 * ambient declarations for `caches.default`.
 */
async function getZoneCache(): Promise<Cache> {
  // Use a named cache to avoid relying on caches.default typings
  return await caches.open(CACHE_NAME_JSON);
}

/**
 * Resolve the zone cache instance used for git objects.
 * Git objects are immutable, so we use a separate cache with longer TTLs.
 */
async function getObjectCache(): Promise<Cache> {
  return await caches.open(CACHE_NAME_OBJECTS);
}

/**
 * Build a same-origin GET Request to use as the cache key.
 *
 * Why same-origin?
 * - Cloudflare recommends keeping the hostname aligned with the Worker hostname
 *   to avoid unnecessary DNS lookups and improve cache efficiency.
 *
 * Key design
 * - Use a dedicated pathname (for example, "/_cache/commits") and include only
 *   the parameters that affect the response in `params`.
 * - Omit empty/undefined params to keep keys clean and deterministic.
 */
export function buildCacheKeyFrom(
  req: Request,
  pathname: string,
  params: Record<string, string | undefined>
): Request {
  const u = new URL(req.url);
  u.pathname = pathname;
  // Reset and set only the parameters we care about
  u.search = "";
  const sp = u.searchParams;
  for (const [k, v] of Object.entries(params)) {
    if (v && v !== "") sp.set(k, v);
  }
  return new Request(u.toString(), { method: "GET" });
}

/**
 * Retrieve JSON from the Workers cache by key request.
 *
 * @param keyReq - The synthetic GET Request produced by `buildCacheKeyFrom()`.
 * @returns Parsed JSON on hit, or null on miss/error.
 */
export async function cacheGetJSON<T = unknown>(keyReq: Request): Promise<T | null> {
  try {
    const cache = await getZoneCache();
    const res = await cache.match(keyReq);
    if (!res || !res.ok) return null;
    const data = (await res.json()) as T;
    return data;
  } catch {
    return null;
  }
}

/**
 * Store JSON into the Workers cache under `keyReq` with a TTL.
 *
 * Implementation
 * - Serializes `payload` to JSON and sets `Cache-Control: public, max-age=...`.
 * - Caller is responsible for picking an appropriate TTL.
 *
 * @param keyReq - Cache key request built via `buildCacheKeyFrom()`
 * @param payload - Any JSON-serializable value
 * @param ttlSeconds - Time to live in seconds
 */
export async function cachePutJSON(
  keyReq: Request,
  payload: unknown,
  ttlSeconds: number
): Promise<void> {
  try {
    const body = JSON.stringify(payload);
    const headers = new Headers();
    headers.set("Content-Type", "application/json; charset=utf-8");
    headers.set("Cache-Control", `public, max-age=${Math.max(0, Math.floor(ttlSeconds))}`);
    const res = new Response(body, { status: 200, headers });
    const cache = await getZoneCache();
    await cache.put(keyReq, res);
  } catch {
    // best-effort only
  }
}

/**
 * Build a cache key for a git object.
 * Git objects are content-addressable and immutable, so we can use long TTLs.
 *
 * @param req - The incoming request (for origin)
 * @param repoId - Repository identifier (owner/repo)
 * @param oid - Object ID (SHA-1 hash)
 * @returns Cache key request
 */
export function buildObjectCacheKey(req: Request, repoId: string, oid: string): Request {
  const u = new URL(req.url);
  u.pathname = `/_cache/obj/${repoId}/${oid.toLowerCase()}`;
  u.search = "";
  return new Request(u.toString(), { method: "GET" });
}

/**
 * Retrieve a git object from cache.
 *
 * @param keyReq - The cache key request
 * @returns Object data with type and payload, or null on miss
 */
export async function cacheGetObject(
  keyReq: Request
): Promise<{ type: string; payload: Uint8Array } | null> {
  try {
    const cache = await getObjectCache();
    const res = await cache.match(keyReq);
    if (!res || !res.ok) return null;

    // Objects are stored as binary with type in header
    const type = res.headers.get("X-Git-Type") || "blob";
    const payload = new Uint8Array(await res.arrayBuffer());
    return { type, payload };
  } catch {
    return null;
  }
}

/**
 * Store a git object in cache with immutable headers.
 * Since git objects are content-addressed, they never change.
 *
 * @param keyReq - Cache key request
 * @param type - Git object type (blob, tree, commit, tag)
 * @param payload - Raw object payload (without git header)
 */
export async function cachePutObject(
  keyReq: Request,
  type: string,
  payload: Uint8Array
): Promise<void> {
  try {
    const headers = new Headers();
    headers.set("Content-Type", "application/octet-stream");
    headers.set("X-Git-Type", type);
    // Git objects are immutable - cache for 1 year
    headers.set("Cache-Control", "public, max-age=31536000, immutable");

    const res = new Response(asBodyInit(payload), { status: 200, headers });
    const cache = await getObjectCache();
    await cache.put(keyReq, res);
  } catch {
    // best-effort only
  }
}

/**
 * Helper to handle the check-load-save cache pattern with ctx.waitUntil.
 * Checks cache first, loads from source if needed, and saves to cache in background.
 *
 * @param cacheKey - The cache key request
 * @param loader - Function to load the data if not cached
 * @param ctx - ExecutionContext for waitUntil (optional)
 * @returns The cached or loaded git object
 */
export async function cacheOrLoadObject<T extends { type: string; payload: Uint8Array }>(
  cacheKey: Request,
  loader: () => Promise<T | undefined>,
  ctx?: ExecutionContext
): Promise<T | undefined> {
  // Try cache first
  const cached = await cacheGetObject(cacheKey);
  if (cached) {
    return cached as T;
  }

  // Load from source
  const result = await loader();
  if (!result) return undefined;

  // Save to cache in background if ctx is provided
  const savePromise = cachePutObject(cacheKey, result.type, result.payload);
  if (ctx) {
    ctx.waitUntil(savePromise);
  } else {
    // If no ctx, we still save but don't wait
    savePromise.catch(() => {}); // Ignore errors
  }

  return result;
}

/**
 * Helper for JSON cache with the check-load-save pattern.
 *
 * @param cacheKey - The cache key request
 * @param loader - Function to load the data if not cached
 * @param ttl - Time to live in seconds
 * @param ctx - ExecutionContext for waitUntil (optional)
 * @returns The cached or loaded data
 */
export async function cacheOrLoadJSON<T>(
  cacheKey: Request,
  loader: () => Promise<T | null>,
  ttl: number,
  ctx?: ExecutionContext
): Promise<T | null> {
  // Try cache first
  const cached = await cacheGetJSON<T>(cacheKey);
  if (cached) {
    return cached;
  }

  // Load from source
  const result = await loader();
  if (!result) return null;

  // Save to cache in background if ctx is provided
  const savePromise = cachePutJSON(cacheKey, result, ttl);
  if (ctx) {
    ctx.waitUntil(savePromise);
  } else {
    // If no ctx, we still save but don't wait
    savePromise.catch(() => {}); // Ignore errors
  }

  return result;
}

/**
 * Variant of cacheOrLoadJSON where the TTL depends on the loaded value.
 * Useful when the response type determines TTL (e.g., tree listings vs blob metadata).
 */
export async function cacheOrLoadJSONWithTTL<T>(
  cacheKey: Request,
  loader: () => Promise<T | null>,
  ttlResolver: (value: T) => number,
  ctx?: ExecutionContext
): Promise<T | null> {
  // Try cache first
  const cached = await cacheGetJSON<T>(cacheKey);
  if (cached) return cached;

  // Load from source
  const result = await loader();
  if (!result) return null;

  // Resolve TTL and save in background
  const ttl = Math.max(0, Math.floor(ttlResolver(result)));
  const savePromise = cachePutJSON(cacheKey, result, ttl);
  if (ctx) ctx.waitUntil(savePromise);
  else savePromise.catch(() => {});
  return result;
}
