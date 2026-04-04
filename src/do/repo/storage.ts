/**
 * Storage operations for Git objects in Durable Object and R2
 *
 * Post-closure, loose object writes are no longer part of the production path.
 * This module retains only readCommitFromStore for the debug endpoint fallback.
 */

import type { RepoStateSchema } from "./repoState.ts";

import { asTypedStorage, objKey } from "./repoState.ts";
import { r2LooseKey } from "@/keys.ts";
import { inflateAndParseHeader, parseCommitText } from "@/git/index.ts";

/**
 * Read and parse a commit object directly from DO storage (fallback to R2 if needed).
 * Legacy: used only by the debug endpoint fallback when a packed read returns a non-commit.
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
