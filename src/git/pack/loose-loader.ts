/**
 * Factory for creating loose object loaders.
 * Used with createMemPackFs to enable thin pack resolution during legacy unpack.
 */

import type { TypedStorage, RepoStateSchema } from "@/do/index.ts";

import { objKey } from "@/do/index.ts";
import { r2LooseKey } from "@/keys.ts";

/**
 * Create a loose object loader that reads from DO storage and R2.
 * Used in unpack operations and connectivity checks.
 *
 * @param store - Durable Object storage
 * @param env - Worker environment with R2 bucket
 * @param prefix - DO prefix for R2 keys
 * @returns Loader function that returns compressed object bytes
 */
export function createLooseLoader(
  store: TypedStorage<RepoStateSchema>,
  env: Env,
  prefix: string
): (oid: string) => Promise<Uint8Array | undefined> {
  return async (oid: string): Promise<Uint8Array | undefined> => {
    // First try DO storage
    const z = (await store.get(objKey(oid))) as Uint8Array | ArrayBuffer | undefined;
    if (z) return z instanceof Uint8Array ? z : new Uint8Array(z);

    // Then try R2
    try {
      const o = await env.REPO_BUCKET.get(r2LooseKey(prefix, oid));
      if (o) return new Uint8Array(await o.arrayBuffer());
    } catch {}

    return undefined;
  };
}
