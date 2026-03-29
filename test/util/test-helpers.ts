/**
 * Shared test utilities for Git operations.
 */

type StringEnvKey = {
  [K in keyof Env]: Env[K] extends string | undefined ? K : never;
}[keyof Env];

/**
 * Temporarily override selected env bindings for the duration of fn(), restoring afterwards.
 */
export async function withEnvOverrides<T, K extends StringEnvKey>(
  env: Env,
  overrides: Pick<Env, K>,
  fn: () => Promise<T>
): Promise<T> {
  const keys = Object.keys(overrides) as K[];
  const prev = new Map<K, Env[K]>();
  for (const key of keys) {
    prev.set(key, env[key]);
    env[key] = overrides[key];
  }
  try {
    return await fn();
  } finally {
    for (const key of keys) {
      const value = prev.get(key);
      if (value !== undefined) {
        env[key] = value;
      } else {
        delete env[key];
      }
    }
  }
}

export * from "./do-retry.ts";
export * from "./git-pack.ts";
export * from "./packed-repo.ts";

/**
 * Generate a per-test unique repo id suffix to avoid shared storage collisions
 * when isolatedStorage is disabled.
 */
export function uniqueRepoId(prefix = "r"): string {
  return `${prefix}-${Math.random().toString(36).slice(2, 10)}`;
}
