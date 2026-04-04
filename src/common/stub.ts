import type { RepoDurableObject, AuthDurableObject } from "@/do/index.ts";
/**
 * Get the Durable Object stub for a repository.
 * @param env - Cloudflare environment bindings
 * @param repoId - Repository identifier (owner/repo)
 * @returns DurableObjectStub for the specified repository
 */
export function getRepoStub(env: Env, repoId: string): DurableObjectStub<RepoDurableObject> {
  const id = env.REPO_DO.idFromName(repoId);
  return env.REPO_DO.get(id);
}

export function getRepoStubByDoId(env: Env, doId: string): DurableObjectStub<RepoDurableObject> {
  const id = env.REPO_DO.idFromString(doId);
  return env.REPO_DO.get(id);
}

export function getAuthStub(env: Env): DurableObjectStub<AuthDurableObject> | null {
  // Enforce centralized auth only when both AUTH_DO and AUTH_ADMIN_TOKEN are set
  const ns = env.AUTH_DO;
  const admin = env.AUTH_ADMIN_TOKEN || "";
  if (!ns || !admin) return null;
  try {
    const id = ns.idFromName("auth");
    return ns.get(id);
  } catch {
    return null;
  }
}
