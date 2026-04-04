import { getRepoStub } from "./stub.ts";

export interface RepoActivity {
  state: "receiving" | "compacting";
  startedAt?: number;
  expiresAt?: number;
}

/**
 * Fetch repository activity for banner rendering.
 * Idle repos return null so callers can keep the existing "render nothing"
 * behavior without interpreting state as correctness data.
 */
export async function getRepoActivity(env: Env, repoId: string): Promise<RepoActivity | null> {
  try {
    const stub = getRepoStub(env, repoId);
    return await stub.getRepoActivity();
  } catch {
    return null;
  }
}
