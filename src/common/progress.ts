import { getRepoStub } from "./stub.ts";

export interface UnpackProgress {
  unpacking: boolean;
  processed?: number;
  total?: number;
  percent?: number;
  queuedCount?: number;
  currentPackKey?: string;
}

export interface RepoActivity {
  state: "receiving" | "compacting";
  startedAt?: number;
  expiresAt?: number;
}

/**
 * Legacy compatibility helper used by the current DO receive path and legacy
 * queue tests while unpacking still exists behind the compatibility surface.
 */
export async function getUnpackProgress(env: Env, repoId: string): Promise<UnpackProgress | null> {
  try {
    const stub = getRepoStub(env, repoId);
    const progress = await stub.getUnpackProgress();
    if ((progress.unpacking && progress.total) || Number(progress.queuedCount || 0) > 0) {
      return progress;
    }
  } catch {}
  return null;
}

/**
 * Fetch repository activity for banner rendering.
 * Idle repos return null so callers can keep the existing "render nothing"
 * behavior without interpreting unpack or hydration state as correctness data.
 */
export async function getRepoActivity(env: Env, repoId: string): Promise<RepoActivity | null> {
  try {
    const stub = getRepoStub(env, repoId);
    return await stub.getRepoActivity();
  } catch {
    return null;
  }
}
