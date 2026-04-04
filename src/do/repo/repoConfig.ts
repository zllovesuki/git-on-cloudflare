export function getConfig(env: Env) {
  const idleMins = Number(env.REPO_DO_IDLE_MINUTES ?? 30);
  const clamp = (n: number, min: number, max: number) =>
    Number.isFinite(n) ? Math.max(min, Math.min(max, Math.floor(n))) : min;
  return {
    idleMs: clamp(idleMins, 1, 60 * 24 * 7) * 60 * 1000,
  };
}
