// Shared stable environment variables for Vitest Workers runs
// These override Wrangler vars via poolOptions.workers.miniflare.bindings
// Miniflare values take precedence over wrangler.jsonc.

export const BASE_TEST_BINDINGS = {
  // Repo DO idle cleanup threshold (minutes)
  REPO_DO_IDLE_MINUTES: "30",

  // Logging level lowered for tests to reduce noise
  LOG_LEVEL: "warn",
} as const;
