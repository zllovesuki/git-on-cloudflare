// @ts-nocheck
import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import path from "path";
import { BASE_TEST_BINDINGS } from "./test/vitest.bindings.ts";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [
    cloudflareTest({
      main: "./src/index.ts",
      wrangler: {
        configPath: "./wrangler.jsonc",
      },
      isolatedStorage: false,
      singleWorker: true,
      miniflare: {
        durableObjectsPersist: false,
        kvPersist: false,
        r2Persist: false,
        cachePersist: false,
        compatibilityDate: "2025-09-02",
        // Enable centralized auth in this auth test suite
        bindings: { ...BASE_TEST_BINDINGS, AUTH_ADMIN_TOKEN: "admin" },
      },
    }),
  ],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  server: { deps: { inline: ["isomorphic-git", "@noble/hashes"] } },
  test: {
    include: ["test/auth.worker.test.ts"],
  },
});
