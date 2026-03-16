// @ts-nocheck
import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import path from "path";
import { BASE_TEST_BINDINGS } from "./test/vitest.bindings.ts";
import { defineConfig } from "vitest/config";

const AUTH_TEST_FILE = "test/auth.worker.test.ts";
const OPTIMIZED_DEPS = ["sanitize-html", "postcss", "source-map-js"];
const INLINE_DEPS = ["isomorphic-git", "@noble/hashes", ...OPTIMIZED_DEPS];
const isAuthSuite =
  process.env.npm_lifecycle_event === "test:auth" ||
  process.argv.some((arg) => arg.includes("auth.worker.test.ts"));

export default defineConfig({
  plugins: [
    cloudflareTest({
      main: "./src/index.ts",
      wrangler: {
        configPath: "./wrangler.jsonc",
      },

      // Windows workaround: disable isolatedStorage to avoid EBUSY teardown failures
      // Avoid filesystem persistence to prevent SQLite CANTOPEN/EBUSY on Windows
      singleWorker: true,
      isolatedStorage: false,

      miniflare: {
        durableObjectsPersist: false,
        kvPersist: false,
        r2Persist: false,
        cachePersist: false,
        // Silence compatibility date warnings by matching installed runtime
        compatibilityDate: "2025-09-02",
        bindings: {
          ...BASE_TEST_BINDINGS,
          AUTH_ADMIN_TOKEN: isAuthSuite ? "admin" : "",
        },
      },
    }),
  ],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  // Inline deps at the vite-node server level to avoid SSR optimizer resolution issues
  server: {
    deps: {
      inline: INLINE_DEPS,
    },
  },
  test: {
    include: isAuthSuite ? [AUTH_TEST_FILE] : ["test/**/*.worker.test.ts"],
    exclude: isAuthSuite ? [] : [AUTH_TEST_FILE],
    deps: {
      optimizer: {
        ssr: {
          enabled: true,
          include: OPTIMIZED_DEPS,
        },
      },
    },
  },
});
