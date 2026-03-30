# AGENTS.md

## Purpose

This repository is a Git Smart HTTP v2 server implemented on Cloudflare Workers with Durable Objects, R2, KV, React SSR, and a small amount of client-side hydration.

Write changes against the current source tree, not the docs alone. Some documentation is slightly behind the live layout.

## Rules from the user

- Reuse existing types/helpers/methods. Do not invent new types/helpers/method unnecessarily, especially do not use any or casting.
- Prefer clarity over cleverness. Favor explicit names and intermediate types over dense inline expressions. Small duplication is acceptable when it materially improves readability and maintenance.
- Comment the code, especially around nuanced behaviors and footguns. This is also not an excuse to be lean on comments.
- Prioritize lookup on `cloudflare-docs` mcp for up-to-date Cloudflare developer docs (if available). Fallback to searching the web if you cannot find the relevant information on this mcp

### Avoid transitive boundary crossings

- Try to keep cross-runtime boundaries to a single hop from the caller. Workers -> DO, Workers -> R2, or DO -> R2 are allowed, but do not chain boundaries transitively. In particular, Workers -> DO -> R2 is not allowed in most circumstances.
- Retain strict mutation boundaries between R2, Workers, and Durable Object: Durable Objects can hold a transaction against its object. Do not attempt to propose a design that resembles a distributed transaction (e.g. Workers reads a row, decide on resolutions, then invoke Durable Object RPC to mutate)
- If there needs to be resolution, Durable Object can resolve that conflict in a transaction and return a tagged union for Workers to decide
- If you need to reach for `blockConcurrencyWhile` in an Durable Object RPC, your design is probably wrong. Try again.
- In general: Workers stays stateless, no mutations; Durable Objects are stateful, transactional.

### Error handling

- Durable Objects do not throw. Use tagged union between Workers and Durable Objects to communicate outcome.
- Reserve `throw` in Durable Object for FUBAR

## Stack At A Glance

- Runtime: Cloudflare Workers with `nodejs_compat`
- Language: TypeScript ESM, strict mode
- UI: React 19 SSR via `react-dom/server`, client islands, Tailwind CSS v4, Vite
- Storage: Durable Object storage, Durable Object SQLite via `drizzle-orm/durable-sqlite`, R2, KV
- Routing: `itty-router`
- Path alias: `@/*` maps to `src/*`
- Formatting: Prettier only; no ESLint config is present

## First Files To Read

- `src/index.ts`: top-level router registration and route ordering
- `src/routes/git.ts`: Git Smart HTTP endpoints, upload-pack/receive-pack behavior
- `src/routes/admin.ts`: owner-authenticated admin JSON endpoints
- `src/routes/auth.ts`: auth UI and auth API endpoints
- `src/do/repo/repoDO.ts`: repository Durable Object authority and background work entrypoints
- `src/do/repo/db/dal.ts`: the required access layer for SQLite-backed repo metadata
- `src/client/server/render.tsx` and `src/client/server/registry.tsx`: SSR view registration and rendering
- `wrangler.jsonc`: bindings, vars, assets handling, compatibility date

## Directory Map

- `src/routes/`: HTTP route registration and route handlers
- `src/do/repo/`: repository Durable Object, unpacking, hydration, maintenance, storage, DB
- `src/do/auth/`: authentication Durable Object
- `src/git/core/`: low-level Git protocol parsing and object helpers
- `src/git/operations/`: fetch, receive, read-path logic, streaming upload-pack implementation
- `src/git/pack/`: pack assembly, pack metadata, unpacking/index helpers
- `src/client/pages/`: route-level React pages
- `src/client/components/`: shared SSR components
- `src/client/islands/`: client-only interactive modules
- `src/client/entries/`: Vite client entrypoints used by SSR pages
- `src/web/`: request parsing, formatting, MIME/JSON helpers
- `src/common/`: shared response, logging, compression, stubs, progress helpers
- `test/`: Vitest worker integration tests and AVA unit tests
- `docs/`: architecture and API notes; useful, but verify against source before relying on path details

## Core Invariants

- Route order matters. `registerAuthRoutes(router)` must stay before `registerUiRoutes(router)` so `/auth` is not shadowed by `/:owner`.
- The Worker owns HTML routing. `wrangler.jsonc` sets `assets.html_handling` to `"none"`; do not move page ownership into the assets layer by accident.
- The repo Durable Object is the source of truth for a single repository. Keep refs/HEAD authority there.
- SQLite access for repo metadata must go through `src/do/repo/db/dal.ts`. Do not add ad hoc raw Drizzle queries in unrelated files.
- `RepoDurableObject.fetch()` intentionally exposes only a small HTTP surface. Keep typed RPC methods as the default internal interface.
- Receive-pack queuing is intentionally bounded: one active unpack plus one queued `unpackNext`. The preflight in `src/routes/git.ts` and the queueing in the DO must stay consistent.
- Git fetch paths are streaming-sensitive. Avoid unnecessary buffering on upload-pack and pack assembly paths.
- Push auth is optional by configuration. Do not accidentally require auth when `AUTH_ADMIN_TOKEN` is unset.
- UI rendering goes through `renderUiView()` and the view registry in `src/client/server/registry.tsx`. New pages should plug into that system rather than inventing a parallel renderer.

## Normal Workflow For Agents

1. Check repo state first with `git status --short`.
2. Read the smallest relevant slice of the codebase before editing.
3. Prefer narrow changes in the subsystem that owns the behavior.
4. Run the smallest useful validation commands before finishing.
5. Update tests when behavior changes.

Dirty worktrees are normal here. Do not overwrite or revert unrelated user changes.

## Commands

### Install and run

```bash
npm install
npm run dev
```

### Build and static validation

```bash
npm run build
npm run typecheck
npm run format:check
```

### Formatting

```bash
npm run format
```

### Tests

`npm run test` runs AVA tests for non-worker units in `test/**/*.test.ts` excluding `*.worker.test.ts`.

`npm run test:workers` runs Vitest against Cloudflare worker integration tests.

`npm run test:auth` runs only `test/auth.worker.test.ts` with `AUTH_ADMIN_TOKEN` enabled in the Vitest pool bindings.

The 42 MiB pack-indexer fixture test is opt-in. Use `PACK_INDEXER_FIXTURE=1 npx vitest run --config vitest.config.ts test/pack-indexer-fixture.worker.test.ts` when you intentionally want to run it.

Targeted commands:

```bash
npx ava test/object-parse.test.ts
npx vitest run --config vitest.config.ts test/receive-push.worker.test.ts
npx vitest run --config vitest.config.ts test/auth.worker.test.ts
```

### Cloudflare and schema maintenance

```bash
npm run cf-typegen
npm run db:gen
```

Do not edit generated migrations under `src/drizzle/` manually. Treat `src/do/repo/db/schema.ts` as the source of truth: make the schema change there first, then run `npm run db:gen` to generate the migration.

## Validation By Change Type

- Git protocol, DO, pack, hydration, caching, or routing changes:
  run `npm run typecheck` and the relevant worker tests in `test/*.worker.test.ts`
- Auth changes:
  run `npm run test:auth`
- Pure parsing or helper changes:
  run the targeted AVA test plus `npm run typecheck`
- UI-only SSR/component changes:
  run `npm run typecheck`; if route behavior changed, add relevant worker coverage
- SQLite schema or DAL changes:
  run `npm run db:gen`, `npm run typecheck`, and the worker tests that cover the affected flow

## UI Notes

- SSR pages live in `src/client/pages/`.
- Shared shell/document logic lives in `src/client/server/`.
- Client interactivity should stay in focused islands under `src/client/islands/`.
- If a page needs client code, wire it through `src/client/entrypoints.ts` and `src/client/server/registry.tsx`.
- Shared CSS starts at `src/client/styles.css`, which imports `src/client/styles/app.css`.

## Testing Notes

- Vitest uses `@cloudflare/vitest-pool-workers` and points at `src/index.ts`.
- The Vitest pool compatibility date should stay aligned with `wrangler.jsonc`.
- Stable test env vars are defined in `test/vitest.bindings.ts`.
- AVA relies on `test/register.js` and `test/loader.js` to resolve the `@/` alias.

## Good Change Patterns

- When adding a route, modify the owning module under `src/routes/` and keep registration order safe.
- When adding repo metadata, decide whether it belongs in DO storage or SQLite; if SQLite, add schema and DAL changes together.
- When adding a new page, register it in `src/client/server/registry.tsx` and add a client entrypoint only if hydration is actually needed.
- When changing pack or fetch behavior, look for existing worker tests before writing new code; the repo already has strong coverage for those paths.

## Avoid

- Broad refactors across route, DO, and UI layers unless the task truly needs it
- Raw SQL/Drizzle access outside the repo DB DAL
- Accidental route shadowing with `/:owner` and `/:owner/:repo`
- Converting streaming paths to buffered implementations without a strong reason
- Assuming README or docs reflect every current file path without checking the source tree
