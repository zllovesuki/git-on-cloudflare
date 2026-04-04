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

### No ad-hoc or duplicated types

- Before introducing a type, check whether a canonical one already exists. Key hubs:
  `src/git/core/objects.ts` (GitObjectType), `src/common/hex.ts` (OID helpers, zeroOid),
  `src/git/object-store/support.ts` (typeCodeToObjectType), `src/git/operations/limits.ts` (Limiter).
- Do not use `ReturnType<>`, `Awaited<ReturnType<>>`, or inline union literals when a named
  type already covers the shape. Extract a named type alias if one doesn't exist yet.
- Do not duplicate helpers. If you need `isZeroOid` or `typeCodeToObjectType`, import the
  existing one—don't redefine it locally.
- Watch for unused imports left behind after refactors; remove them.

### Keep comments in sync with code

- When modifying behavior, update every comment, JSDoc, and inline note that describes the
  old behavior in the same change. Outdated comments are worse than no comments.
- Review neighboring comments when editing a function—if the surrounding prose no longer
  matches the logic, fix it.

### Remove implementation-phase language

- Do not leave wording like "Phase 1/2/3", "TODO: next phase", "streaming-push WIP", or
  similar milestone markers in code, comments, or filenames once the feature is merged or
  the phase boundary is no longer meaningful. These create confusion for future readers.
- If a prior change left such references and you're editing the same area, clean them up.

### Visibility logging on new code paths

- Every non-trivial code path that touches R2, DO RPC, or background work must include
  structured logging using `createLogger` from `src/common/logger.ts` (or the DO's
  `this.logger`).
- Follow the established conventions: kebab-case message identifiers scoped by component
  (e.g. `"receive:finalize-committed"`), appropriate log level (debug for flow, info for
  state changes, warn for recoverable errors, error for hard failures), and structured
  extra fields with relevant context (oid, packKey, counts, etc.).
- When adding a branch or error path to an existing function that already logs, add
  matching visibility for the new path—don't leave silent gaps.

### Limiter usage on platform-bound calls

- Every R2 read/write and outbound DO RPC in a request-scoped code path must go through
  the `Limiter` from `src/git/operations/limits.ts` via `limiter.run(label, fn)`.
  Obtain the limiter with `getLimiter(cacheCtx)` or pass it through options.
- Use a descriptive label prefixed by target (e.g. `"r2:get-pack"`, `"do:get-object-compat"`).
- Respect the subrequest budget (`DEFAULT_SUBREQUEST_BUDGET = 900`). If the code path has
  its own budget (like `RECEIVE_SUBREQUEST_BUDGET`), use `countSubrequest()` to track it.
- Never bypass the limiter for "just one call"—the hard 1000-subrequest and
  6-concurrent-connection ceilings apply to the entire request, not individual call sites.

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
- `src/do/repo/repoDO.ts`: repository Durable Object — metadata authority, streaming receive leases, compaction, and background work
- `src/do/repo/db/dal.ts`: the required access layer for SQLite-backed repo metadata
- `src/client/server/render.tsx` and `src/client/server/registry.tsx`: SSR view registration and rendering
- `wrangler.jsonc`: bindings, vars, assets handling, compatibility date

## Directory Map

- `src/routes/`: HTTP route registration and route handlers
- `src/do/repo/`: repository Durable Object, streaming receive, compaction, maintenance, storage, DB
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
- Streaming receive uses a lease model: one active receive lease at a time, acquired via `beginReceive()` and committed via `finalizeReceive()`. Legacy receive-pack queuing (one active unpack plus one queued `unpackNext`) remains for repos in `legacy` mode during the rollback window.
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

- Git protocol, DO, pack, compaction, caching, or routing changes:
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
- Re-introducing loose objects as a correctness dependency
- Assuming README or docs reflect every current file path without checking the source tree
