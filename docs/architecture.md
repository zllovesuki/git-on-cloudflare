# Architecture Overview

This project implements a Git Smart HTTP v2 server on Cloudflare Workers using a hybrid of Durable Objects (DO) and R2.

## Module Structure

The codebase is organized into focused modules with `index.ts` export files:

- **`/git`** - Core Git functionality
  - `operations/` - Git operations (upload-pack, receive-pack)
  - `core/` - Protocol handling, pkt-line, readers
  - `pack/` - Pack assembly, unpacking, indexing
- **`/do`** - Durable Objects
  - `repo/repoDO.ts` - Repository Durable Object (per-repo authority)
  - `auth/authDO.ts` - Authentication Durable Object
- **`/auth`** - Authentication module
  - `verify.ts` - Token verification
- **`/cache`** - Two-tier caching system
  - UI layer caching (JSON responses)
  - Git object caching (immutable objects)
- **`/web`** - Web UI utilities
  - `format.ts` - Content formatting helpers
  - `render.ts` - Page rendering
  - `templates.ts` - React view compatibility shim
- **`/ui`** - React SSR UI layer
  - `server/` - Document shell, manifest resolution, render registry
  - `pages/` - Route-level React page components
  - `components/` - Shared server-rendered UI building blocks
  - `islands/` - Small client-side interactive modules
  - `client/entry.tsx` - Browser entry for CSS and islands
- **`/common`** - Shared utilities
  - `compression.ts`, `hex.ts`, `logger.ts`, `response.ts`, `stub.ts`, `progress.ts`
- **`/registry`** - Owner/repo registry management
- **`/routes`** - HTTP route handlers
  - `git.ts` - Git protocol endpoints (upload-pack, receive-pack)
  - `ui.ts` - Web UI routes for browsing repos
  - `auth.ts` - Authentication UI and API endpoints
  - `admin.ts` - Admin routes for registry management

## Core Components

### Worker Entry (`src/index.ts`)

- Routes for Git endpoints, admin JSON, and the web UI
- Integrates all route handlers via AutoRouter

### Repository DO (`src/do/repo/repoDO.ts`)

- Owns refs/HEAD and loose objects for a single repo
- HTTP endpoints (minimal):
  - `POST /receive` — receive-pack (push) handler
- Typed RPC methods (selected):
  - `listRefs()`, `setRefs()`, `getHead()`, `setHead()`, `getHeadAndRefs()`
  - `getObjectStream()`, `getObject()`, `getObjectSize()`
  - `getPackLatest()`, `getPacks()`, `getPackOids()`, `getPackOidsBatch()`
  - `getUnpackProgress()` — unpacking status/progress for UI gating (includes `queuedCount` and `currentPackKey`)
- Push flow: raw `.pack` is written to R2, a fast index-only step writes `.idx`, and unpack is queued for background processing on the DO alarm in small time-budgeted chunks.
- Maintains pack metadata (`lastPackKey`, `lastPackOids`, `packList`). Exact pack membership lives in SQLite (`pack_objects`), not in KV.

#### Receive-pack queueing

- The DO maintains at most one active unpack (`unpackWork`) and a one-deep next slot (`unpackNext`).
- When a push arrives while unpacking is active:
  - If `unpackNext` is empty, the new pack is staged as `unpackNext`.
  - If `unpackNext` is already occupied, the DO returns HTTP 503 with `Retry-After` pre-body.
- The Worker performs a preflight call to the DO RPC `getUnpackProgress()` and returns 503 early when a next pack is already queued, avoiding unnecessary upload.

### Auth DO (`src/auth/authDO.ts`)

- Stores owners → token hashes
- `/verify` for Worker auth checks; `/users` for admin UI/API
- PBKDF2-SHA256 with 100k iterations for password hashing

### Caching Layer (`src/cache/`)

- **UI Cache**: 60s for HEAD/refs, 5min for README, 1hr for tag commits
- **Object Cache**: Immutable Git objects cached for 1 year
- **Pack discovery and memoization**: `src/git/operations/packDiscovery.ts#getPackCandidates()` coalesces per-request discovery using DO metadata (latest + list) with a best‑effort R2 listing fallback. Results are memoized in `RequestMemo`.
- **Per-request limiter and soft budget**: All DO/R2 calls in read and upload paths use a concurrency limiter and a soft subrequest budget to avoid hitting platform limits.

### Durable Objects SQLite (drizzle-orm)

- The Repository DO maintains a small SQLite database using `drizzle-orm/durable-sqlite` for metadata that benefits from indexed lookups and batch queries.
- Migrations run during DO initialization via `migrate(db, migrations)` and Wrangler `new_sqlite_classes` (see `wrangler.jsonc` and `drizzle.config.ts`).
- Tables:
  - `pack_objects(pack_key, oid)` — exact membership of OIDs per pack; indexed by `oid` for fast lookups and batched IN queries.
  - `hydr_cover(work_id, oid)` — hydration coverage set per work id to build thick packs.
  - `hydr_pending(work_id, kind, oid)` — pending OIDs for hydration work; `kind` ∈ {`base`, `loose`}; PK `(work_id, kind, oid)` and index on `(work_id, kind)`.
- Access policy: all SQLite operations must go through the DAL (`src/do/repo/db/dal.ts`). Avoid raw drizzle queries outside the DAL.
- Usage highlights:
  - `getPackOidsBatch()` efficiently loads OID membership for multiple packs in one call.
  - Hydration stores coverage in SQLite and emits thick packs (no deltas) while persisting pack membership immediately for robust coverage.
- Registry note: owner→repo registry still uses Workers KV (`OWNER_REGISTRY`) for the web UI owner listing. Pack discovery and membership no longer use KV.

### Static assets and UI rendering (env.ASSETS + React SSR)

- React page components are rendered on the Worker through `renderToReadableStream()` in `src/ui/server/render.tsx`.
- `src/web/templates.ts` now delegates existing `renderView()` calls into the React view registry so route/backend logic stays unchanged.
- The browser entry at `src/ui/client/entry.tsx` imports `src/styles/app.css` and mounts focused islands for theme switching, ref picking, merge expansion, auth management, blob copy actions, and repo admin controls.
- Production assets are built by Vite and served through the `ASSETS` binding using the generated manifest (`dist/client/manifest.json`).
- Development runs through the Cloudflare Vite plugin so Worker code, TSX, and CSS all participate in the same hot-reload pipeline.
- Assets config uses `html_handling: "none"` so the Worker controls routes like `/auth` without the assets layer intercepting them.

## Background processing and alarms

- The repo DO `alarm()` performs multiple duties:
  1. Unpack chunks within a time budget
  2. Hydration slices (resumable segment building and coverage thickening)
  3. Idle cleanup for empty, idle repos
  4. Periodic pack maintenance (prune old packs + metadata)
  - `handleUnpackWork()` - Processes pending unpack work
  - Hydration helpers: `startHydration()` (RPC), `clearHydration()` (RPC), `processHydrationSlice()`
  - `handleIdleAndMaintenance()` - Manages idle cleanup and maintenance
  - `shouldCleanupIdle()` - Determines if cleanup is needed
  - `performIdleCleanup()` - Executes cleanup
  - `purgeR2Mirror()` - Handles R2 cleanup
- Unpack chunking is controlled via env vars: `REPO_UNPACK_CHUNK_SIZE`, `REPO_UNPACK_MAX_MS`, `REPO_UNPACK_DELAY_MS`, `REPO_UNPACK_BACKOFF_MS`.

## Logging

- Structured JSON logs are emitted with a minimal logger. Set `LOG_LEVEL` to `debug|info|warn|error` to control verbosity.

See also:

- [Storage model](./storage.md)
- [Data flows](./data-flows.md)
- Top-level `README.md` for development and testing commands.
