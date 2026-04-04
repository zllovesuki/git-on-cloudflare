# Architecture Overview

This project implements a Git Smart HTTP v2 server on Cloudflare Workers using a hybrid of Durable Objects (DO) and R2.

## Module Structure

The codebase is organized into focused modules with `index.ts` export files:

- **`/git`** - Core Git functionality
  - `operations/` - Git operations (upload-pack, receive-pack)
  - `core/` - Protocol handling, pkt-line, readers
  - `pack/` - Pack assembly, indexing
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

- Metadata authority for a single repo. The data plane lives in R2 packs.
- HTTP endpoints (minimal):
  - `POST /receive` — legacy receive-pack handler (rollback window only)
- Typed RPC methods (selected):
  - `listRefs()`, `setRefs()`, `getHead()`, `setHead()`, `getHeadAndRefs()`
  - `beginReceive()`, `finalizeReceive()`, `abortReceive()` — streaming receive lease lifecycle
  - `beginCompaction()`, `commitCompaction()` — queue-driven pack compaction
  - `getActivePackCatalog()` — pack catalog snapshot for worker-local reads
  - `getRepoStorageMode()`, `setRepoStorageModeGuarded()` — two-state mode toggle (`legacy` ↔ `streaming`)
  - `getUnpackProgress()` — legacy unpacking status (rollback window)
- Streaming push (default): the Worker writes `.pack` and `.idx` to R2, then commits refs and pack-catalog metadata atomically through typed DO RPCs.
- Legacy push (rollback window): raw `.pack` is written to R2, a fast index-only step writes `.idx`, and unpack is queued for background processing via DO alarm.
- Maintains pack metadata (`lastPackKey`, `lastPackOids`, `packList`). Exact pack membership lives in `.idx` files in R2, with SQLite (`pack_objects`) as a legacy mirror.

#### Receive-pack queueing (legacy mode only)

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
  - `pack_catalog(pack_key, ...)` — authoritative pack metadata: key, state, tier, sequence range, object count, byte sizes, creation/supersession timestamps. Drives both read-path discovery and compaction planning.
  - `pack_objects(pack_key, oid)` — legacy OID membership per pack; still populated for rollback compatibility. `.idx` files in R2 are authoritative for read-path lookups.
  - `hydr_cover(work_id, oid)` — hydration coverage set per work id (legacy rollback window).
  - `hydr_pending(work_id, kind, oid)` — pending OIDs for hydration work (legacy rollback window).
- Access policy: all SQLite operations must go through the DAL (`src/do/repo/db/dal.ts`). Avoid raw drizzle queries outside the DAL.
- Registry note: owner→repo registry still uses Workers KV (`OWNER_REGISTRY`) for the web UI owner listing. Pack discovery and membership no longer use KV.

### Static assets and UI rendering (env.ASSETS + React SSR)

- React page components are rendered on the Worker through `renderToReadableStream()` in `src/ui/server/render.tsx`.
- `src/web/templates.ts` now delegates existing `renderView()` calls into the React view registry so route/backend logic stays unchanged.
- Client assets are split across `src/ui/client/entries/*.ts`, with `src/ui/client/entries/styles.ts` loading shared UI CSS and route-specific entrypoints mounting only the islands each page needs.
- Production assets are built by Vite and served through the `ASSETS` binding using the generated manifest (`dist/client/manifest.json`).
- Development runs through the Cloudflare Vite plugin so Worker code, TSX, and CSS all participate in the same hot-reload pipeline.
- Assets config uses `html_handling: "none"` so the Worker controls routes like `/auth` without the assets layer intercepting them.

## Background processing and alarms

- The repo DO `alarm()` is mode-aware:
  - **Streaming repos**: lightweight lease cleanup, compaction queue re-arm, idle cleanup.
  - **Legacy repos (rollback window)**: unpack chunks within a time budget, hydration slices (resumable segment building and coverage thickening), idle cleanup, and periodic pack maintenance.
- Helpers:
  - `handleUnpackWork()` - Processes pending unpack work (legacy mode)
  - `rearmCompactionQueueFromAlarm()` - Triggers compaction when requested (streaming mode)
  - Hydration helpers: `startHydration()` (RPC), `clearHydration()` (RPC), `processHydrationSlice()` (legacy mode)
  - `handleIdleAndMaintenance()` - Manages idle cleanup and maintenance
  - `shouldCleanupIdle()` - Determines if cleanup is needed
  - `performIdleCleanup()` - Executes cleanup
  - `purgeR2Mirror()` - Handles R2 cleanup

## Logging

- Structured JSON logs are emitted with a minimal logger. Set `LOG_LEVEL` to `debug|info|warn|error` to control verbosity.

See also:

- [Storage model](./storage.md)
- [Data flows](./data-flows.md)
- Top-level `README.md` for development and testing commands.
