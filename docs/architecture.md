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
- Typed RPC methods (selected):
  - `listRefs()`, `setRefs()`, `getHead()`, `setHead()`, `getHeadAndRefs()`
  - `beginReceive()`, `finalizeReceive()`, `abortReceive()` — receive lease lifecycle
  - `beginCompaction()`, `commitCompaction()` — queue-driven pack compaction
  - `getActivePackCatalog()` — pack catalog snapshot for worker-local reads
- Push: the Worker writes `.pack` and `.idx` to R2, then commits refs and pack-catalog metadata atomically through typed DO RPCs. One active receive lease at a time; concurrent pushes receive `503 Retry-After: 10`.
- Pack metadata lives in `pack_catalog` (SQLite). Exact pack membership lives in `.idx` files in R2.

### Auth DO (`src/auth/authDO.ts`)

- Stores owners → token hashes
- `/verify` for Worker auth checks; `/users` for admin UI/API
- PBKDF2-SHA256 with 100k iterations for password hashing

### Caching Layer (`src/cache/`)

- **UI Cache**: 60s for HEAD/refs, 5min for README, 1hr for tag commits
- **Object Cache**: Immutable Git objects cached for 1 year
- **Pack discovery and memoization**: `src/git/object-store/catalog.ts#loadActivePackCatalog()` loads the active pack catalog through the Repo DO once per request and memoizes the snapshot in `RequestMemo`.
- **Per-request limiter and soft budget**: All DO/R2 calls in read and upload paths use a concurrency limiter and a soft subrequest budget to avoid hitting platform limits.

### Durable Objects SQLite (drizzle-orm)

- The Repository DO maintains a small SQLite database using `drizzle-orm/durable-sqlite` for metadata that benefits from indexed lookups and batch queries.
- Migrations run during DO initialization via `migrate(db, migrations)` and Wrangler `new_sqlite_classes` (see `wrangler.jsonc` and `drizzle.config.ts`).
- Tables:
  - `pack_catalog(pack_key, ...)` — authoritative pack metadata: key, state, tier, sequence range, object count, byte sizes, creation/supersession timestamps. Drives both read-path discovery and compaction planning.
- Access policy: all SQLite operations must go through the DAL (`src/do/repo/db/dal.ts`). Avoid raw drizzle queries outside the DAL.
- Registry note: owner→repo registry still uses Workers KV (`OWNER_REGISTRY`) for the web UI owner listing. Pack discovery and membership no longer use KV.

### Static assets and UI rendering (env.ASSETS + React SSR)

- React page components are rendered on the Worker through `renderToReadableStream()` in `src/client/server/render.tsx`.
- Route handlers call `renderUiView()` and the view registry in `src/client/server/registry.tsx` so SSR pages and fragments share one rendering path.
- Client assets are split across `src/client/entries/*.ts`, with `src/client/entries/styles.ts` loading shared UI CSS and route-specific entrypoints mounting only the islands each page needs.
- Production assets are built by Vite and served through the `ASSETS` binding using the generated manifest (`dist/client/manifest.json`).
- Development runs through the Cloudflare Vite plugin so Worker code, TSX, and CSS all participate in the same hot-reload pipeline.
- Assets config uses `html_handling: "none"` so the Worker controls routes like `/auth` without the assets layer intercepting them.

## Background processing and alarms

- The repo DO `alarm()` handles:
  - Lightweight lease cleanup (expired receive/compaction leases)
  - Compaction queue re-arm when `compactionWantedAt` is set
  - Idle cleanup (purge empty repos after idle timeout)
- Helpers:
  - `rearmCompactionQueueFromAlarm()` - Triggers compaction when requested
  - `handleIdleAndMaintenance()` - Manages idle cleanup and alarm scheduling
  - `shouldCleanupIdle()` - Determines if cleanup is needed
  - `performIdleCleanup()` - Executes cleanup
  - `purgeR2Mirror()` - Handles R2 cleanup

## Logging

- Structured JSON logs are emitted with a minimal logger. Set `LOG_LEVEL` to `debug|info|warn|error` to control verbosity.

See also:

- [Storage model](./storage.md)
- [Data flows](./data-flows.md)
- Top-level `README.md` for development and testing commands.
