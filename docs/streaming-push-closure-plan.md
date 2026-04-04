# Streaming-Push Closure Release

> **Implementation complete.** All steps in this plan have been executed. This document is preserved as a record of the closure implementation.

## Context

Phase 5 cutover (98ad7dd) has been deployed and verified in production. The cutover runbook (`MIGRATION-STREAMING-PUSH.md`) requires per-repo inventory through `GET /admin/storage-mode`, which calls `getRepoStorageModeControl()` → `getActivePackCatalogSnapshot()` → `ensureRepoMetadataDefaults()`. A validated cutover therefore **already materializes `pack_catalog` and persists `repoStorageMode` for every repo it touches**. There are no remaining cold repos after validated cutover.

This is the final release. It removes all legacy code paths — rollback backfill, hydration, unpack, storage-mode switching, legacy pack tracking, and cold-start migration scaffolding. After this release, rollback to pre-streaming is impossible.

The codebase should be substantially smaller afterward (~3000+ lines of production code removed, ~12 test files deleted, `isomorphic-git` dependency dropped).

---

## Implementation Plan

### Step 1: Rewrite test seeders onto post-cutover state

All shared test seeders currently write legacy KV keys (`packList`, `lastPackKey`, `lastPackOids`) and use `indexPackOnly` from `src/git/pack/unpack.ts` (to be deleted). Rewrite them first so all subsequent steps can freely delete legacy code.

**`test/util/packed-repo.ts`:**

- Rename `seedLegacyPackedRepo` → `seedPackedRepoState`
- Replace `indexPackOnly` with streaming pack indexer (`src/git/pack/indexer/`) to produce `.idx` and write to R2
- Insert `pack_catalog` rows via DAL (`upsertPackCatalogRow`) with `kind: "receive"`, `state: "active"`
- Bump `packsetVersion` and `nextPackSeq`
- Stop writing `lastPackKey`, `lastPackOids`, `packList`
- `registerTestPack`: same pattern — replace `indexPackOnly`, use catalog, stop writing KV keys
- Keep `deleteLooseObjectCopies`, `readRepoCatalogState`, `buildTreePayload` as-is

**`test/util/pack-first.ts`:**

- `seedPackFirstRepo` calls `seedLegacyPackedRepo` — update to use the renamed function

**`src/do/repo/repoDO/seeding.ts`:**

- Replace `import { encodeGitObjectAndDeflate, indexPackOnly, ... }` with `encodeGitObject` from `@/git/core/index.ts`
- Use streaming pack indexer to produce `.idx` and write to R2
- Insert a `pack_catalog` row via DAL
- Bump `packsetVersion` and `nextPackSeq`
- Remove writes to `lastPackKey`, `lastPackOids`, `packList`
- Keep `withPack: false` branch writing loose objects to DO storage

**`test/ls-refs-filters.worker.test.ts`** and **`test/commit-diff.worker.test.ts`:**

- Replace `encodeGitObjectAndDeflate` import with `encodeGitObject` from `@/git/core/index.ts` (superset: also returns `raw`)

**`test/fetch-streaming.worker.test.ts`** and **`test/multipack-union.worker.test.ts`:**

- Replace `seedMinimalRepo(false)` (zero-pack seeding) with pack-backed seeding via `seedMinimalRepo(true)` or equivalent. Non-empty zero-pack repos are unsupported after closure.

**Verify:** `npm run typecheck`

---

### Step 2: Remove storage-mode and rollback controls end-to-end

This is the highest-impact structural change. Remove the entire concept of `RepoStorageMode` and all associated control/transition machinery.

**Files to delete:**

- `src/contracts/repoStorageMode.ts`
- `src/do/repo/catalog/storageMode.ts`
- `src/do/repo/catalog/legacyCompat.ts`
- `src/maintenance/legacyCompatBackfill.ts`

**`src/do/repo/repoState.ts`:**

- Remove `RepoStorageMode` type
- Remove `LegacyCompatBackfillState` type
- Remove `repoStorageMode` and `legacyCompatBackfill` from `RepoStateSchema`

**`src/do/repo/repoDO.ts`:**

- Remove storage-mode RPCs: `getRepoStorageMode()`, `getRepoStorageModeControl()`, `setRepoStorageMode()`, `setRepoStorageModeGuarded()`
- Remove backfill RPCs: `requestLegacyCompatBackfill()`, `beginLegacyCompatBackfill()`, `storeLegacyCompatBatch()`, `completeLegacyCompatBackfill()`, `failLegacyCompatBackfill()`
- Remove all associated imports from `./catalog.ts`

**`src/routes/admin.ts`:**

- Remove `GET/PUT /:owner/:repo/admin/storage-mode` routes
- Remove `POST /:owner/:repo/admin/storage-mode/backfill` route
- Remove `/hydrate` alias routes (both POST and DELETE) — keep `/compact` routes
- Remove `StorageModePayload` type and `isStorageModePayload` validator

**`src/maintenance/queue.ts`:**

- Remove `LegacyCompatBackfillQueueMessage` from union type
- Remove `isLegacyCompatBackfillMessage` function and handler dispatch
- Remove import of `handleLegacyCompatBackfillMessage`

**`src/do/repo/catalog/shared.ts`:**

- Remove `sanitizeRawStorageMode()` function
- Remove `repoStorageMode` from `BeginReceiveResult` type

**`src/do/repo/catalog/state.ts`:**

- Delete `getRepoStorageModeValue()` and `setRepoStorageModeValue()` functions
- Remove `RepoStorageMode` import

**`src/do/repo/catalog/index.ts`:**

- Remove re-exports from deleted modules (`storageMode.ts`, `legacyCompat.ts`)

**`src/client/islands/repo-admin/StorageModeCard.tsx`:**

- Delete entire file

**`src/client/islands/repo-admin/index.tsx`, `types.ts`, `useRepoAdminActions.ts`:**

- Remove `<StorageModeCard>` component usage and import
- Remove `storageModeControl` from props/state
- Remove `setStorageMode`, `requestLegacyCompatBackfill` actions
- Remove imports of `RepoStorageMode`, `RepoStorageModeControl`, `RollbackCompatControl`
- `CompactionCard` becomes unconditionally available

**Server-side admin page:** Remove `storageModeControl` prop passing

**Verify:** `npm run typecheck`

---

### Step 3: Collapse receive and advertisement to streaming-only

**`src/routes/git.ts` — `handleReceivePackPOST()`:**

- Remove `stub.getRepoStorageMode()` call
- Remove the legacy branch (unpack progress preflight, `stub.fetch("https://do/receive"...)`)
- Always call `handleStreamingReceivePackPOST` — the function body simplifies to just the streaming call

**`src/git/core/protocol.ts`:**

- Stop querying the DO for mode — always advertise streaming receive capabilities (`side-band-64k`, `quiet`)

**`src/git/receive/streamReceivePack.ts`:**

- Remove the `begin.repoStorageMode !== "streaming"` check
- Simplify `BeginReceiveResult` accordingly (remove `repoStorageMode` field usage)

**`src/do/repo/catalog/receive.ts`:**

- Remove mode check (`currentMode !== "streaming"` guard) from `finalizeReceiveState`
- Remove `currentMode` from `FinalizeReceiveResult` type variants
- Update `ensureRepoMetadataDefaults` call (no longer returns mode)

**`src/do/repo/catalog/compaction/lease.ts` and `requests.ts`:**

- Remove mode checks (`currentMode !== "streaming"` guards) — compaction is unconditional

**Verify:** `npm run typecheck`

---

### Step 4: Delete legacy receive, unpack, and hydration code

**Files to delete:**

- `src/do/repo/repoDO/receive.ts` — legacy DO receive handler
- `src/do/repo/unpack.ts` — alarm-driven unpack worker
- `src/do/repo/repoDO/hydration.ts` — hydration alarm handler
- `src/do/repo/hydration/` — entire directory
- `src/git/operations/receive.ts` — legacy `receivePack()`
- `src/git/pack/unpack.ts` — `indexPackOnly`, `unpackPackToLoose`, `encodeGitObjectAndDeflate`
- `src/git/pack/loose-loader.ts`

**Barrel exports to update:**

- `src/git/pack/index.ts` — remove `export * from "./unpack.ts"` and `export * from "./loose-loader.ts"`
- `src/git/operations/index.ts` — remove `export * from "./receive.ts"`

**`src/do/repo/repoDO.ts`:**

- Remove `/receive` route from `fetch()` — `fetch()` now always returns 404
- Remove `handleReceive()` private method
- Remove `getUnpackProgress()` RPC method
- Remove `startHydration()` and `clearHydration()` RPC methods
- Remove `handleHydrationWork()` private method
- In `alarm()`: remove the entire legacy `else` branch — always run compaction re-arm
- Remove imports: `handleUnpackWork`, `getUnpackProgress`, `handleReceiveRequest`, `enqueueHydrationTask`, `summarizeHydrationPlan`, `clearHydrationState`, `handleHydrationAlarmWork`

**`src/common/progress.ts`:**

- Remove `UnpackProgress` interface and `getUnpackProgress()` function
- Keep `RepoActivity` and `getRepoActivity()`

**Verify:** `npm run typecheck`

---

### Step 5: Remove migration scaffolding needed only before/during cutover

**Files to delete:**

- `src/do/repo/catalog/legacyBackfill.ts` — KV→catalog backfill
- `src/do/repo/db/migrate.ts` — KV→SQL migration

**`src/do/repo/repoDO.ts` constructor:**

- Remove `migrateKvToSql(...)` call
- Remove `sanitizeRawStorageMode(...)` call (already deleted in Step 2)
- Remove associated imports

**`src/do/repo/catalog/state.ts`:**

- Remove `hydrateLegacyCatalog` import and empty-catalog fallback. `getActivePackCatalogSnapshot()` just calls `listActivePackCatalog(db)` directly.

**`src/do/repo/catalog/shared.ts`:**

- Replace `ensureRepoMetadataDefaults()` with a smaller bootstrap helper that initializes only `refsVersion`, `packsetVersion`, `nextPackSeq` (and `head` if needed). Remove all missing-mode inference and shadow-read normalization. Return `void`.

**`src/do/repo/repoState.ts`:**

- Remove `PackOidsKey` type, `packOidsKey()` helper, `Record<PackOidsKey, ...>` intersection

**Verify:** `npm run typecheck`

---

### Step 6: Remove legacy mirrors, pack tracking, periodic maintenance, and dead config

**Files to delete:**

- `src/do/repo/packs.ts` — legacy pack-list management (`getPacks`, `removePackFromList`, `calculateStableEpochs`)

**`src/do/repo/repoState.ts`:**

- Remove from `RepoStateSchema`: `lastPackKey`, `lastPackOids`, `packList`, `unpackWork`, `unpackNext`, `hydrationWork`, `hydrationQueue`, `lastMaintenanceMs`
- Remove `UnpackWork`, `HydrationTask`, `HydrationReason`, `HydrationStage`, `HydrationWork` types

**`src/do/repo/repoDO.ts`:**

- Remove `getPacks()` RPC method

**`src/do/repo/catalog/shared.ts`:**

- Remove `mirrorLegacyPackKeys()` function

**`src/do/repo/catalog/receive.ts`:**

- Remove `mirrorLegacyPackKeys(store, activeCatalog)` call from `finalizeReceiveState`

**`src/do/repo/catalog/compaction/lease.ts`:**

- Remove `mirrorLegacyPackKeys(store, activeCatalog)` call from `commitCompactionState`

**`src/do/repo/packOperations.ts`:**

- Remove `removePackFromList` import and call — pack deletion consults `pack_catalog` only

**`src/do/repo/maintenance.ts`:**

- Gut to idle cleanup only. Remove `performMaintenance()`, `runMaintenance()`, `isMaintenanceDue()`
- Remove imports: `calculateStableEpochs`, `enqueueHydrationTask`, `getPackOidsHelper`, `deletePackObjects`, `getConfig`
- **Critical: `shouldCleanupIdle` correctness.** Currently uses `!lastPackKey` as the "repo has data" guard before destructive purge. After removing `lastPackKey`, replace with an explicit pack catalog check. The idle cleanup must only proceed when ALL three conditions hold:
  1. `refs` is empty
  2. HEAD is unborn/missing
  3. `pack_catalog` has zero active rows (query via `getPackCatalogCount(db) === 0`)
- Do NOT infer emptiness from refs/HEAD alone — a repo with no refs but active packs must not be purged
- Keep: `handleIdleAndMaintenance` (just idle cleanup + schedule next alarm), `performIdleCleanup`, `purgeR2Mirror`

**`src/do/repo/scheduler.ts`:**

- Remove entire legacy branch (unpack/hydration scheduling)
- Remove `"unpack"`, `"hydration"`, and `"maint"` from alarm reason types
- Simplify to: compaction wake/retry and idle cleanup only

**`src/do/repo/repoConfig.ts`:**

- Remove `keepPacks`, `packListMax`, `unpackChunkSize`, `unpackMaxMs`, `unpackDelayMs`, `unpackBackoffMs`
- Remove `REPO_KEEP_PACKS`, `REPO_PACKLIST_MAX`, `REPO_UNPACK_*`, `REPO_DO_MAINT_MINUTES` env var refs
- Only `idleMs` remains (from `REPO_DO_IDLE_MINUTES`)

**`src/routes/ui/helpers.ts`:**

- Remove `computeNextMaintenance()` function and its `getConfig` import

**`src/routes/ui/adminPage.ts`:**

- Remove `computeNextMaintenance` call and `nextMaintenanceIn`/`nextMaintenanceAt` data

**`src/do/repo/debug/` (types.ts and state builder):**

- Remove `repoStorageMode`, `rollbackCompat`, `unpackWork`, `unpackNext`, `hydration*`, `lastMaintenanceMs` from debug snapshot

**Verify:** `npm run typecheck`

---

### Step 7: Drop legacy SQLite tables, remove `isomorphic-git`, clean config files

**`src/do/repo/db/schema.ts`:**

- Remove `packObjects`, `hydrCover`, `hydrPending` table definitions and exported types
- **Keep** `'legacy'` in `pack_catalog` kind CHECK constraint (existing rows with `kind='legacy'` must remain valid)

**`src/do/repo/db/dal/` and `src/do/repo/db/index.ts`:**

- Remove all functions operating on deleted tables: `insertPackOids`, `getPackOids`, `getPackOidsSlice`, `getPackObjectCount`, `deletePackObjects`, `oidExistsInPacks`, `normalizePackKeysInPlace`
- Keep `normalizePackKey` (basename extraction) if used by surviving pack catalog ops — relocate if needed

**Run:** `npm run db:gen` to generate migration dropping the three tables

**`package.json`:** remove `isomorphic-git` from dependencies

**`vitest.config.ts`:** remove `isomorphic-git` from inline deps list

**`wrangler.jsonc`:** remove `REPO_UNPACK_*`, `REPO_KEEP_PACKS`, `REPO_PACKLIST_MAX`, `REPO_DO_MAINT_MINUTES` from vars section

**`test/vitest.bindings.ts`:** remove corresponding test bindings

**Run:** `npm install`, `npm run cf-typegen`

**Verify:** `npm run typecheck`, `npm run db:gen` succeeds

---

### Step 8: Delete legacy tests, add closure-specific tests

**Test files to delete:**

- `test/streaming-receive.rollback.worker.test.ts`
- `test/streaming-default-cutover.worker.test.ts`
- `test/pack-first-read-path.storage-mode.worker.test.ts`
- `test/receive-push.worker.test.ts`
- `test/receive-queue.worker.test.ts`
- `test/fetch-during-unpack.worker.test.ts`
- `test/unpack-progress.worker.test.ts`
- `test/hydration-coverage-epochs.worker.test.ts`
- `test/hydration-clear-deletes-packobjects.worker.test.ts`
- `test/calculate-stable-epochs.worker.test.ts`
- `test/migrate-packkeys.worker.test.ts`
- `test/create-mem-pack-fs.test.ts`

**`test/maintenance.worker.test.ts`:** Remove hydration assertions, unpack references, legacy-mode test cases. Keep idle cleanup and compaction scheduling coverage.

**`test/packed-object-store.catalog.worker.test.ts`:** Remove assertions about automatic catalog backfill and storage-mode behavior. Keep only pack-catalog invariants.

**Add closure-specific assertions** (in existing or new test files):

- `receive-adv` asserts streaming receive capabilities are always advertised (no mode query)
- Admin route tests assert `/admin/storage-mode`, `/admin/storage-mode/backfill`, `/admin/hydrate` return 404
- Compaction tests assert `requestCompaction()` never returns mode-mismatch
- Seeded repos work without first-access catalog backfill

**Verify:** `npm run typecheck`, `npm run test`, `npm run test:workers`

---

### Step 9: Documentation cleanup and final sweep

**`README.md`:**

- Move upgrade warning to immediately after the first paragraph (above Quick Demo), make it a prominent callout
- State explicitly: cutover 98ad7dd is the last rollback-capable checkpoint; the closure release is destructive and irreversible
- Remove `REPO_UNPACK_*`, `REPO_KEEP_PACKS`, `REPO_PACKLIST_MAX`, `REPO_DO_MAINT_MINUTES` from Configuration section
- Remove the Limitations bullet about receive-pack buffering (streaming doesn't buffer)
- Update "Admin endpoints for hydration and repository management" text

**`MIGRATION-STREAMING-PUSH.md`:** Rewrite around three start states:

- _Fresh deployment_: deploy latest directly, everything is streaming by default
- _At validated cutover (98ad7dd)_: deploy closure directly; all repos already have catalogs and mode state from the cutover validation. **Rollback is no longer possible after closure.**
- _Before cutover_: must deploy and validate cutover first (run per-repo inventory, promote to streaming), then deploy closure
- Remove the "cold repo" operator branch — validated cutover already hydrated catalog and mode state for all repos
- Make prominent: **CUTOVER is the last safe deployment checkpoint that allows per-repo rollback. CLOSURE is destructive.**

**`docs/streaming-push.md`:**

- Update Status: "Streaming is production. Legacy paths removed in closure release."
- Convert closure checklist from future work to completed status
- Remove rollback window references, legacy mode documentation

**`docs/streaming-push-cutover-plan.md`:** Mark as historical/archived

**`CLAUDE.md` (AGENTS.md):**

- Core Invariants: remove legacy receive-pack queuing — just describe streaming lease model
- Update `repoDO.ts` description in First Files To Read
- Remove "Converting streaming paths to buffered implementations" from Avoid section
- Remove "Remove implementation-phase language" rule (migration is complete)
- Update Validation By Change Type: remove hydration references

**Other docs** (`docs/architecture.md`, `docs/data-flows.md`, `docs/storage.md`, `docs/api-endpoints.md`, `docs/caching.md`):

- Remove unpack/hydration from descriptions
- Remove rollback/legacy mode references
- Update receive path to describe streaming as the only path

**Final sweep:** grep `src/` for stale references to `storage-mode`, `backfill`, `hydrate`, `unpack`, `hydration`, `packList`, `lastPackOids`, `REPO_KEEP_PACKS`, `REPO_PACKLIST_MAX`, `REPO_UNPACK`, `REPO_DO_MAINT_MINUTES`, `shadow-read`, `phase`. Clean up stale comments.

**Verify:** `npm run typecheck`, `npm run format:check`, full test suite

---

## Dependency Graph

```
Step 1 (rewrite seeders)
  └→ Step 2 (remove storage-mode + rollback controls)
       └→ Step 3 (collapse receive to streaming-only)
            └→ Step 4 (delete legacy receive/unpack/hydration code)
                 └→ Step 5 (remove migration scaffolding)
                      └→ Step 6 (remove legacy mirrors, pack tracking, periodic maintenance)
                           └→ Step 7 (drop SQLite tables, remove isomorphic-git, clean config)
                                └→ Step 8 (delete legacy tests, add closure tests)
                                     └→ Step 9 (documentation)
```

Each step is independently verifiable with `npm run typecheck`.

---

## Key Files Modified/Deleted

### Entire files deleted (~20+ source files)

`src/contracts/repoStorageMode.ts`, `src/do/repo/catalog/storageMode.ts`, `src/do/repo/catalog/legacyCompat.ts`, `src/do/repo/catalog/legacyBackfill.ts`, `src/do/repo/unpack.ts`, `src/do/repo/repoDO/receive.ts`, `src/do/repo/repoDO/hydration.ts`, `src/do/repo/hydration/*`, `src/do/repo/packs.ts`, `src/do/repo/db/migrate.ts`, `src/maintenance/legacyCompatBackfill.ts`, `src/git/operations/receive.ts`, `src/git/pack/unpack.ts`, `src/git/pack/loose-loader.ts`, `src/client/islands/repo-admin/StorageModeCard.tsx`

### Major modifications

`src/do/repo/repoDO.ts`, `src/do/repo/repoState.ts`, `src/do/repo/catalog/shared.ts`, `src/do/repo/catalog/receive.ts`, `src/do/repo/catalog/state.ts`, `src/do/repo/maintenance.ts`, `src/do/repo/scheduler.ts`, `src/do/repo/repoConfig.ts`, `src/do/repo/db/schema.ts`, `src/routes/admin.ts`, `src/routes/git.ts`, `src/maintenance/queue.ts`, `src/common/progress.ts`

---

## Assumptions

1. Production is already on validated cutover 98ad7dd with no remaining cold repos
2. Closure does not support direct deployment onto a pre-cutover data-bearing repo — the migration guide handles that path
3. Stale legacy DO keys (`obj:*`, `packList`, etc.) and loose objects may remain on disk after closure but will not be read, maintained, or backfilled
4. `pack_catalog` remains the sole authoritative pack metadata source
5. `'legacy'` stays in the `pack_catalog` kind CHECK constraint for existing rows — no historical data rewriting

---

## Verification

After all steps:

```bash
npm run typecheck
npm run format:check
npm run test            # AVA unit tests
npm run test:workers    # Vitest worker integration tests
npm run test:auth       # Auth tests
```

Confirm:

- No code references `RepoStorageMode`, `shadow-read`, or legacy mode checking
- No `REPO_UNPACK_*`, `REPO_KEEP_PACKS`, `REPO_PACKLIST_MAX`, `REPO_DO_MAINT_MINUTES` in config
- `isomorphic-git` not in `package.json`
- `repoDO.ts` has no `/receive` HTTP handler, no unpack/hydration/backfill RPCs
- All repos are implicitly streaming (no mode field read or written)
- Removed admin endpoints return 404
- `README.md` has prominent upgrade notice
- `MIGRATION-STREAMING-PUSH.md` covers all deployment scenarios with clear rollback warning
