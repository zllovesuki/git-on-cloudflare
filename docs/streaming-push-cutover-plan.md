# Phase 5: Streaming-by-Default Cutover With Legacy Rollback Window

> **Historical document.** The cutover described here has been completed and the closure release has removed all legacy code paths. This plan is preserved for historical context only. See `MIGRATION-STREAMING-PUSH.md` for current deployment guidance and `docs/streaming-push-closure-plan.md` for the closure implementation plan.

## Context

Phases 1-4 of the streaming push migration are landed. The pack-first object store, streaming receive, and queue-driven compaction are all operational.

Normal push-created repos always have at least one pack in R2 (the legacy receive path writes a `.pack` on every non-delete push). Zero-pack repos were not a normal operator-facing steady state by the time streaming fetch became the default. However, legacy loose-object fallback paths did exist historically, so repos with refs but no active packs should be treated as an operator exception requiring manual intervention — not as something that never existed in code.

Phase 5 retires the legacy code paths. It is split into two named deploy checkpoints:

- **Cutover release** (this session): `shadow-read` is retired from the type system. New repos default to `streaming`. Dead DO RPCs and shadow-read validation plumbing are removed. Legacy receive/alarm branches remain callable for repos explicitly set to `legacy` mode, preserving a working rollback path without requiring a full deploy rollback.
- **Closure release** (future session): after the rollback window closes, remove all remaining compatibility-only surfaces: `/admin/hydrate`, rollback backfill, legacy receive/unpack/hydration code, legacy wrangler vars, and compatibility-only tests.

This plan covers the **cutover release only**. The closure release is documented as future work at the end.

---

## Cutover Release

### Step 1: Narrow `RepoStorageMode` type and normalize `shadow-read`

**`src/do/repo/repoState.ts`:**

- Change `RepoStorageMode` from `"legacy" | "shadow-read" | "streaming"` to `"legacy" | "streaming"`

**`src/do/repo/catalog/shared.ts` — `ensureRepoMetadataDefaults()`:**

- When `mode` is undefined (no stored key), the default depends on repo state:
  - If the repo is truly empty (no refs, no `lastPackKey`, no `packList`, AND `packsetVersion` is 0 or undefined): default to `"streaming"`
  - If any of those signals indicate prior activity (`packsetVersion > 0`, refs present, `lastPackKey` set, non-empty `packList`): default to `"legacy"` — this prevents silently auto-promoting cold existing repos that predate the mode key
  - Using `packsetVersion > 0` as an additional signal makes the heuristic more robust than relying only on refs/packList/lastPackKey
- Document this rule explicitly in `MIGRATION-STREAMING-PUSH.md`: cold repos with data but no stored mode key will come up as `legacy` and must be promoted manually
- Note: `ensureRepoMetadataDefaults()` has no logger parameter. Shadow-read normalization happens in a different site — see below.

**Shadow-read normalization** — add a dedicated helper `sanitizeRawStorageMode()` in `src/do/repo/catalog/shared.ts`:

```ts
/**
 * Reads the raw persisted storage mode and normalizes stale values.
 * Must run AFTER ensureRepoMetadataDefaults() (which handles missing keys)
 * and BEFORE any typed read of repoStorageMode.
 *
 * Returns the canonical RepoStorageMode after normalization.
 */
export async function sanitizeRawStorageMode(
  storage: DurableObjectStorage,
  logger: Logger
): Promise<RepoStorageMode> {
  const raw = (await storage.get("repoStorageMode")) as string | undefined;
  if (raw === "shadow-read") {
    await storage.put("repoStorageMode", "streaming");
    logger.info("mode:normalize-shadow-read", { previous: raw });
    return "streaming";
  }
  return (raw as RepoStorageMode) ?? "legacy";
}
```

- Call this in `src/do/repo/repoDO.ts` constructor's `blockConcurrencyWhile`, after `ensureRepoMetadataDefaults()` and before any other work
- The helper reads raw storage (untyped), normalizes, and returns a typed `RepoStorageMode` — keeping the type boundary clean
- `ensureRepoMetadataDefaults()` handles the missing-key case; `sanitizeRawStorageMode()` handles the stale-key case. They compose in sequence.

**`src/do/repo/catalog/storageMode.ts`:**

- Remove all `shadow-read` branches from `canTransition()`, `buildRepoStorageModeBlockers()`, `buildModeMessage()`, `isRepoStorageMode()`
- New transition rules:
  - `legacy → streaming`: requires `activePackCount > 0` (repos with pack data), OR the repo is truly empty (`refs` is empty AND `activePackCount === 0` AND `packsetVersion === 0`)
  - `streaming → legacy`: requires rollback backfill ready, OR the repo is truly empty (same condition: no refs, no packs, `packsetVersion === 0`)
- The empty-repo gate is NOT just "zero packs" — it requires no refs either, to avoid the unsupported loose-only-with-refs case
- Remove `ALL_REPO_STORAGE_MODES` constant (now just two modes)

**`src/contracts/repoStorageMode.ts`:**

- Update the type to `"legacy" | "streaming"`. Remove `shadow-read` from all contracts.

**Admin UI** — `src/client/islands/repo-admin/StorageModeCard.tsx`:

- Remove the three-step pipeline visualization (`legacy → shadow-read → streaming`)
- Replace with a two-state toggle: `legacy` (rollback) ↔ `streaming` (active)
- Remove any `shadow-read` references in button labels, descriptions, and blocker messages

### Step 2: Remove dead DO RPCs

These RPCs have zero production callers. Remove the public methods from `RepoDurableObject` and their backing implementations.

**`src/do/repo/repoDO.ts`** — remove these methods and their imports:

- `getObjectStream()` → backing: `src/do/repo/storage.ts:getObjectStream`
- `getObjectSize()` → backing: `src/do/repo/storage.ts:getObjectSize`
- `hasLooseBatch()` → backing: `src/do/repo/storage.ts:hasLooseBatch`
- `getObjectRefsBatch()` → backing: `src/do/repo/storage.ts:getObjectRefsBatch`
- `getPackLatest()` → backing: `src/do/repo/packs.ts:getPackLatest`
- `getPackOids()` → backing: `src/do/repo/packs.ts:getPackOids`
- `getPackOidsBatch()` → backing: `src/do/repo/packs.ts:getPackOidsBatch`

**`src/do/repo/storage.ts`** — delete: `getObjectStream`, `getObjectSize`, `hasLooseBatch`, `getObjectRefsBatch`

**`src/do/repo/packs.ts`** — delete the RPC-facing wrapper functions: `getPackLatest`, `getPackOids`, `getPackOidsBatch`. Note: `src/do/repo/hydration/helpers.ts` calls the DB-layer `getPackOids` from `db/index.ts` directly, NOT these packs.ts wrappers. The wrappers are exclusively called from their repoDO.ts RPC methods. Safe to delete.

**`src/git/pack/loose-loader.ts`** — delete `createStubLooseLoader` (exported but zero callers). Keep `createLooseLoader` (still used by legacy `indexPackOnly` during rollback window).

### Step 3: Remove shadow-read validation and compatibility fallback from the read path

**`src/git/operations/read/objects.ts`:**

- Delete `readCompatibilityLooseObject()` (lines 61-120)
- Delete `maybeValidateShadowRead()` (lines 122-143)
- Simplify `readLooseObjectRaw()`: remove the `readCompatibilityLooseObject` fallback branch inside `loadPackedFirst()`, remove `compatLegacy`, remove `maybeValidateShadowRead()` calls. The function now only calls `readObject()` from the pack-first store.
- Remove the local `logOnce` helper (duplicate of the one in `object-store/support.ts`)
- Remove imports: `loadRepoStorageMode`, `validatePackedObjectShadowRead`

**`src/git/object-store/shadow.ts`** — delete entire module

**`src/git/object-store/catalog.ts`** — remove `loadRepoStorageMode()` (no read-path code queries mode anymore)

**`src/git/object-store/index.ts`** — remove `export * from "./shadow.ts"`

### Step 4: Remove `getObject` compatibility RPC

Depends on step 3 (sole production caller `readCompatibilityLooseObject` is now gone).

**`src/do/repo/repoDO.ts`** — remove `getObject()` method and its import from `storage.ts`

**`src/do/repo/storage.ts`** — delete `getObject()` function

### Step 5: Remove `loaderCap`/`loaderCalls` and shadow-read memo fields

Depends on step 3.

**`src/cache/cache.ts`** — remove from `RequestMemo`:

- `loaderCalls?: number` (line 47)
- `loaderCap?: number` (line 49)
- `repoStorageMode?: RepoStorageMode` (line 33) — no read path queries mode anymore
- `repoStorageModePromise?: Promise<RepoStorageMode>` (line 35) — same
- Remove the `RepoStorageMode` import

### Step 6: Keep legacy receive/alarm branches callable (rollback window)

**Key design decision**: unlike the previous plan revision, the legacy receive path in `src/routes/git.ts` and the unpack/hydration alarm branches in `src/do/repo/repoDO.ts` are NOT removed in the cutover release. This ensures that `legacy` mode is a real working rollback mode — an operator can set a repo to `legacy` and it immediately starts using the buffered receive and unpack/hydration pipeline, without requiring a full deploy rollback.

**`src/routes/git.ts` — `handleReceivePackPOST()`:**

- Keep the existing mode branch: `streaming` routes to `handleStreamingReceivePackPOST`, `legacy` routes to the DO `/receive` path
- Remove only the `shadow-read` reference (it was handled the same as `legacy`; now there's just `legacy`)
- Simplify the mode query: `getRepoStorageMode()` now returns `"legacy" | "streaming"` (no shadow-read case)

**`src/do/repo/repoDO.ts` — `alarm()`:**

- Keep the existing branch: `streaming` runs compaction re-arm, `legacy` runs `handleUnpackWork` and `handleHydrationWork`
- Remove only the `shadow-read` handling (it was in the `else` branch with `legacy`)
- Do NOT auto-clear stale unpack/hydration state — that state may be needed if the repo is rolled back to `legacy`

**`src/do/repo/repoDO.ts` — `fetch()`:**

- Keep the `/receive` route (line 142-144) — it's still needed when a repo is in `legacy` mode

**`src/do/repo/scheduler.ts`:**

- Keep the legacy `else` branch (lines 51-101) fully functional
- Remove `shadow-read` from the mode check (it was part of the `else` with `legacy`)

### Step 7: Simplify capability advertisement

**`src/git/core/protocol.ts`** — in the `git-receive-pack` branch:

- The `getRepoStorageMode()` call stays (it still determines whether to advertise `side-band-64k` and `quiet`, which only the streaming path supports)
- Remove `shadow-read` from the mode check: `supportsStreamingReceiveSideband = storageMode === "streaming"` (already correct, just clean up any shadow-read reference if present)

Note: we do NOT unconditionally advertise `side-band-64k`/`quiet` because repos in `legacy` mode during the rollback window still use the DO buffered path which does not support sideband. If we advertised sideband for legacy repos, clients would expect sideband framing and get raw bytes instead.

### Step 8: Delete compatibility-only tests

**Delete entirely** (test removed RPCs or shadow-read-only behavior):

1. `test/has-loose-batch.worker.test.ts` — tests removed RPC
2. `test/do-packoids-batch.worker.test.ts` — tests removed RPC
3. `test/progress-queued.worker.test.ts` — tests `getUnpackProgress` via a removed route-level caller
4. `test/object-store-shadow.worker.test.ts` — tests shadow-read validation
5. `test/packed-object-store.shadow.worker.test.ts` — tests shadow-read packed store

**Keep as rollback-window compatibility coverage:**

- `test/unpack-progress.worker.test.ts` — `getUnpackProgress()` is still a callable RPC during the rollback window. Keep to confirm the RPC works for legacy repos.
- `test/hydration-coverage-epochs.worker.test.ts` — hydration coverage logic is still used by the legacy path. Hydration packs help make the serving set complete for legacy fetch; the coverage helpers in `src/do/repo/hydration/helpers.ts` and the re-enqueue logic in `src/do/repo/maintenance.ts` are still callable for repos in `legacy` mode.
- `test/hydration-clear-deletes-packobjects.worker.test.ts` — `clearHydration()` is still a callable RPC via the admin `/hydrate` DELETE alias. Keeps the compatibility surface tested.

**Keep until closure release** (legacy paths are still callable):

- `test/receive-push.worker.test.ts` — tests legacy receive e2e (still callable for `legacy` repos)
- `test/receive-queue.worker.test.ts` — tests legacy unpack queueing (still runs for `legacy` repos)
- `test/fetch-during-unpack.worker.test.ts` — tests fetch during legacy unpack (still possible)
- `test/create-mem-pack-fs.test.ts` (AVA) — rollback-only pack/index helpers still exist
- `test/maintenance.worker.test.ts` — keep fully; hydration assertions still valid for `legacy` repos
- `test/calculate-stable-epochs.worker.test.ts` — covers logic still used by `src/do/repo/maintenance.ts` via `src/do/repo/packs.ts` for legacy repos
- `test/migrate-packkeys.worker.test.ts` — `src/do/repo/db/migrate.ts` still runs in the DO constructor, and the migration guide supports upgrades from pre-phase-1 commits

**Rewrite** (not delete):

- `test/multipack-union.worker.test.ts` — still valid multi-pack fetch coverage, but uses isomorphic-git toolchain. Rewrite to use streaming pack-first toolchain (pack catalog + idx views).
- `test/pack-first-read-path.storage-mode.worker.test.ts` — remove `shadow-read` test cases. Keep `legacy` and `streaming` transition tests for the two-mode contract.

### Step 9: Add new test coverage

**New tests or additions to existing test files:**

- **Empty repo streaming default**: new repo defaults to `streaming`, receives first push via streaming pipeline without manual mode flip, fetch works afterward
- **Shadow-read normalization**: repo with persisted `shadow-read` mode is normalized to `streaming` on first DO instantiation, with structured log
- **Empty-repo escape hatch**: truly empty repo (no refs, no packs, `packsetVersion === 0`) in `streaming` can transition to `legacy` without rollback backfill (and vice versa)
- **Non-empty zero-pack repo blocked**: a repo with refs but no active packs cannot transition `legacy → streaming` (the unsupported loose-only case)
- **Rollback compatibility**: one suite proving `legacy` mode still works for receive, unpack, and fetch after cutover deploy — keep `test/streaming-receive.rollback.worker.test.ts` and extend if needed

### Step 10: Rename `readLooseObjectRaw` → `readObjectRaw` (optional cleanup)

This is treated as optional post-behavioral-cutover cleanup. It adds import churn across many files for no risk reduction. If done, keep it as the last code change before docs.

**`src/git/operations/read/objects.ts`** — rename function, update JSDoc.

**Callers**: `tree.ts`, `commits.ts`, `objects.ts` (internal), plus test files.

If deferred: leave the rename for the closure release when the churn is already happening.

### Step 11: Update repoDO.ts class docstring and comments

Remove references to:

- `shadow-read` mode
- Loose-object writes as a primary path

Update to describe:

- Streaming receive is the default path
- Legacy receive/unpack/hydration remain callable for repos in `legacy` mode during the rollback window. Hydration packs (`pack-hydr-*`) and hydration coverage help make the serving set complete for legacy fetch — they are not merely deprecated admin baggage while `legacy` mode exists.
- The DO is metadata-authority; data plane lives in R2 packs

### Step 12: Write `MIGRATION-STREAMING-PUSH.md`

**New file: `MIGRATION-STREAMING-PUSH.md`** (repo root, not `docs/`, for operator visibility)

Operator runbook. Structure:

#### Section 1: Upgrade Path by Starting Version

| Upgrading from                               | Required steps                                                                                    |
| -------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| Before `c202a4a` (pre-phase-1)               | Deploy phases 1-4 first (`a76650c`). Run repos through phase 1-4 validation gates before cutover. |
| Between `c202a4a` and `a76650c` (phases 1-3) | Deploy `a76650c` (phase 4) first. Verify compaction works. Then deploy cutover.                   |
| At `a76650c` (phase 4, current)              | Deploy cutover directly.                                                                          |

Warn: do NOT skip intermediate phases. Each validates correctness for the next:

- Phase 1 seeds `pack_catalog` and validates pack-first reads
- Phase 2 cuts over all read paths to pack-first
- Phase 3 enables streaming receive for canary repos
- Phase 4 enables queue-driven compaction

#### Section 2: Cutover Deploy

1. Deploy the cutover commit
2. New repos automatically default to `streaming`
3. Repos previously on `shadow-read` auto-normalize to `streaming` on next DO access (logged)
4. Inventory existing repos by mode: `GET /:owner/:repo/admin/storage-mode`
5. For `legacy` repos with active packs: `PUT` with `{"mode": "streaming"}`
6. For `legacy` repos that need break-glass rollback: first run `POST /admin/storage-mode/backfill`, poll `GET /admin/storage-mode` until `rollbackCompat.status === "ready"` (backfill is async), then `PUT` to `streaming`
7. **Truly empty repos** (no refs, no packs, `packsetVersion === 0`): `PUT` directly to `streaming`, or leave on `legacy` — they return 503 on fetch regardless because there's nothing to serve
8. **Unsupported: non-empty zero-pack repos** (refs exist but no active packs — e.g., loose-only repos from non-standard seeding): these cannot be promoted to `streaming`. The admin `PUT /admin/storage-mode` endpoint will reject the transition with a blocker message. The operator must either push a pack to the repo first (via `git push` while in `legacy` mode, which creates a pack) or purge and recreate the repo.
9. **Cold repos with no stored mode key**: existing repos that predate the `repoStorageMode` key will default to `legacy` if they have any data signal (`packsetVersion > 0`, refs, `lastPackKey`, or `packList`), or `streaming` if truly empty. This prevents silent auto-promotion. Operators must manually promote data-bearing repos.
10. Note: repos in `legacy` mode still function normally (receive, fetch, unpack). Legacy is a working mode, not just a label.

#### Section 3: Rollback Procedure

Per-repo rollback does not require a full deploy rollback:

1. For affected repos: `POST /admin/storage-mode/backfill` to queue rollback compatibility data preparation. This backfills `obj:*` and `pack_objects` from active packs so the legacy path can serve correctly. Backfill is async (batched via the maintenance queue) — poll `GET /admin/storage-mode` until `rollbackCompat.status === "ready"` before proceeding.
2. `PUT /admin/storage-mode` with `{"mode": "legacy"}`
3. The repo immediately starts using legacy receive/unpack/hydration paths. Hydration packs (`pack-hydr-*`) and hydration coverage state remain part of the legacy compatibility surface — they help make the serving set complete for legacy fetch. The legacy alarm will schedule hydration work as needed.
4. Full deploy rollback to `a76650c` is available as a last resort but not required for per-repo rollback.

#### Section 4: Closure Deploy (future)

Operator-facing summary of what changes when the rollback window closes:

1. Confirm all repos stable on `streaming` for the window duration
2. Deploy the closure release commit
3. `/admin/hydrate`, `/admin/storage-mode/backfill`, and `PUT /admin/storage-mode` are removed
4. Rollback to pre-streaming is no longer possible
5. All repos are implicitly streaming

### Step 13: Update documentation

**`README.md`:**

- Replace "Time-budgeted background unpacking" with streaming push description
- Remove unpack-as-primary-mechanism language
- Add upgrade notice: "If upgrading from a commit before `a76650c`, read `MIGRATION-STREAMING-PUSH.md` for the required deployment sequence."

**`AGENTS.md`:**

- "Validation By Change Type": replace "hydration" with "compaction"
- Update `repoDO.ts` description in First Files To Read
- Core Invariants: replace "Receive-pack queuing is intentionally bounded: one active unpack plus one queued" with streaming receive lease model
- Avoid section: remove "Converting streaming paths to buffered implementations"

**`docs/streaming-push.md`:**

- Add "Phase 5 cutover completed" status note at the top
- Append a "Closure Release Implementation Checklist" section (agent-facing) with the full task list:
  1. Remove emergency backfill tool: `src/maintenance/legacyCompatBackfill.ts`, `src/do/repo/catalog/legacyCompat.ts`, backfill RPCs, `POST /admin/storage-mode/backfill`, `LegacyCompatBackfillState`, `test/streaming-receive.rollback.worker.test.ts`
  2. Remove `/admin/hydrate` aliases and `startHydration`/`clearHydration` RPCs
  3. Remove `mirrorLegacyPackKeys()` and `lastPackKey`/`lastPackOids`/`packList` from `RepoStateSchema`
  4. Remove `REPO_UNPACK_*` from `wrangler.jsonc`, `repoConfig.ts`, `vitest.bindings.ts`; run `npm run cf-typegen`
  5. Delete `src/do/repo/unpack.ts`, `src/do/repo/repoDO/receive.ts`, `src/do/repo/repoDO/hydration.ts`, `src/do/repo/hydration/`, `src/git/operations/receive.ts`, `src/git/pack/loose-loader.ts`, `src/git/pack/unpack.ts` (rewrite `seeding.ts` first)
  6. Remove `unpackWork`, `unpackNext`, `hydrationWork`, `hydrationQueue` and types from `RepoStateSchema`
  7. Drop `pack_objects`, `hydr_cover`, `hydr_pending` tables via `npm run db:gen`
  8. Remove `isomorphic-git` from `package.json` and `vitest.config.ts`
  9. Remove `RepoStorageMode` type and all storage-mode RPCs/admin endpoints
  10. Delete remaining legacy tests: `receive-push`, `receive-queue`, `fetch-during-unpack`, `create-mem-pack-fs`, `calculate-stable-epochs`, `migrate-packkeys`, `unpack-progress`, `hydration-coverage-epochs`, `hydration-clear-deletes-packobjects`, `maintenance` hydration assertions
  11. Final docs cleanup: mark closure complete, remove all legacy references

**`docs/architecture.md`** and **`docs/data-flows.md`:**

- Update receive path description from buffered to streaming as the default
- Mark unpack/hydration as rollback-window-only in data flow descriptions
- Mark older rollout-context sections as archival

**`docs/caching.md`:**

- Remove references to `loaderCap`/`loaderCalls`
- Remove references to `getPackLatest` as the former seeding path

**`docs/api-endpoints.md`:**

- Update receive-pack endpoint description
- Note `/admin/hydrate` as compatibility alias (rollback window only)

**`docs/storage.md`:**

- Update storage model to describe streaming as the default path

---

## Verification

After all steps:

```bash
npm run typecheck
npm run test          # AVA unit tests
npm run test:workers  # Vitest worker integration tests
npm run test:auth     # Auth-specific tests
npm run format:check
```

Confirm:

- No PRIMARY correctness test exercises shadow-read validation (shadow-read plumbing is deleted)
- Kept hydration/unpack tests (`hydration-coverage-epochs`, `hydration-clear-deletes-packobjects`, `unpack-progress`, `receive-push`, `receive-queue`, etc.) are rollback-path compatibility coverage, not primary correctness tests — they prove the legacy rollback mode works, not the streaming path
- `repoDO.ts` remains a thin delegator
- The streaming receive path handles empty repos correctly
- New repos default to `streaming` without manual intervention
- Repos previously on `shadow-read` auto-normalize to `streaming`
- Legacy mode is still functional for repos explicitly set to `legacy` (rollback story works)
- `test/receive-push.worker.test.ts` and `test/receive-queue.worker.test.ts` still pass (legacy paths callable)
- `getUnpackProgress` RPC stays (still called by the legacy branch in `src/routes/git.ts:231`)

---

## Closure Release (future session)

NOT implemented in this session. The agent-facing implementation checklist is appended to `docs/streaming-push.md` (step 13). The operator-facing closure steps are in Section 4 of `MIGRATION-STREAMING-PUSH.md` (step 12).
