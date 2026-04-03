# Modernize Fetch/Upload-Pack: Ordered Pack Snapshot + Shared Rewrite Core

## Context

Phase 3 of the streaming-push work already introduced the right low-level
building blocks:

- `IdxView` with typed arrays and byte-bounded caching
- pack-first object lookup and object reads in Worker code
- the newer streaming receive pipeline and pack indexer
- iterative, bounded-memory processing instead of large ad hoc Maps

The fetch/upload-pack path still predates that work. It still has its own idx
parsing stack, its own pack metadata layer, multiple fetch-only serving modes,
and compatibility fallback branches that should no longer be part of fetch
correctness.

This proposal replaces the current fetch assembler stack with one ordered
pack-snapshot rewrite pipeline that:

- serves fetch immediately
- stays correct when loose compatibility data is deleted
- reuses the pack-first object store
- is shaped so Phase 4 compaction can reuse the same rewrite core

No new schema, sidecar metadata, or env vars are introduced.

## Pack Ordering Contract

The rewrite engine receives a caller-ordered snapshot and must honor that order
for duplicate-object selection and stable tie-breaking.

- Fetch passes active packs in the exact order returned by the active pack
  catalog snapshot.
- Today that order is `seqHi DESC, tier DESC` for active rows.
- Phase 4 compaction will pass `[source packs..., remaining active packs...]`.

This contract matters because duplicate selection must stay deterministic and
because compaction needs explicit control over which pack wins when the same
object appears in multiple sources.

## Reader Reuse Decision

Do not invent a third unrelated pack-read strategy.

The new rewrite engine should follow the same buffered range-read policy already
used by the newer pack-indexer resolve reader:

- preload sequential chunks when locality exists
- provide direct `readRange()` for exact spans
- provide `readWindow()` for progressive reads without double buffering

In this fetch refactor, the rewrite path should start with that policy instead
of the current fetch-only `groupCache` design.

If measurements later show that fetch needs a stronger byte-budgeted multi-window
cache, add it as a narrow optimization after correctness is landed. Do not make
that extra cache part of the first correctness rewrite.

## Goals

- Do more with less code.
- Delete duplicated idx parsing and fetch-only pack metadata structures.
- Keep protocol v2 behavior, route behavior, and response semantics stable.
- Preserve request limiter and subrequest accounting.
- Keep Worker to DO to one metadata hop for the active catalog snapshot, then
  Worker to R2 only for fetch serving.
- Make fetch correctness depend only on the active pack catalog plus R2 packs.
- Produce one serving core that Phase 4 compaction can reuse.

## Non-Goals

- Do not change Git protocol semantics.
- Do not restore loose-only fetch.
- Do not redesign UI or unrelated read paths in this pass.
- Do not remove legacy receive or hydration systems beyond the narrow migrations
  required by idx-cache deletion and `IdxView` adoption.
- Do not rename `readLooseObjectRaw()` in this pass.
- Do not add new database tables, catalog fields, or storage-mode flags.

## Current Problems

1. The fetch path still carries multiple plan and serving branches:
   - `InitCloneUnion`
   - `IncrementalSingle`
   - `IncrementalMulti`
   - dead buffered-mode leftovers

2. The current assembler duplicates work that already has a better replacement:
   - `src/git/pack/idxCache.ts`
   - `src/git/pack/packMeta.ts:parseIdxV2()`
   - per-pack `Map<string, number>` and `Map<number, number>` state in
     `src/git/pack/assemblerStream.ts`

3. Fetch closure planning still has compatibility fallback reads:
   - mainline enrichment in `src/git/operations/fetch/neededFast.ts`
   - missing-ref fallback in `src/git/operations/fetch/neededFast.ts`
   - `findCommonHaves()` fallback in `src/git/operations/closure.ts`

4. Initial clone and closure-timeout fallback paths can inflate fetch scope from
   the actual closure result to a full pack union.

5. `src/git/object-store/store.ts:readObjectRefsBatch()` still walks objects
   serially even though the new object-store path is otherwise pack-first.

6. Hydration still depends on the old parsed-idx shape in:
   - `src/do/repo/hydration/status.ts`
   - `src/do/repo/hydration/stages/scanDeltas.ts`

7. The current assembler shape is fetch-specific and not a good Phase 4
   compaction surface.

## Files To Delete

| File                              | Why                                                                                                 |
| --------------------------------- | --------------------------------------------------------------------------------------------------- |
| `src/git/pack/idxCache.ts`        | Duplicate idx cache; `loadIdxView()` already provides the needed cache and request-local coalescing |
| `src/git/pack/assemblerStream.ts` | Replaced by `rewrite.ts`                                                                            |
| `src/git/operations/heavyMode.ts` | Only exists to shape compatibility loose fallback behavior during closure                           |

## Files To Create

| File                      | Purpose                                                       |
| ------------------------- | ------------------------------------------------------------- |
| `src/git/pack/rewrite.ts` | Shared pack rewrite engine for fetch now and compaction later |

## Core Types

```ts
type OrderedPackSnapshot = {
  packs: {
    packKey: string;
    packBytes: number;
    idx: IdxView;
  }[];
};

type UploadPackPlan =
  | {
      type: "Serve";
      repoId: string;
      snapshot: OrderedPackSnapshot;
      neededOids: string[];
      ackOids: string[];
      signal?: AbortSignal;
      cacheCtx?: CacheContext;
    }
  | {
      type: "RepositoryNotReady";
    };
```

This replaces `InitCloneUnion`, `IncrementalSingle`, and `IncrementalMulti`.

There should be no public `serveMode` field. Fast-path passthrough decisions
belong inside the rewrite engine after it has already selected the actual object
set.

## High-Level Flow After Refactor

1. `handleFetchV2Streaming()` parses the request and keeps the current sideband
   response shape.
2. The planner loads the active catalog once and eagerly resolves an ordered
   snapshot with `packBytes` and `IdxView`.
3. The planner computes:
   - `ackOids` from pack-first membership checks
   - `neededOids` from pack-first closure only
   - initial-clone unions directly from eager `IdxView`s

4. `resolvePackStream()` calls one rewrite entrypoint with the ordered snapshot
   and `neededOids`.
5. The rewrite engine emits a valid PACK stream:
   - select needed objects from ordered source packs
   - pull in delta bases
   - topologically order base before dependent
   - rewrite OFS distances until stable
   - stream payload bytes from R2 with bounded memory

## Detailed File Plan

### 1. `src/git/operations/fetch/types.ts`

Replace the current plan union with `OrderedPackSnapshot` plus `UploadPackPlan`.

Required changes:

- remove `InitCloneUnion`, `IncrementalSingle`, and `IncrementalMulti`
- add `OrderedPackSnapshot`
- add `UploadPackPlan`
- keep `ackOids`, `signal`, and `cacheCtx` on the serve plan

Why:

- the old branches are implementation artifacts, not meaningful protocol states
- a single serve plan makes fetch and compaction share the same serving contract

### 2. `src/git/operations/fetch/plan.ts`

Rewrite the planner around `OrderedPackSnapshot`.

Required changes:

1. Load active catalog rows directly with `loadActivePackCatalog()`.
2. If the active catalog is empty, return `RepositoryNotReady`.
3. For each active row:
   - call `loadIdxView(env, packKey, cacheCtx, packBytes)`
   - build one snapshot entry with `packKey`, `packBytes`, and `idx`

4. Keep active catalog order as the authoritative pack preference order.
5. Reuse request-local memo state so later object-store calls see the same
   `packCatalog` and `idxViews`.
6. Do not route the serving plan through `getPackCandidates()`.

Implementation footgun:

The planner is the one place that has both `packKey` and `packBytes`. If it
falls back to `getPackCandidates()`, later `loadIdxView()` calls lose the size
hint and pay avoidable `head()` reads.

Initial clone path:

- if `haves.length === 0`, enumerate the union directly from eager `IdxView`s
- iterate `idx.count` and materialize OIDs with `getOidHexAt()`
- deduplicate with `Set<string>`
- delete `buildUnionNeededForKeys()`
- delete `countMissingRootTreesFromWants()`

Incremental path:

- call `computeNeededFast()` using only the pack-first object store
- remove `beginClosurePhase()` and `endClosurePhase()`
- if closure times out, return the partial `neededOids` result instead of
  upgrading to a full union

Negotiation:

- compute `ackOids` with `findCommonHaves()` only when needed
- keep the current `done ? [] : ackOids` behavior

Logging:

- keep the current summary style
- add one snapshot summary log with pack count, idx loads, cheap hit/miss
  counters, and total indexed object count when cheap to compute

### 3. `src/git/operations/closure.ts`

Trim this module to the parts fetch still needs.

`findCommonHaves()`:

- keep the 128-have cap
- keep the same return shape
- remove the fallback loop that calls `readLooseObjectRaw()`
- rely only on `hasObjectsBatch()`

Delete:

- `buildUnionNeededForKeys()`
- `countMissingRootTreesFromWants()`

Potential follow-up deletion:

- `iterPackOids()` if nothing else still uses it

Why:

- these helpers only exist to support old fetch planning branches
- fetch negotiation should no longer cross into compatibility object reads

### 4. `src/git/operations/fetch/neededFast.ts`

Convert closure planning to fully pack-first behavior.

Required changes:

1. Remove the compatibility fallback block that calls `readLooseObjectRaw()` for
   missing refs.
2. Remove `heavyMode` integration and `loader-capped` handling.
3. Keep timeout flagging so callers can still detect partial closure.
4. Preserve the stop-set and mainline optimization, but make mainline
   enrichment pack-first.

Mainline enrichment:

- replace `readLooseObjectRaw()` with pack-first reads
- reuse the object store, not a second ad hoc reader
- preserve the current guard and budget shape unless it is intentionally
  remeasured:
  - only run when `ackOids.length > 0 && ackOids.length < 10`
  - max 20 mainline steps
  - stop after about 2 seconds

Batching and memoization:

- keep queue batching at 128 logical OIDs
- keep request-local refs memo behavior
- continue omitting missing objects from the refs map rather than throwing

Failure behavior:

- missing objects remain omitted
- closure does not fall back to compatibility loose data
- timeout returns the partial closure result already accumulated

Why:

- this removes the last fetch correctness dependence on compatibility reads

### 5. `src/git/object-store/store.ts`

Make `readObjectRefsBatch()` bounded-parallel.

Required changes:

- process OIDs in bounded batches using `MAX_SIMULTANEOUS_CONNECTIONS`
- within each batch, use `Promise.all()` over `readObject()`
- preserve current semantics:
  - missing objects remain omitted
  - commit/tree/tag/leaf handling stays the same
  - request-local memoization still comes from `readObject()`

Why:

- the current serial walk is a bottleneck on large closure traversals

### 6. `src/git/pack/rewrite.ts`

Create the shared rewrite engine that replaces `assemblerStream.ts`.

Public entrypoint:

```ts
export async function rewritePack(
  env: Env,
  snapshot: OrderedPackSnapshot,
  neededOids: string[],
  options?: {
    signal?: AbortSignal;
    limiter?: { run<T>(label: string, fn: () => Promise<T>): Promise<T> };
    countSubrequest?: (n?: number) => void;
    onProgress?: (msg: string) => void;
  }
): Promise<ReadableStream<Uint8Array> | undefined>;
```

Internal design:

- use the ordered snapshot directly
- do not reload pack metadata through a second fetch-only stack
- keep selection and layout state in typed arrays
- allow small targeted lookup state where it improves readability, but do not
  rebuild full per-pack OID and offset Maps that `IdxView` already replaced

Suggested per-selection state:

```ts
const packSlot = new Uint8Array(capacity);
const entryIndex = new Uint32Array(capacity);
const offset = new Float64Array(capacity);
const nextOffset = new Float64Array(capacity);
const typeCode = new Uint8Array(capacity);
const origHeaderLen = new Uint16Array(capacity);
const baseSlot = new Int32Array(capacity);
const sizeVarBuf = new Uint8Array(capacity * 5);
const sizeVarLen = new Uint8Array(capacity);
const outHeaderLen = new Uint16Array(capacity);
const outOffset = new Float64Array(capacity);
```

#### Rewrite Phases

1. Select objects.
   - for each `neededOid`, search packs in snapshot order with `findOidIndex()`
   - first pack wins on duplicates
   - deduplicate by `(packSlot, entryIndex)`, not by OID string
   - include all required delta bases
   - for `OFS_DELTA`, resolve base by offset within the same pack
   - for `REF_DELTA`, resolve base OID by searching the ordered snapshot

2. Detect passthrough.

   After selection, if:
   - the snapshot has exactly one pack
   - every object in that pack is selected
   - no header rewrite is needed

   then stream the existing `.pack` body from R2, strip the old 20-byte trailer,
   hash the bytes as they stream, and emit a fresh trailer.

   Keep this decision inside the engine, not in the planner.

3. Read headers in batches.
   - sort selected entries by `(packSlot, offset)`
   - coalesce header reads instead of fetching headers one at a time
   - whole-pack preload remains valid for small packs
   - reuse `readPackHeaderExFromBuf()` when whole-pack bytes are already in
     memory

4. Topologically order output.
   - base before dependent
   - stable tie-break by snapshot order first, then source offset
   - on incomplete ordering or cycle, log and fail the request

5. Converge output header lengths.
   - preserve compressed payload bytes unchanged
   - recompute OFS distances with `encodeOfsDeltaDistance()`
   - iterate until header lengths stabilize or a fixed sanity cap is reached
   - if convergence fails, log and fail the request

6. Stream the output.
   - emit PACK header
   - emit rewritten entry headers and original compressed payloads
   - emit SHA-1 trailer
   - keep sideband outside the rewrite engine

#### Read Policy Inside `rewrite.ts`

Do not reintroduce the old fetch-only `groupCache` as the default design.

Instead:

- keep a per-pack buffered reader with the same chunked preload and `readWindow`
  behavior already proven in the newer pack-indexer resolve path
- use exact `readRange()` for large spans
- keep whole-pack preload for small packs

If a second-stage optimization is needed after correctness lands, add a
byte-budgeted per-pack window cache and measure it explicitly. That should be an
optimization patch, not part of the first correctness rewrite.

Limiter and subrequest accounting:

- every R2 read path inside the engine must respect the request limiter
- every R2 read path must account through `countSubrequest()`
- preserve the existing one-shot soft-budget warning style

Why:

- this gives fetch and compaction one serving core
- it deletes duplicated idx parsing and most fetch-only metadata code
- it stays aligned with the newer streaming push patterns

### 7. `src/git/operations/fetch/execute.ts`

Collapse this module to a thin wrapper around the rewrite engine.

Required changes:

- delete single-pack versus multi-pack dispatch
- delete fallback-from-single-to-multi retry
- call `rewritePack(env, plan.snapshot, plan.neededOids, options)`

Why:

- the planner already has the ordered snapshot
- the rewrite engine should be the only serving path

### 8. `src/git/operations/uploadStream/index.ts`

Keep this as the protocol entrypoint, but simplify it.

Required changes:

- remove the early `getPackCandidates()` preflight
- remove the re-export of `computeNeededFast`
- keep:
  - request parsing
  - negotiation-only response path
  - sideband muxing
  - fatal response behavior
  - `499` handling on abort

Behavior to preserve:

- no buffered mode
- no `X-Git-Streaming` switching
- same `repositoryNotReadyResponse()` behavior when no active packs exist

### 9. `src/git/operations/fetch/protocol.ts`

Delete the dead buffered helper:

- `respondWithPacketizedPack()`

Keep:

- `buildAckSection()`
- `buildAckOnlyResponse()`

Update tests that still import the deleted helper.

### 10. `src/git/pack/packMeta.ts`

Trim this module to low-level pack helpers still used elsewhere.

Keep:

- `readPackHeaderEx()`
- `readPackHeaderExFromBuf()`
- `readPackRange()`
- `encodeOfsDeltaDistance()`
- `mapWithConcurrency()` if it still has callers

Delete:

- `IdxParsed`
- `PackMeta`
- `loadPackMeta()`
- `parseIdxV2()`
- `readUint64BE()` if it becomes unused

Why:

- `IdxView` is the shared idx representation now
- these legacy shapes only exist to support the old fetch assembler and old
  hydration callers

### 11. Hydration callers

Migrate off `loadIdxParsed()` in:

- `src/do/repo/hydration/status.ts`
- `src/do/repo/hydration/stages/scanDeltas.ts`

Required changes:

- replace `loadIdxParsed()` with `loadIdxView()`
- update `buildPhysicalIndex()` so it can work from a narrow `IdxView`-based
  input
- use `getOidHexAt()` only where hex materialization is actually needed
- use `findOidIndex()` for `REF_DELTA` lookup instead of rebuilding full OID
  arrays for search

Important:

This is not a purely mechanical swap. `buildPhysicalIndex()` currently expects
plain string-array OIDs and offset arrays, so the helper needs to be reshaped.

### 12. `src/git/pack/index.ts`

Update exports carefully.

Required changes:

- export `rewrite.ts` instead of `assemblerStream.ts`
- keep current exports that still have live callers:
  - `unpack.ts`
  - `loose-loader.ts`
  - `packMeta.ts`
  - `build.ts`
  - `indexer/index.ts`

Do not drop `unpack.ts` or `loose-loader.ts` from the barrel in this pass.

### 13. File deletions after cutover

Once the rewrite engine is wired in and validated, delete:

- `src/git/pack/assemblerStream.ts`
- `src/git/pack/idxCache.ts`
- `src/git/operations/heavyMode.ts`

## Deleted Or Simplified Paths

After the refactor, remove or simplify:

- `src/git/pack/assemblerStream.ts`
- `src/git/pack/idxCache.ts`
- `src/git/operations/heavyMode.ts`
- `respondWithPacketizedPack()`
- `buildUnionNeededForKeys()`
- `countMissingRootTreesFromWants()`
- fetch-time compatibility fallback in `findCommonHaves()`
- fetch-time compatibility fallback in `computeNeededFast()`

Potential follow-up deletion if it becomes unused:

- `iterPackOids()`

## Logging

Follow existing logging style:

- compact summary counters
- one-shot soft-budget warnings
- no logging tests

Useful new summary points:

- snapshot load:
  - pack count
  - idx loads
  - cheap cache hits versus misses
  - total indexed objects when cheap

- rewrite selection:
  - requested OIDs
  - selected entries
  - added delta bases

- streaming:
  - whole-pack hits
  - buffered-reader cache hits versus misses if cheap
  - direct range fallbacks
  - total time

## Test Plan

### Existing tests to keep green

- negotiation and ack behavior
- streaming pack response
- gzip request body handling
- fetch while receive or unpack work overlaps
- pack-first fetch after loose deletion

### Existing tests to update

- `test/fetch-streaming.worker.test.ts`
  - remove the buffered-mode test
  - remove `X-Git-Streaming` from remaining fetch tests

- `test/upload-pack-acks.test.ts`
  - stop importing `respondWithPacketizedPack()`

- `test/pack-indexer.resolve.ofs.worker.test.ts`
  - switch from `parseIdxV2()` to `parseIdxView()` plus `getOidHexAt()`

- tests that import `computeNeededFast` from `uploadStream/index.ts`
  - import from `fetch/neededFast.ts` directly if they still need the symbol

### New tests to add

1. Single-pack fetch with all loose objects deleted.
2. Multi-pack incremental fetch where required bases span multiple packs.
3. Duplicate-object selection honoring active catalog order.
4. Cross-pack `REF_DELTA` rewrite correctness.
5. `OFS_DELTA` rewrite correctness with header-length convergence.
6. Single-pack passthrough fast path.
7. Mid-stream abort preserving current `499` and sideband fatal behavior.
8. Closure timeout returning the partial closure result instead of silently
   upgrading to full union.
9. Rewrite contract for future compaction:
   - ordered source packs first
   - remaining active packs second
   - only required external bases included

10. Differential correctness gate while both engines exist:

- run the new `rewrite.ts` and old `assemblerStream.ts` on the same logical
  inputs
- compare pack validity and object coverage
- use this as a temporary landing gate before deleting the old assembler

## Validation Commands

Minimum validation for this refactor:

```bash
npm run typecheck
npx vitest run --config vitest.config.ts test/fetch-streaming.worker.test.ts
npx vitest run --config vitest.config.ts test/pack-first-fetch-and-ui.worker.test.ts
npx vitest run --config vitest.config.ts test/fetch-during-unpack.worker.test.ts
npx vitest run --config vitest.config.ts test/pack-first-read-path.closure.worker.test.ts
npx vitest run --config vitest.config.ts test/upload-pack-content-encoding.worker.test.ts
```

Additional targeted checks:

```bash
npx vitest run --config vitest.config.ts test/receive-push.worker.test.ts
npx vitest run --config vitest.config.ts test/streaming-receive.worker.test.ts
npx vitest run --config vitest.config.ts test/pack-indexer.resolve.ofs.worker.test.ts
npx ava test/object-parse.test.ts
npx ava test/ofs-delta-encode.test.ts
npx ava test/ofs-delta-known-encodings.test.ts
```

Full validation before landing:

```bash
npm run typecheck
npm run test
npm run test:workers
npm run format:check
```

## Recommended Implementation Order

1. Introduce `OrderedPackSnapshot` and the simplified serve-plan types.
2. Rewrite the planner to load the active catalog and eager `IdxView`s directly,
   but keep the current assembler temporarily so the surface area shrinks first.
3. Remove closure compatibility fallbacks and make `readObjectRefsBatch()`
   bounded-parallel.
4. Implement `src/git/pack/rewrite.ts`.
5. Add a temporary differential test that compares the new rewrite path against
   `assemblerStream.ts` on the same inputs while both still exist.
6. Wire `resolvePackStream()` to the new rewrite engine.
7. Delete `assemblerStream.ts`, `idxCache.ts`, `heavyMode.ts`, and the dead
   buffered helper.
8. Trim `packMeta.ts`.
9. Migrate hydration callers to `IdxView`.
10. Update tests and run full validation.

## Acceptance Criteria

- Fetch correctness depends only on the active pack catalog plus R2 packs.
- No fetch correctness path depends on compatibility loose objects.
- The planner loads the active catalog once and each idx once per request.
- The planner does not route serving-path planning through
  `getPackCandidates()`, because `packBytes` must be preserved for hinted
  `loadIdxView()` calls.
- Single-pack and multi-pack fetch use the same rewrite engine.
- Duplicate-object choice follows active catalog order.
- Mainline enrichment in `computeNeededFast()` keeps the current guard and
  budget shape unless it is intentionally remeasured and changed.
- The rewrite engine is reusable for Phase 4 compaction by passing a different
  ordered snapshot.
- Dead fetch-specific idx parsing and buffered-mode code is removed.
- The new rewrite path uses the same buffered pack-read policy as the newer
  pack-indexer resolve path, unless later measurement proves a stronger cache is
  necessary.
