# Streaming Push and R2-First Repository Design

## Status

This document specifies the replacement for the current unpack-and-hydrate repository pipeline.

The new design makes R2 `.pack` and `.idx` objects the only required source of truth for Git object data. The repository Durable Object remains the source of truth for metadata only: refs, HEAD, pack catalog state, receive/compaction leases, and rollout state.

The design is intentionally conservative about complexity:

- No Git protocol changes.
- No repo metadata stored in R2.
- No exact `pack_objects(pack_key, oid)` correctness dependency.
- No correctness dependency on DO `obj:*` or R2 loose mirrors.
- No new distributed transaction.
- No new tunable env vars beyond a single Queue binding.
- No object-level garbage collection in the initial rollout.

## Why This Exists

The current implementation buffers the entire receive-pack request body in memory, indexes through `isomorphic-git` over an in-memory filesystem, then unpacks to loose objects in the Durable Object and mirrors them to R2. That model does not scale and violates the intended ownership boundary:

- the DO is doing data-plane work instead of metadata-plane work;
- correctness depends on loose object materialization;
- fetch/object reads still depend on loose-object semantics;
- exact pack membership is stored in SQLite even though R2 `.idx` already contains the authoritative mapping;
- background hydration is a second correctness mechanism layered on top of unpacking.

The new model replaces all of that with immutable packs in R2, worker-local range reads, and compact metadata in the DO.

## Goals

1. Streaming-first receive path with bounded memory.
2. R2-first data model: packs in R2 are authoritative for object data.
3. DO metadata-only correctness boundary.
4. No externally visible Git protocol changes.
5. Existing packed repos continue to work.
6. Existing repos can lose all loose objects and still remain correct.
7. Fetch becomes naturally pack-first and streaming.
8. Heavy lifting moves out of the DO and into Workers/Queue consumers.
9. Rollout remains staged and reversible.

## Non-Goals and Deliberate Simplifications

These are intentional, not omissions:

1. The rollout does not implement full Git garbage collection of unreachable objects. Compaction preserves the union of source-pack objects. This matches current retention behavior more closely and avoids a repo-wide mark phase in the first release.
2. The fetch/read path loads whole `.idx` files into memory when needed. This is acceptable because the representative `.idx` is about 3 MB while the representative `.pack` is about 42 MB. The constraint is “do not buffer the pack”, not “never buffer an idx”.
3. Push concurrency is simplified to one active receive lease per repo. The current one-active plus one-queued unpack model is removed because unpacking is removed. Returning `503 Retry-After: 10` to concurrent pushes is Git-compatible and materially simpler.
4. UI raw/blob reads may buffer an individual packed object when it is delta-resolved and not already present in the optional loose cache. This is acceptable because the streaming-first requirement is about pack ingress/egress paths, not ad hoc UI reads; the UI already enforces size caps.
5. Bloom filters are not introduced initially. Active pack count is bounded by compaction, and the `.idx` fanout table already gives an exact, compact lookup structure.

## Source of Truth

### Data plane

Required for correctness:

- `do/<do-id>/objects/pack/*.pack`
- `do/<do-id>/objects/pack/*.idx`

Optional caches only:

- DO `obj:*`
- R2 loose mirrors under `do/<do-id>/objects/loose/*`

### Metadata plane

Required for correctness:

- DO KV: `refs`, `head`, `refsVersion`, `packsetVersion`, `nextPackSeq`, receive/compaction leases, rollout mode
- DO SQLite: active/superseded pack catalog

Legacy data that must remain on disk for rollback and emergency backfill, but is not required by the new design:

- `pack_objects`
- `hydr_cover`
- `hydr_pending`
- `lastPackOids`
- `unpackWork`
- `unpackNext`
- `obj:*`

## Key Design Decisions

### 1. `.idx` is treated as data-plane state, not repo metadata

Refs, HEAD, pack catalog, leases, and rollout mode remain in the DO. Standard Git `.idx` files live next to `.pack` files in R2 because they are required to address immutable pack contents efficiently. No JSON manifests or repo catalog records are stored in R2.

### 2. Receive writes pack data first, commits metadata last

The Worker only publishes a new pack to the repo catalog after:

- the `.pack` object is fully written to R2;
- the `.idx` is successfully built and written to R2;
- pack integrity and thin-pack base validation pass;
- ref/update validation passes.

This avoids a distributed transaction. R2 objects are immutable blobs; the DO metadata only points to fully written pack pairs.

### 3. Fetch and object reads stop depending on loose objects

All fetch planning, object existence checks, commit/tree/tag/blob reads, raw/blob views, diff, and merge-history traversal must use a shared pack-first object store in Worker code. DO object RPCs become compatibility shims or are removed from callers.

### 4. Hydration is replaced by queue-driven pack compaction

The system no longer unpacks to loose objects or builds “hydration packs”. It keeps a bounded set of immutable active packs and periodically compacts older packs into larger packs in a background Queue consumer.

### 5. Compaction is tiered, not full-repo repack

The initial compaction strategy is LSM-like:

- receive packs enter tier 0;
- when a tier exceeds the fixed fan-in, the oldest packs in that tier are merged into one pack in the next tier;
- active pack count remains bounded;
- the algorithm does not require a full-repo rewrite on each push.

This is simpler than full snapshot compaction and good enough for the target workload.

## Assumptions

1. The representative push remains within the Worker request body limit. The supplied sample is about 42 MB, which is under the documented minimum 100 MB request-body limit on paid Cloudflare plans.
2. Workers and Durable Objects run with a 128 MB memory limit; request/response streaming is available; R2 `put()` accepts a `ReadableStream`; R2 ranged reads are strongly supported in Workers; Queue consumers have 15 minutes of wall time.
3. Active pack count is kept low by compaction, so loading a handful of `.idx` files into memory is safe and simpler than inventing a second metadata structure.
4. Existing repos that already have packs in R2 are the primary migration case. Loose-only repos need an explicit one-time pack migration before they can be switched to the new correctness path.

## Cloudflare Constraints Used By The Design

These are the platform facts the design relies on:

- Workers and Durable Objects have a 128 MB memory limit.
- HTTP request handling has no hard wall-time limit while the client stays connected.
- Queue consumers have a 15 minute wall-time limit.
- Workers Paid supports up to 10,000 subrequests per invocation and up to six simultaneous outgoing connections.
- R2 `put()` is strongly consistent and accepts a `ReadableStream`.
- R2 `get()` supports ranged reads by `offset` and `length`.
- R2 multipart uploads exist, but require 5 MiB parts except the last part.

The design deliberately does not use Worker-to-R2 multipart upload in the initial rollout because it adds state management and part assembly complexity with no material benefit for the 40 MB target pack.

## New Metadata Model

### New SQLite table: `pack_catalog`

Add a new table via `src/do/repo/db/schema.ts` and DAL helpers in `src/do/repo/db/dal.ts`.

Required columns:

- `packKey TEXT PRIMARY KEY`
- `kind TEXT NOT NULL`
  - `receive`
  - `compact`
  - `legacy`
- `state TEXT NOT NULL`
  - `active`
  - `superseded`
- `tier INTEGER NOT NULL`
- `seqLo INTEGER NOT NULL`
- `seqHi INTEGER NOT NULL`
- `objectCount INTEGER NOT NULL`
- `packBytes INTEGER NOT NULL`
- `idxBytes INTEGER NOT NULL`
- `createdAt INTEGER NOT NULL`
- `supersededBy TEXT NULL`

Indexes:

- active packs ordered by `state`, `seqHi DESC`
- active packs in a tier ordered by `state`, `tier`, `seqLo`

Notes:

- `seqLo` and `seqHi` are catalog ordering markers, not Git semantics.
- Active packs must always represent a disjoint cover of receive-sequence ranges.
- New receive packs start with `seqLo = seqHi = nextPackSeq`.
- A compacted pack gets `seqLo = min(source.seqLo)` and `seqHi = max(source.seqHi)`.

### DO KV keys that remain authoritative

- `refs`
- `head`
- `refsVersion`
- `packsetVersion`
- `nextPackSeq`
- `receiveLease`
- `compactLease`
- `compactionWantedAt`
- `repoStorageMode`

`repoStorageMode` is temporary rollout metadata:

- `legacy`
- `shadow-read`
- `streaming`

This is repo-local, not an env var. It keeps rollout state explicit while the read and receive cutovers are still in progress.

Admin control surface rules:

- the admin UI and admin endpoint only expose `legacy` and `shadow-read` while streaming receive is not yet implemented;
- both `legacy` and `shadow-read` keep fetch/UI reads on the same pack-first Worker path;
- `legacy` disables packed-vs-compatibility validation;
- `shadow-read` enables packed-vs-compatibility validation;
- `legacy -> shadow-read` requires at least one active pack in `pack_catalog`;
- `shadow-read -> legacy` is the fast way to disable validation if canary noise appears;
- mode changes are blocked while a receive lease or compaction lease is active.

Lease contents:

- `receiveLease = { token, createdAt, expiresAt }`
- `compactLease = { token, createdAt, expiresAt }`

Fixed code constants, not env vars:

- receive lease TTL: 30 minutes
- compaction lease TTL: 20 minutes

Expired leases are cleared:

- on the next `beginReceive()` / `beginCompaction()` call; and
- by a lightweight DO alarm cleanup path.

### Legacy compatibility mirrors

During rollout, keep updating:

- `packList`
- `lastPackKey`

These become mirrors of the active catalog order for rollback and debugging only. `lastPackOids` is not kept current once the streaming receive path is enabled.

## Cross-System Mutation Rules

### Rule 1

Only the DO mutates repo metadata.

### Rule 2

Workers and Queue consumers may write immutable R2 blobs before metadata commit, but those blobs are not visible to the repo until the DO commits the catalog row.

### Rule 3

Deleting unreferenced staged pack blobs and superseded R2 pack pairs is best-effort and retryable. Failure to delete must never affect correctness.

### Rule 4

Compaction never mutates source packs in place. It writes a new pack, then atomically swaps catalog rows in the DO.

### Rule 5

Fetch and UI reads operate on a read-only snapshot of the active catalog. They never mutate repo state.

### Rule 6

Queue delivery is a hint, not the durable record of pending compaction. The durable record is `compactionWantedAt` in DO metadata. Queue delivery failure may delay compaction, but it must not lose the need for compaction.

## Receive Path

### Overview

The Worker handles receive-pack end to end. The DO is used only for:

- acquiring a receive lease;
- reading current refs/HEAD/version metadata;
- committing ref changes and the new pack row;
- clearing the lease.

The DO no longer exposes `POST /receive`.

### Client-visible behavior

Unchanged:

- Git Smart HTTP request/response format.
- `report-status`.
- delete-only pushes.
- stale `old-oid` rejection.
- invalid ref rejection.
- thin-pack acceptance when bases exist.

Changed internally:

- concurrent push handling becomes “one active receive lease per repo”; additional pushes get `503 Retry-After: 10`.
- `X-Repo-Changed` and `X-Repo-Empty` are computed in the Worker after DO finalization, not forwarded from a DO HTTP endpoint.

### Step-by-step algorithm

1. Worker calls `stub.beginReceive()` before reading the body.
2. If a valid receive lease already exists, the Worker immediately returns `503 Retry-After: 10`.
3. If the lease is granted, the Worker incrementally parses the pkt-line command section from `request.body` until the flush packet.
4. The remaining bytes are treated as the raw pack stream.
5. The Worker writes the raw pack stream directly to a staged R2 key such as `do/<id>/objects/pack/pack-rx-<leaseToken>.pack`.
6. During the upload stream, the Worker validates only stream-level invariants that are cheap to check in one pass:
   - pack starts with `PACK`
   - version is 2
   - trailer SHA-1 matches the streamed body
   - byte count matches the actual received length
7. After the R2 upload resolves, the Worker runs the new indexer against the staged pack in R2 and writes `pack-rx-<leaseToken>.idx`.
8. The Worker runs thin-pack base validation and ref-target connectivity validation using the new pack-first object store and the active pack catalog snapshot.
9. The Worker calls `stub.finalizeReceive(...)`.
10. The DO atomically:
    - rechecks the lease token;
    - rechecks `old-oid` expectations against current refs;
    - allocates `nextPackSeq`;
    - inserts the new active pack row;
    - updates `refs`, `head`, `refsVersion`, `packsetVersion`, `packList`, and `lastPackKey`;
    - sets or refreshes `compactionWantedAt` if the catalog now violates the compaction policy;
    - clears the receive lease;
    - returns whether compaction should be queued.
11. The Worker returns the pkt-line `report-status` response.
12. If compaction should run, the Worker enqueues one idempotent Queue message `{ repoId }` in `ctx.waitUntil(...)`.

### Failure handling

If any step before `finalizeReceive()` fails:

- the Worker calls `stub.abortReceive(leaseToken)` best-effort;
- the staged `.pack` and `.idx` are deleted best-effort;
- the response is either `400`, `415`, `500`, or `503` depending on failure class.

If `finalizeReceive()` fails stale-old-oid validation:

- the Worker deletes staged `.pack` and `.idx` best-effort;
- the Worker returns a normal Git `report-status` rejection, not an HTTP 409.

If the client disconnects after `finalizeReceive()` succeeds:

- refs and catalog remain committed;
- a retry from the client is expected to fail stale-old-oid, which is standard Git behavior.

## New Pack Indexer

### Why a new indexer exists

The current `isomorphic-git` path requires the whole pack in memory. The replacement must index from R2 without buffering the entire pack.

### Design

The new indexer lives in `src/git/pack/indexer/` and runs entirely in Worker code.

The indexer is two-stage:

1. `scanPack()`
2. `resolveDeltasAndWriteIdx()`

Implementation note:

- `scanPack()` cannot rely on `DecompressionStream` alone because Git pack entries are concatenated deflate streams and the scanner must know exactly how many compressed bytes each entry consumed;
- the implementation therefore needs a byte-accounting inflate cursor in JS/WASM that exposes end-of-stream position for each packed entry.

### `scanPack()`

Input:

- staged R2 `.pack`
- active pack catalog snapshot for external-base lookup

Behavior:

- reads the pack sequentially from R2 in fixed-size ranges;
- validates header and trailer;
- records, for each object:
  - ordinal
  - offset
  - packed type
  - header length
  - packed span end
  - CRC32 of the raw packed entry
  - base offset for `OFS_DELTA`
  - base oid for `REF_DELTA`
  - result size for delta objects
- computes final oid immediately for non-delta objects by inflating the object payload stream and hashing `"<type> <size>\\0<payload>"`.

Output:

- a compact object-entry table;
- `offset -> index` lookup;
- the list of non-delta resolved objects;
- the list of unresolved delta objects.

Implementation constraint:

- The object-entry table must use typed arrays or chunked binary buffers, not arrays of large JS objects.
- OIDs are stored as raw 20-byte values internally; hex strings are materialized lazily.

### `resolveDeltasAndWriteIdx()`

Behavior:

- resolves unresolved delta objects in dependency order;
- supports:
  - in-pack `OFS_DELTA`
  - in-pack `REF_DELTA`
  - thin-pack external `REF_DELTA` bases
- rejects any delta whose base cannot be resolved from either:
  - an already-scanned in-pack object, or
  - the active pack catalog snapshot.

Memory strategy:

- resolved base payloads are kept in an LRU cache with a hard byte budget;
- evicted base payloads may be recomputed from the pack/object store if needed later;
- correctness does not depend on cache hits.

Delta application:

- parse the delta header varints;
- verify the declared base size;
- materialize the result payload into a pre-sized buffer;
- compute the final oid from `"<baseType> <resultSize>\\0<resultPayload>"`.

After all object oids are resolved:

- sort entries by oid;
- emit a standard Git idx v2 file;
- write it to R2;
- return `objectCount`, `idxBytes`, and the parsed `IdxView`.

### Why this is feasible

For the representative pack:

- pack: about 42 MB
- idx: about 2.7 MB
- objects: about 97k

The design never holds the whole pack in memory. It only holds:

- the compact entry table;
- a bounded payload cache for delta bases;
- the final idx buffer.

That fits comfortably within the 128 MB memory limit for the target workload.

## Connectivity Validation

Validation remains intentionally limited to what the current product already enforces.

### Pack-level validation

Reject the push if any packed object is structurally invalid or any thin-pack external base cannot be resolved from the active catalog snapshot.

### Ref command validation

Keep the existing rules:

- reject `HEAD` updates;
- reject invalid ref names;
- delete requires existing ref and matching `old-oid`;
- create requires zero `old-oid`;
- update requires matching `old-oid`;
- no partial apply: all commands succeed or none apply.

### Target-object connectivity validation

For each non-delete `newOid`:

- resolve tags transitively, with a hard depth limit of 8;
- if the final type is `commit`, require:
  - object exists;
  - root tree exists;
  - each parent exists;
- if the final type is `tree` or `blob`, require it exists.

The design does not add full `fsck` or full tree-walk validation.

## Fetch and Read Path

### New shared object store

Create `src/git/object-store/` and move all correctness-critical object reads to it.

Required APIs:

- `loadActivePackCatalog(repoId)`
- `loadIdxView(packKey)`
- `findObject(oid)`
- `hasObjectsBatch(oids)`
- `readObject(oid)`
- `readObjectRefsBatch(oids)`
- `readBlobStream(oid)`
- `iterPackOids(packKey)`

### Catalog loading

`getPackCandidates()` stops using `pack_objects` and R2 listing as correctness sources.

New behavior:

- ask the DO only for the active pack catalog;
- cache the result in request memo;
- sort active packs by `seqHi DESC`, then by `tier DESC`.

R2 listing remains migration-only fallback, never the primary read path.

### Idx loading

`IdxView` replaces `pack_objects` as the membership source.

Behavior:

- load the entire `.idx` object into memory;
- keep raw fanout, raw name table, and raw offset tables;
- build `oid -> index` lazily or via binary search over the raw name table;
- build `offsetToIndex` and `nextOffset` once per loaded idx.

### Object resolution

`findObject(oid)`:

1. iterate active packs in catalog order;
2. use the loaded `.idx` to check membership;
3. return the first hit as `(packKey, objectIndex, offset, nextOffset)`.

`readObject(oid)`:

1. locate the object by `.idx`;
2. read the object entry header and compressed payload span from the pack via range reads;
3. if base object type, inflate and return payload;
4. if delta, recursively resolve base and apply delta;
5. memoize per request.

### Closure planning

Replace all current dependencies on DO `hasLooseBatch()`, `getPackOids*()`, and `getObjectRefsBatch()`.

New rules:

- `findCommonHaves()` uses `hasObjectsBatch()` over the active pack catalog.
- `buildUnionNeededForKeys()` unions `.idx` membership from the selected packs.
- `computeNeededFast()` uses worker-local `readObjectRefsBatch()` over packed objects.

### Streaming fetch

The existing streaming assembler remains the fetch data path, with two changes:

1. pack discovery comes from the active pack catalog rather than `packList + pack_objects`;
2. all object/membership lookups come from the worker-local object store.

This means:

- single-pack fetch remains streaming;
- multi-pack fetch remains streaming;
- fetch becomes correct even when all loose objects are removed.

### UI and admin read paths

The following routes must stop depending on DO object RPCs for correctness:

- tree
- blob
- raw
- rawpath
- commits
- commit diff
- merge fragment expansion

Required behavior:

- if a loose cache entry exists, it may be used;
- if not, the route must read from packs;
- if a blob is delta-resolved and over the existing UI size cap, preserve the current “too large” behavior.
- `readBlobStream()` must preserve correctness for raw/blob routes, but it is not required to be zero-copy; it may materialize an individual packed object or delta base chain as long as it never requires whole-pack buffering.

## Optional Loose Cache

The DO may continue to cache loose objects, but only as an opportunistic cache.

Rules:

- no fetch or receive correctness may depend on it;
- compaction does not require it;
- deleting all loose data must not break clones, fetches, or UI reads.

Permitted uses:

- hot tree/commit/tag cache
- hot raw/blob cache
- temporary compatibility shims during rollout

## Background Compaction

### Why compaction exists

Removing unpack/hydration means packs in R2 must themselves remain a manageable serving set. Compaction is the replacement for “loose objects + hydration packs”.

### Queue model

Add one Queue binding, for example `REPO_MAINT_QUEUE`.

Producer:

- the main Worker after successful receive finalization;
- admin compaction trigger;
- migration jobs.

Consumer:

- the same Worker export, with `queue()` implemented in `src/git/compaction/run.ts`.

Recommended Queue config:

- `max_batch_size = 1`
- `max_batch_timeout = 1`
- default retries are acceptable

No new env vars are required.

The DO alarm remains in use, but only for lightweight metadata work:

- expire stale receive and compaction leases;
- retry queue re-arm when `compactionWantedAt` is set and no compaction lease is active;
- never perform pack indexing, unpacking, or compaction itself.

### Compaction policy

Fixed constants in code:

- fan-in: 4
- one compaction lease per repo

Policy:

1. tier 0 contains receive packs;
2. if more than four active packs exist in a tier, compact the oldest four in that tier;
3. output one pack in the next tier;
4. source packs become `superseded` only after DO commit.
5. no compaction lease may be granted while a receive lease is active for the same repo.
6. receive has priority over compaction; if a receive lease becomes active after compaction starts, `commitCompaction()` must fail and the queue worker must retry later.

### Compaction algorithm

1. Queue consumer calls `stub.beginCompaction()`.
2. The DO either returns “no work” or returns:
   - lease token
   - `packsetVersion`
   - selected source packs
   - full active catalog snapshot
   - target tier
3. The worker computes `needed = union(source pack idx membership)`.
4. The worker calls the existing or updated streaming assembler with:
   - `packKeys = full active catalog`
   - `needed = union(source pack membership)`

The full active catalog is passed so that delta bases outside the source set are pulled in automatically. The output pack therefore becomes self-contained enough to replace the source packs.

5. The output stream is written directly to a staged compacted-pack key in R2.
6. The new indexer runs against the staged compacted pack and writes the staged `.idx`.
7. The worker calls `stub.commitCompaction(...)`.
8. The DO atomically:
   - rechecks the lease token;
   - rechecks `packsetVersion`;
   - rechecks that the selected source packs are still active and unchanged;
   - marks the new pack active;
   - marks source packs superseded;
   - updates `packList`, `lastPackKey`, and `packsetVersion`;
   - clears or refreshes `compactionWantedAt` based on the post-commit catalog state;
   - clears the compaction lease.
9. The worker deletes superseded R2 blobs best-effort in `waitUntil(...)`.

### What compaction does not do

It does not:

- rewrite refs;
- delete unreachable objects from within a pack;
- require loose objects;
- require hydration state or `pack_objects`.

## Existing Repositories

### Existing packed repos

Migration is automatic:

1. On DO startup or first access, if `pack_catalog` is empty:
   - seed from the union of `lastPackKey`, `packList`, and the full R2 `.pack` listing under the repo prefix;
   - ignore `pack_objects` for correctness.
2. For each discovered pack:
   - require `.pack` and `.idx` to exist;
   - parse `.idx` fanout to get object count;
   - insert a `legacy` `active` row into `pack_catalog`;
   - assign synthetic sequential `seqLo = seqHi` values by:
     - preserving `packList` / `lastPackKey` order when available;
     - appending any R2-only packs in `uploaded` order so no existing pack is omitted.
3. Legacy `pack-hydr-*` packs are inserted as normal active packs. They have no special correctness role under the new design.

### Existing loose-only repos

These cannot switch directly to `repoStorageMode = streaming`.

Required migration:

1. Traverse reachable objects from current refs using the legacy loose path.
2. Build one pack in R2 with a streaming non-delta pack writer.
3. Index it with the new indexer.
4. Insert it into `pack_catalog` as a `legacy` active pack.
5. Leave loose data untouched.

Only after that migration completes may the repo be switched to streaming mode.

### Emergency rollback after a canary repo accepted streaming pushes

Because streaming receive no longer writes `pack_objects` or loose correctness data, emergency rollback for an affected repo requires a backfill tool.

The emergency backfill tool must:

1. read the active pack catalog;
2. materialize objects from packs;
3. repopulate legacy `obj:*` and `pack_objects`;
4. leave new pack data untouched.

This tool is intentionally admin-only and only needed during phase-3 canaries.

## Compatibility Surface

### Routes and headers

Unchanged public Git routes:

- `GET /:owner/:repo/info/refs`
- `POST /:owner/:repo/git-upload-pack`
- `POST /:owner/:repo/git-receive-pack`

Internal headers:

- `X-Repo-Changed`
- `X-Repo-Empty`

These remain worker-internal and continue to exist if callers still use them.

### RPC compatibility

The rollout must make the following compatibility story explicit in code:

- `getObjectStream`, `getObject`, and `getObjectSize`
  - become pack-first shims or are removed from all correctness callers in phase 2;
- `hasLooseBatch`
  - is replaced by `hasObjectsBatch` semantics and may remain only as a compatibility wrapper;
- `getObjectRefsBatch`
  - is replaced by a worker-local packed-object batch reader;
- `getPackLatest`, `getPackOids`, and `getPackOidsBatch`
  - stop being correctness inputs for fetch planning and become compatibility-only or are removed from callers;
- `getUnpackProgress`
  - is removed from route preflight logic and replaced by `beginReceive()` lease acquisition.

No implementation phase may leave a correctness path half-migrated across these APIs.

### Progress and activity UI

The current UI renders unpack progress in multiple places. That caller graph must be migrated explicitly.

Replacement contract:

- replace `getUnpackProgress()` in `src/common/progress.ts` with `getRepoActivity()`;
- `getRepoActivity()` returns a neutral idle state plus two optional live states:
  - `receiving`
  - `compacting`
- the existing progress banner remains in place, but its text changes:
  - `receiving`: “Receiving push…”
  - `compacting`: “Compacting packs…”
  - idle: render nothing.

Files that must migrate together:

- `src/common/progress.ts`
- `src/routes/ui/overview.ts`
- `src/routes/ui/tree.ts`
- `src/routes/ui/commits.ts`
- `src/routes/ui/adminPage.ts`
- `src/client/components/ProgressBanner.tsx`
- `src/client/pages/OverviewPage.tsx`
- `src/client/pages/TreePage.tsx`
- `src/client/pages/CommitsPage.tsx`
- `src/client/pages/AdminPage.tsx`

During phase 2, these files may continue to render a banner, but they must stop interpreting `unpackWork`, `queuedCount`, or hydration presence as correctness signals.

### Admin endpoints

Current hydration routes are retained as compatibility aliases during rollout:

- `POST /:owner/:repo/admin/hydrate`
- `DELETE /:owner/:repo/admin/hydrate`

New semantics:

- `POST` returns a compaction plan by default and triggers compaction when `dryRun === false`;
- `DELETE` clears queued compaction work for the repo, not “hydration packs”.

Add explicit new aliases:

- `POST /:owner/:repo/admin/compact`
- `DELETE /:owner/:repo/admin/compact`

Add a small storage-mode control endpoint:

- `GET /:owner/:repo/admin/storage-mode`
- `PUT /:owner/:repo/admin/storage-mode`

`PUT` accepts only:

- `legacy`
- `shadow-read`

Mode meaning during the read-path rollout:

- `legacy`: packed reads remain authoritative and validation is disabled
- `shadow-read`: the same packed reads remain authoritative and are validated against compatibility reads

It must reject:

- `streaming`
- mode changes while a receive lease is active
- mode changes while a compaction lease is active
- `legacy -> shadow-read` when the repo still has zero active packs

Do not remove the `/hydrate` routes until the admin UI has been migrated and the rollback window has closed.

Current pack-deletion admin behavior must also change:

- `DELETE /:owner/:repo/admin/pack/:packKey`
  - may only delete `superseded` packs;
  - must reject deletion of `active` packs by default;
  - any forced delete of an `active` pack remains an explicit admin-only break-glass path and is outside normal rollout assumptions.

### Debug/admin fields

Replace loose/unpack/hydration-centric fields with:

- `receiveLease`
- `compaction`
- `activePacks`
- `supersededPacks`
- `packCatalogVersion`

Keep legacy fields in debug output during rollout, but mark them as compatibility-only and allow them to show neutral values:

- `unpacking: false`
- `queuedCount: 0`
- `lastPackOids: undefined`

Current debug endpoints must stay functional, but they must read through the pack-first object store:

- `GET /:owner/:repo/admin/debug-state`
- `GET /:owner/:repo/admin/debug-commit/:commit`
- `GET /:owner/:repo/admin/debug-oid/:oid`

Admin UI migration rule:

- phase 2 introduces parallel compaction-oriented props and debug fields;
- phase 3 stops deriving status from `pack-hydr-*`, `unpackWork`, `hydrationQueue`, and hydration-pack counts;
- phase 4 may rename UI components and types, but the compatibility alias endpoints remain until the rollback window closes.

Files that must be migrated together:

- `src/routes/ui/helpers.ts`
- `src/do/repo/debug.ts`
- `src/client/islands/repo-admin/types.ts`
- `src/client/islands/repo-admin/useRepoAdminActions.ts`
- `src/client/islands/repo-admin/RepoOverviewCard.tsx`
- `src/client/islands/repo-admin/HydrationCard.tsx`
- `src/client/islands/repo-admin/PackFilesCard.tsx`
- `src/client/islands/repo-admin/index.tsx`

Required compatibility behavior during the rollout window:

- the admin island may keep the existing `hydration*` prop names temporarily, but those props must be populated from compaction data, not legacy hydration state;
- pack-removal warnings must change from “hydration packs can break fetch” to “active packs may still be referenced until compaction supersedes them”;
- `pack-hydr-*` filename matching must not remain the source of truth for repo status once phase 2 begins.

## What Becomes Obsolete

The following subsystems become legacy-only once rollout is complete:

- DO `POST /receive`
- background unpacking
- hydration planning
- hydration segment building
- `pack_objects` as correctness state
- `lastPackOids` as a read shortcut
- `hasLooseBatch()` as a correctness API

The data/schema remain on disk. The product stops depending on them.

## Module Deliverables

The implementation must keep `repoDO.ts` thin by pushing logic into helper modules.

Required new modules:

- `src/do/repo/catalog.ts`
- `src/do/repo/receiveLease.ts`
- `src/do/repo/compactionLease.ts`
- `src/do/repo/legacyCompat.ts`
- `src/git/receive/pktSectionStream.ts`
- `src/git/receive/streamReceivePack.ts`
- `src/git/pack/indexer/scan.ts`
- `src/git/pack/indexer/inflateCursor.ts`
- `src/git/pack/indexer/resolve.ts`
- `src/git/pack/indexer/writeIdx.ts`
- `src/git/object-store/catalog.ts`
- `src/git/object-store/idxView.ts`
- `src/git/object-store/lookup.ts`
- `src/git/object-store/readObject.ts`
- `src/git/object-store/readRefsBatch.ts`
- `src/git/object-store/delta.ts`
- `src/git/compaction/plan.ts`
- `src/git/compaction/run.ts`
- `src/git/compaction/legacyLoosePack.ts`

Primary files to update:

- `src/routes/git.ts`
- `src/routes/admin.ts`
- `src/routes/ui/adminPage.ts`
- `src/routes/ui/overview.ts`
- `src/routes/ui/tree.ts`
- `src/routes/ui/commits.ts`
- `src/routes/ui/raw.ts`
- `src/routes/ui/helpers.ts`
- `src/index.ts`
- `src/common/progress.ts`
- `src/do/repo/repoDO.ts`
- `src/do/repo/debug.ts`
- `src/do/repo/db/schema.ts`
- `src/do/repo/db/dal.ts`
- `src/client/components/ProgressBanner.tsx`
- `src/client/pages/AdminPage.tsx`
- `src/client/pages/CommitsPage.tsx`
- `src/client/pages/OverviewPage.tsx`
- `src/client/pages/TreePage.tsx`
- `src/client/islands/repo-admin/types.ts`
- `src/client/islands/repo-admin/useRepoAdminActions.ts`
- `src/client/islands/repo-admin/RepoOverviewCard.tsx`
- `src/client/islands/repo-admin/HydrationCard.tsx`
- `src/client/islands/repo-admin/PackFilesCard.tsx`
- `src/client/islands/repo-admin/index.tsx`
- `src/git/operations/closure.ts`
- `src/git/operations/fetch/neededFast.ts`
- `src/git/operations/packDiscovery.ts`
- `src/git/operations/read/diff.ts`
- `src/git/operations/read/objects.ts`
- `src/git/operations/read/tree.ts`
- `src/git/operations/read/commits.ts`
- `src/git/pack/assemblerStream.ts`
- `wrangler.jsonc`

## Phased Implementation Plan

Each phase below is a releasable slice.

### Phase 1: Pack Catalog and Worker Object Store Shadow Mode

Deliverables:

- add `pack_catalog` schema and DAL helpers;
- add DO helpers for catalog reads, leases, and legacy pack-catalog seeding;
- add worker-local `IdxView` and packed-object resolver;
- add read-path shadow validators that compare packed-object reads against legacy reads when `repoStorageMode = shadow-read`;
- add queue binding in `wrangler.jsonc`, but do not trigger compaction yet.

Migration work:

- automatic `pack_catalog` backfill for packed repos;
- no behavior change for receive path;
- no behavior change for admin routes yet.

Validation gate:

- `npm run typecheck`
- existing worker tests remain green
- new tests proving packed-object reads match legacy loose reads on seeded repos

Not done until:

- a repo with only packed data can answer object reads through the new object store with loose data deleted in test setup;
- no caller outside migration/debug reads `pack_catalog` directly without the DAL.

### Phase 2: Read Path Cutover

Deliverables:

- switch fetch planning, object reads, raw/blob/tree/commit/diff routes to the new worker-local object store;
- stop correctness dependence on DO `getObject*`, `hasLooseBatch`, `getPackOids*`, and `getObjectRefsBatch`;
- keep those RPCs only as compatibility shims or debug-only helpers;
- update admin/debug output to show pack-catalog state;
- replace unpack-progress UI with repo-activity UI and migrate the admin island from hydration-derived status to compaction-derived status;
- add an admin-only storage-mode control that reads the current mode and switches between `legacy` and `shadow-read` with lease and active-pack guardrails;
- keep fetch/UI serving on the pack-first object store in both modes; the control only toggles validation;
- replace loose-oriented test seeding with pack-first helpers so new tests do not silently depend on `obj:*`.

Migration work:

- set selected repos to `repoStorageMode = shadow-read` first;
- use `shadow-read` canaries to validate packed reads before streaming receive and compaction land.

Validation gate:

- `npm run typecheck`
- relevant worker fetch/UI tests
- new tests that delete DO `obj:*` and R2 loose mirrors and then verify fetch, tree, blob, raw, commit, and diff still work

Not done until:

- no fetch/UI correctness path depends on loose objects;
- no page or admin island still derives status from `unpackWork`, `unpackNext`, `hydrationQueue`, or `pack-hydr-*`;
- `readLooseObjectRaw()` is either removed from callers or reimplemented as a pack-first compatibility wrapper.

### Phase 3: Streaming Receive Cutover

Deliverables:

- replace DO `/receive` forwarding with worker-local streaming receive;
- add receive lease RPCs;
- implement staged R2 write, new indexer, thin-pack validation, connectivity checks, and DO finalization;
- keep `packList` and `lastPackKey` mirrored for rollback;
- add emergency legacy backfill tool for canary rollback.

Migration work:

- enable `repoStorageMode = streaming` only for canary repos that already have at least one active pack;
- loose-only repos must complete one-time pack migration first.
- phase 3 is canary-only; broad rollout is blocked on phase 4 compaction.

Validation gate:

- `npm run typecheck`
- receive-pack worker tests rewritten for single active receive lease behavior
- new tests for:
  - delete-only push
  - stale old-oid
  - invalid ref
  - thin pack with external base present
  - thin pack with missing external base rejected
  - push followed by deletion of all loose data, then fetch still works

Not done until:

- no code path buffers the entire receive-pack request body;
- the sample target workload can be pushed locally without exceeding memory;
- staged pack cleanup on failure is tested.

### Phase 4: Queue-Driven Compaction

Deliverables:

- implement `beginCompaction` / `commitCompaction` / `abortCompaction`;
- implement queue consumer;
- compact tier overflows into the next tier;
- update fetch pack discovery to read only the active catalog;
- repurpose `/admin/hydrate` as a compaction alias and add `/admin/compact`.

Migration work:

- stop scheduling unpack/hydration for repos in streaming mode;
- keep legacy unpack/hydration data untouched;
- migrate admin UI text and helper logic from hydration to compaction, including `src/client/islands/repo-admin/*` and `src/routes/ui/helpers.ts`.

Validation gate:

- `npm run typecheck`
- worker tests covering:
  - compaction trigger
  - version/lease conflict handling
  - receive-priority conflict where compaction commit is forced to retry
  - source-pack supersession
  - fetch correctness during and after compaction

Not done until:

- active pack count stays bounded in tests after repeated pushes;
- compaction never requires loose objects;
- old source packs are only deleted after successful catalog commit.

### Phase 5: Legacy Path Retirement

Deliverables:

- remove legacy receive/unpack/hydration methods from active code paths;
- keep legacy data/schema in place;
- keep legacy unpack/hydration Wrangler vars and compatibility route aliases in place until the rollback window closes;
- keep the emergency backfill tool until rollout is declared complete;
- downgrade legacy RPCs and tests to compatibility-only or delete them once the rollback window closes.

Migration work:

- move all repos to `repoStorageMode = streaming`;
- keep canary rollback procedure documented until the emergency backfill tool is no longer required.

Validation gate:

- `npm run typecheck`
- full relevant worker suite
- manual canary checklist signed off

Not done until:

- `repoDO.ts` remains a thin delegator;
- no correctness test exercises unpack or hydration.

## Automated Testing Plan

Automated tests must not assume `uncommitted-fixture/` is committed.

### New or rewritten worker tests

1. Receive path streams without `request.arrayBuffer()`.
2. Single active receive lease returns `503` to concurrent pushes.
3. Thin-pack base resolution uses existing packed objects, not loose objects.
4. Fetch works after deleting all DO `obj:*`.
5. Fetch works after deleting all R2 loose mirrors.
6. `hasObjectsBatch()` and `readObjectRefsBatch()` are pack-first.
7. Active catalog backfill from legacy `packList` works.
8. Legacy loose-only migration produces the first active pack.
9. Compaction replaces source packs and keeps fetch correct.
10. Emergency legacy backfill reconstructs rollback data from active packs.
11. `DELETE /admin/pack/:packKey` rejects deletion of active packs.
12. Debug endpoints continue to work after all loose data is removed.
13. `/admin/hydrate` continues to function as a compatibility alias for compaction during rollout.
14. Test repo seed helpers can build pack-only repos without writing loose objects.
15. Pack-first test seeding replaces `seedMinimalRepo()` loose assumptions in new and migrated tests.

### Legacy tests that become compatibility-only

- hydration clear/delete tests
- unpack progress tests
- one-deep unpack queue tests
- `pack_objects` exact membership tests

These should either be:

- rewritten to assert compatibility shims during the rollout window, or
- deleted in phase 5.

### Performance and memory tests

Add a local-only benchmark script under `scripts/` that:

- uses `uncommitted-fixture/` when present;
- measures peak memory, index time, and receive latency;
- is not required for CI.

CI should instead generate smaller deterministic packs with:

- long OFS-delta chains;
- thin REF_DELTA bases;
- multiple receive packs triggering compaction.

Test seeding must also change:

- stop relying on `seedMinimalRepo()` writing loose objects for packed repos;
- add a packed-only seed helper and use it in all fetch/read tests that are meant to validate the new correctness path.

## Acceptance Criteria

The rollout is successful only if all of the following are true:

1. A repo push with about 8,000 commits, about 70,000 loose objects’ worth of content, a 40 MB pack, and a 3 MB idx completes without buffering the whole pack in memory.
2. After such a push, deleting all DO `obj:*` keys does not break fetch, tree/blob/raw reads, diff, or commit browsing.
3. After such a push, deleting all R2 loose mirrors does not break fetch, tree/blob/raw reads, diff, or commit browsing.
4. Fetch planning no longer depends on `pack_objects`, `lastPackOids`, `unpackWork`, `unpackNext`, or hydration state.
5. Active pack count remains bounded by the compaction policy after repeated pushes.
6. The DO is never required to materialize object data for correctness.
7. The only metadata authority remains the DO.
8. No route order or auth behavior regresses.
9. Existing packed repos migrate without requiring loose objects.
10. Legacy loose-only repos have an explicit migration path before cutover.

## Final Notes For Implementers

1. Do not bloat `src/do/repo/repoDO.ts`. Every new behavior belongs in a helper module and is only wired through thin RPC methods.
2. Do not add raw Drizzle queries outside the DAL.
3. Do not reintroduce exact per-pack SQLite membership as a correctness dependency.
4. Do not make loose objects required again, even as a “temporary shortcut”.
5. Do not store repo metadata in R2.
6. Prefer simple fixed constants over new env vars unless a hard operational need appears during implementation.
