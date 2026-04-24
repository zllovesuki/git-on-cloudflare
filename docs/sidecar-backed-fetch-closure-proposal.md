# One-Shot Plan: Sidecar-Backed Fetch Closure

## Summary

Implement per-pack logical object-reference sidecars and use them for final `git-upload-pack` closure planning. This directly addresses the incident: fetch planning must not read thousands of packed objects from R2 just to discover commit/tree/tag edges.

The pressure test changed the plan in four important ways: sidecar readiness is checked before streaming starts, `.refs` becomes a required artifact for newly active packs, backfill uses the streaming indexer with existing `.idx` help for tricky `REF_DELTA` bases, and tree gitlinks are excluded from closure edges.

## Design Decisions

- Add an R2 sidecar beside every pack: `packKey.replace(/\.pack$/, ".refs")`, exposed through `packRefsKey(packKey)` in `src/keys.ts`.
- Keep sidecars as derived R2 artifacts. Do not add SQLite tables and do not add DO methods unless an existing RPC cannot provide the current active catalog.
- Sidecars are required for final fetches with haves. Initial clone with no haves stays idx-only via `buildInitialCloneNeeded()`.
- Missing/corrupt/stale sidecar on final fetch returns `503 Retry-After: 10` before `packfile\n` is emitted, and queues idempotent backfill.
- The production fetch planner must not fall back to `readObjectRefsBatch()` for active packs. That path is the scalability problem.

## Sidecar Format and Types

- Create a focused module, e.g. `src/git/pack/refIndex.ts`, with named types:
  - `PackRefView`
  - `PackRefBuildResult`
  - `PackRefSnapshotEntry`
  - `PackRefSnapshotLoadResult`
- Binary format v1:
  - magic/version
  - `objectCount`
  - `packBytes`
  - pack checksum from the `.idx` trailer
  - idx checksum from the `.idx` trailer
  - `typeCodes` in idx OID order, using canonical Git object type codes for commit/tree/blob/tag
  - `refStarts` length `objectCount + 1`
  - flat raw 20-byte ref OIDs
- Parser rejects bad magic/version, count mismatch, pack byte mismatch, checksum mismatch, truncation, non-monotonic `refStarts`, and invalid final offset.
- Extend `IdxView` to expose `packChecksum` and `idxChecksum`.
- Extend `RequestMemo` with `packRefViews` and `packRefViewPromises`.
- Add a byte-capped isolate LRU for `PackRefView`, keyed by `packKey + idxChecksum`. Do not keep unbounded global string maps.

## Indexer Changes

- Refactor the pack indexer around a named `PackRefsBuilder`.
- Add final object type storage to `PackEntryTable`; do not overwrite existing pack type fields because rewrite/idx logic still needs pack type codes.
- Feed `PackRefsBuilder` whenever a full logical object payload is available:
  - non-delta objects in `scanPack()`
  - resolved delta objects in `resolveDeltaEntry()`
  - external REF_DELTA fallback objects after delta application
- Add a new tree parser for closure refs that includes mode parsing and excludes gitlinks (`160000`). Keep existing `parseTreeChildOids()` behavior unless all callers are reviewed.
- Extract one shared OID-order helper from `writeIdxV2()` and use it for both `.idx` and `.refs`. Tie-break duplicate OIDs deterministically by pack entry index.
- Replace or wrap `resolveDeltasAndWriteIdx()` with an artifact-writing API that writes `.idx` and `.refs` together and returns `{ objectCount, idxBytes, refIndexBytes, idxView }`.

## Receive and Compaction

- Receive with a non-delete pack:
  - stage `.pack`
  - scan/resolve
  - write `.idx`
  - write `.refs`
  - run connectivity
  - call `finalizeReceive()`
- If receive fails after staging, cleanup deletes `.pack`, `.idx`, and `.refs`.
- Delete-only receive does no sidecar work.
- Compaction writes `.refs` for the compact target before `commitCompaction()`.
- Compaction retry/conflict/error cleanup deletes staged `.pack`, `.idx`, and `.refs`.
- Delayed superseded-pack delete deletes `.refs` with `.pack` and `.idx`.
- Admin `removePack()` deletes `.refs` for superseded packs. Missing `.refs` is a successful no-op.
- Purge remains prefix-based and needs no special-case sidecar logic.

## Fetch Path

- Move final fetch planning before creating the response stream. The stream should only start after snapshot, idx, sidecar, and closure planning are ready.
- Add `loadPackRefSnapshot(env, repoId, snapshot, cacheCtx)`:
  - loads one sidecar per active pack
  - validates against the corresponding `IdxView` and `PackCatalogRow`
  - uses `limiter.run("r2:get-pack-refs", ...)`
  - logs `stream:plan:ref-snapshot`
- Add sidecar-backed closure planner:
  - preserve current have cap behavior unless tests intentionally change it
  - common haves are active-pack membership checks from idx snapshots
  - stop set starts with acknowledged haves and retains the existing first-parent enrichment
  - walk wants through sidecar refs with a cursor queue
  - use request-local maps bounded by active indexed object count
  - compute reachable want closure minus stop-set reachability
- If planner budget is exceeded, return `503 Retry-After: 10` before streaming and log `stream:plan:closure-budget-exceeded`.
- Keep `readObjectRefsBatch()` for non-fetch callers/tests only, or leave it as an explicitly non-production fallback with tests proving fetch does not call it.

## Backfill

- Add queue message `pack-ref-backfill` to `RepoMaintenanceQueueMessage`.
- Fetch queues one message per missing/corrupt/stale active pack using `ctx.waitUntil`; duplicate messages are allowed.
- Queue handler:
  - gets current active catalog through existing DO RPC with limiter/counting
  - acks if target pack is no longer active
  - loads target `.idx`; if valid `.refs` already exists, ack
  - runs the streaming scan/resolve machinery with the target pack excluded from the external-base catalog
  - seeds same-pack `REF_DELTA` dependency edges from the existing `.idx` so backfill does not fall back to recursive packed-object reads for objects already in the target pack
  - tries duplicate external-base candidates in catalog order so a newer unusable duplicate does not hide an older materializable base
  - writes only the `.refs` sidecar; the deterministic `.idx` is already present and must not be rewritten by backfill
  - retries transient R2/DO failures
  - acks deterministic invalid pack/idx failures after logging
- The DO never reads or writes R2 for backfill.

## State Matrix

| State                                       | Behavior                                                    |
| ------------------------------------------- | ----------------------------------------------------------- |
| Initial clone, no haves                     | Existing idx-only path; sidecar not required                |
| Negotiation request, `done=false`           | Existing ACK-only behavior; sidecar not required            |
| Final fetch, all sidecars valid             | Sidecar closure, then stream pack                           |
| Final fetch with duplicate wants            | Queue and send each canonical active object once            |
| Final fetch reaches duplicate OID in packs  | Newest snapshot-order pack is canonical                     |
| Force-push leaves overlapping active packs  | Traverse newest pack first; old overlap does not grow queue |
| Final fetch has no common haves             | Sidecar closure runs with an empty stop set                 |
| Final fetch, sidecar missing/corrupt/stale  | Queue backfill, return `503 Retry-After: 10` before stream  |
| Final fetch exceeds closure timeout         | Return `503 Retry-After: 10` before stream                  |
| Final fetch exceeds missing-ref cap         | Return `503 Retry-After: 10` before stream                  |
| Active pack has idx missing/corrupt         | Existing repository-not-ready behavior                      |
| Newly received pack `.refs` write fails     | Abort receive, cleanup staged artifacts, no catalog commit  |
| Receive connectivity rejects                | Cleanup staged `.pack/.idx/.refs`, no catalog commit        |
| Finalize receive conflict/lease mismatch    | Cleanup staged `.pack/.idx/.refs`                           |
| Compaction target `.refs` write fails       | Abort/retry compaction, cleanup staged artifacts            |
| Superseded pack delete sees missing `.refs` | Log debug/info and continue                                 |
| Superseded pack delete                      | Delete `.pack`, `.idx`, and `.refs` together                |
| Admin remove superseded pack                | Delete `.pack`, `.idx`, `.refs`; report artifact outcomes   |
| Backfill enqueue fails                      | Log warning; still return retry readiness response          |
| Backfill target no longer active            | Ack as stale                                                |
| Backfill sees valid sidecar already present | Ack as duplicate/no-op                                      |
| Backfill transient R2/DO failure            | Retry with delay                                            |
| Backfill deterministic invalid pack/idx     | Log invalid-pack and ack                                    |

## Logging and Limits

- Add structured logs for:
  - `ref-index:write-start`
  - `ref-index:write-complete`
  - `ref-index:write-error`
  - `stream:fetch:ref-index-missing`
  - `stream:plan:ref-snapshot`
  - `ref-index:backfill-start`
  - `ref-index:backfill-complete`
  - `ref-index:backfill-stale-pack`
  - `ref-index:backfill-invalid-pack`
- Every new R2 and DO call goes through the existing limiter/counting pattern:
  - `r2:get-pack-refs`
  - `r2:put-pack-refs`
  - `r2:delete-pack-refs`
  - `do:get-active-pack-catalog`
- Queue sends are logged and scheduled with `waitUntil`; queue worker R2/DO work uses the same limiter rules as compaction.

## Tests

- Unit tests:
  - sidecar encode/decode round trip
  - every validation rejection case
  - tree parser excludes `160000` gitlinks
  - commit/tree/tag/blob edge encoding
  - duplicate OID deterministic ordering
- Worker tests:
  - receive writes `.refs`
  - compaction writes target `.refs`
  - cleanup deletes `.refs` on receive rejection, finalize conflict, lease mismatch, compaction retry, and superseded delete
  - final fetch with valid sidecars does not call per-object `r2:get-range` during closure
  - missing sidecar returns `503` before `packfile\n`
  - backfill creates sidecar and subsequent fetch succeeds
  - corrupt/stale sidecar is rebuilt
  - annotated tag chain closure works
  - submodule gitlink is not required as an object in the superproject
- Large fixture:
  - opt-in test using `uncommitted-fixture/pack-395a180893e59dad8ef9d7fa135ecd8b1b399bb1.pack`
  - assert sidecar generation handles 97,409 objects
  - assert closure planning uses sidecar reads and stays within request subrequest/memory expectations
- Validation commands:
  - `npm run typecheck`
  - targeted fetch/indexer/receive/compaction worker tests
  - opt-in large fixture test when validating scale

## Assumptions

- Temporary `503 Retry-After` is acceptable for incremental fetches while legacy active packs are backfilled.
- No protocol support for shallow/filter fetch is added in this change.
- Sidecars are derived artifacts; correctness still depends on `.pack`, `.idx`, and the DO active pack catalog.
