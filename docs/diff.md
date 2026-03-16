# Commit Diff Proposal

This document proposes a staged implementation for a GitHub-style "what changed in this commit" view that fits the current `git-on-cloudflare` architecture and respects the existing soft IO/subrequest budget.

## Goals

- Show the files changed by a commit in the web UI.
- Keep the implementation aligned with the current Worker read path instead of introducing a second object-read stack in the Repository DO.
- Preserve runtime headroom by short-circuiting unchanged subtrees and applying hard caps.
- Keep the initial implementation small enough to land without schema or storage changes.

## Non-goals

- Exact GitHub parity.
- Rename or copy detection.
- Combined merge diffs.
- Eager inline patch rendering for all files on first page load.
- New SQLite tables or write-path materialization in v1 or v2.

## Relevant Current Architecture

- The commit detail page already exists at `GET /:owner/:repo/commit/:oid` in `src/routes/ui.ts`.
- The current page only renders commit metadata through `src/ui/pages/CommitPage.tsx`.
- The authoritative object read path is the Worker-side `readLooseObjectRaw(...)` in `src/git/operations/read.ts`.
  - It already handles DO-first reads, pack fallback, per-request memoization, immutable object caching, and the request limiter.
- The Repository DO exposes batch helpers such as `getObjectsBatch(...)` and `getObjectRefsBatch(...)`, but `getObjectRefsBatch(...)` only returns referenced OIDs. It does not preserve tree entry names or modes, so it is not sufficient by itself for a path-level file diff.
- The read path already enforces a soft budget through `countSubrequest(...)` and bounded concurrency through `SubrequestLimiter`.
- Commit OIDs are immutable, so diff results are strong candidates for JSON cache entries.

## Why the Diff Should Live in the Worker Read Path

The lowest-surface implementation is to compute the commit delta in the Worker, using the same object loading path that already serves commits, trees, and blobs.

Reasons:

- `readLooseObjectRaw(...)` already knows how to fall back from DO state to R2 packs.
- Request memoization and immutable object caching are already in place.
- Adding a DO-side diff RPC would either be incomplete for packed objects or would duplicate Worker read logic.
- No SQLite schema changes are required.

## V1: Changed File List on the Commit Page

### User-facing scope

V1 adds a "Files changed" section to `GET /:owner/:repo/commit/:oid`.

V1 should show:

- A summary count of files changed.
- A list of changed paths.
- Per-file change type:
  - `A` for added
  - `M` for modified
  - `D` for deleted
- A note when the result is truncated.
- For merge commits, a note that the comparison is against the first parent.

V1 should not show:

- Inline patch hunks.
- Rename detection.
- Combined merge comparison.

### Proposed code changes

#### `src/git/operations/read.ts`

Add a small diff-specific API next to the existing commit/tree readers:

```ts
export type CommitDiffChangeType = "A" | "M" | "D";

export interface CommitDiffEntry {
  path: string;
  changeType: CommitDiffChangeType;
  oldOid?: string;
  newOid?: string;
  oldMode?: string;
  newMode?: string;
}

export interface CommitDiffResult {
  baseCommitOid?: string;
  compareMode: "root" | "first-parent";
  entries: CommitDiffEntry[];
  added: number;
  modified: number;
  deleted: number;
  total: number;
  truncated: boolean;
  truncateReason?: "max_files" | "max_tree_pairs" | "time_budget" | "soft_budget";
}

export async function listCommitChangedFiles(
  env: Env,
  repoId: string,
  oid: string,
  cacheCtx?: CacheContext,
  opts?: {
    maxFiles?: number;
    maxTreePairs?: number;
    timeBudgetMs?: number;
  }
): Promise<CommitDiffResult>;
```

Recommended helper internals:

- `readTreeEntriesMemoized(...)`
- `walkAddedOrDeletedSubtree(...)`
- `diffTreePair(...)`

Keep the logic in `read.ts` for v1 so the code surface stays small. If v2 grows substantially, the diff logic can later move to `src/git/operations/diff.ts`.

#### `src/routes/ui.ts`

Update the commit route to load and cache the diff result:

- Keep the existing `readCommitInfo(...)` call.
- Add a cache key such as `/_cache/commit-diff` with params:
  - `repo`
  - `oid`
  - `v=1`
- Load the diff through `cacheOrLoadJSONWithTTL(...)`.
- Pass the diff result into `renderUiView(env, "commit", ...)`.

Because the route is keyed by commit OID, the diff result is immutable and can use a longer TTL than branch-based UI pages.

Suggested TTL for the JSON diff payload:

- `3600` to `86400` seconds

#### `src/ui/pages/CommitPage.tsx`

Extend the page props to include:

- `diffCompareMode`
- `diffEntries`
- `diffSummary`
- `diffTruncated`
- `diffTruncateReason`

Recommended UI:

- Summary row: `12 files changed`
- Merge label when `compareMode === "first-parent" && parents.length > 1`
- A simple table or list:
  - badge for `A`, `M`, `D`
  - path
  - link for files present in the target commit
- Deleted paths can render as plain text or link to the first parent when one exists

If change-type styling is done with dynamic classes, make sure the corresponding classes are safelisted in `src/styles/app.css`.

### V1 algorithm

#### Base selection

- If the commit has no parent, compare against an empty tree state.
- If the commit has one or more parents, compare against `parents[0]`.
- For merges, set `compareMode` to `first-parent` and expose that in the UI.

Do not use a synthetic empty tree object OID. Use an in-memory empty-tree sentinel so the code does not depend on the empty tree object being present in repo storage.

#### Tree walk

The diff algorithm should compare tree objects recursively and short-circuit whenever tree OIDs match.

High-level flow:

1. Read the target commit via `readCommitInfo(...)`.
2. Resolve the base tree:
   - root commit: no base tree
   - otherwise: read `parents[0]` and use its tree OID
3. Walk the pair `(baseTreeOid, targetTreeOid)` recursively.
4. If the two tree OIDs are equal, stop immediately.
5. When both sides are trees, compare entries by name.
6. When a subtree exists only on one side, recursively enumerate leaf paths as `A` or `D`.
7. When both sides contain non-tree entries at the same path:
   - same OID and same mode: unchanged
   - otherwise: emit `M`
8. When the path changes type:
   - file -> tree: emit `D` for the file, then enumerate added leaves under the tree
   - tree -> file: enumerate deleted leaves from the tree, then emit `A` for the file

Treat any non-`40000` mode as a leaf entry. That keeps symlinks and submodule entries in the same file-like bucket for v1.

#### Ordering

Return entries sorted by path before rendering. This keeps results stable across requests and cache hits.

### V1 runtime guardrails

The tree walk must degrade gracefully before it risks the worker runtime limit.

Recommended defaults:

- `maxFiles`: `300`
- `maxTreePairs`: `2000`
- `timeBudgetMs`: `1500` to `2500`

When a limit is hit:

- Stop traversal.
- Return accumulated results.
- Set `truncated: true`.
- Set `truncateReason`.

This should be treated as a partial success, not an error.

The helper should also treat repeated soft-budget exhaustion from `readLooseObjectRaw(...)` as a truncation signal rather than throwing a hard failure.

### V1 caching and memoization

Use two layers already present in the codebase:

- Per-request memo via `CacheContext.memo`
- Cache API via `cacheOrLoadJSONWithTTL(...)`

Within a single diff computation, use a local tree-entry memo:

```ts
const treeMemo = new Map<string, TreeEntry[]>();
```

This is enough for v1. No new `RequestMemo` field is required.

### V1 testing

Add tests for the read-layer helper and the route integration.

Recommended cases:

- Root commit with only added files.
- Single-file modification.
- Nested directory addition.
- Nested directory deletion.
- File -> directory transition.
- Directory -> file transition.
- Merge commit compares only against the first parent.
- Truncation when `maxFiles` is exceeded.
- Truncation when `timeBudgetMs` is exceeded.

## V2: Lazy Patch Expansion Per File

### User-facing scope

V2 keeps the v1 file list and adds on-demand patch expansion for individual files.

The commit page should still render quickly with only the file list on first load. Patch bodies should be fetched only when the user expands a file.

### Why patches should be lazy

Generating patch hunks is much more expensive than generating a changed-file list because it requires blob reads, text decoding, and diff generation.

Lazy loading keeps the hot path cheap and predictable:

- Initial commit page load only reads commit/tree objects.
- Blob reads only happen for the file the user expands.
- Large or binary files can be skipped without penalizing the entire page.

### Proposed code changes

#### Reuse v1 diff result as the index

V2 should not recompute the full tree diff for every patch request.

Instead:

1. Load the cached v1 `CommitDiffResult`.
2. Find the requested path in `entries`.
3. Use that entry's `oldOid` and `newOid` to build the patch.

This keeps v2 layered on top of v1 instead of duplicating tree traversal.

#### New route in `src/routes/ui.ts`

Add a lazy patch endpoint:

- `GET /:owner/:repo/commit/:oid/diff?path=<path>`

Suggested response shape:

```ts
{
  path: string,
  changeType: "A" | "M" | "D",
  oldOid?: string,
  newOid?: string,
  oldTooLarge?: boolean,
  newTooLarge?: boolean,
  binary?: boolean,
  skipped?: boolean,
  skipReason?: "binary" | "too_large" | "not_found" | "too_many_lines",
  patch?: string
}
```

The endpoint can return JSON for a small React island to render.

#### Read-layer helper

Add a blob-level helper, either in `src/git/operations/read.ts` or a new `src/git/operations/diff.ts` if the logic becomes large:

```ts
export async function readCommitFilePatch(
  env: Env,
  repoId: string,
  oid: string,
  path: string,
  cacheCtx?: CacheContext,
  opts?: {
    maxBlobBytes?: number;
    maxPatchBytes?: number;
    maxLines?: number;
  }
);
```

Recommended flow:

1. Load cached v1 diff metadata.
2. Resolve `oldOid` and `newOid`.
3. Read only the blobs needed for the requested path.
4. Reject binary or oversized content with a structured summary.
5. Generate a unified patch string.
6. Cache the path-specific patch response.

### Patch generation choice

There is no existing diff dependency in `package.json`.

Recommended approach for v2:

- Add a small pure-JS diff dependency rather than maintaining a custom Myers implementation in-repo.

If adding a dependency is undesirable, a minimal line-based diff utility can be implemented locally, but that increases code surface and maintenance cost.

### V2 runtime guardrails

V2 should be much stricter than v1 because blob content is larger and text diff generation is CPU-heavy.

Recommended defaults per expanded file:

- `maxBlobBytes`: `128 KiB`
- `maxPatchBytes`: `256 KiB`
- `maxLines`: `4000`

Behavior:

- If either side exceeds the size cap, return a summary without a patch.
- If either side is binary, return a summary without a patch.
- If the generated patch would exceed the output cap, truncate or skip it with a clear reason.

### V2 UI

Add a small island, for example:

- `src/ui/islands/commit-diff-expander.tsx`

Behavior:

- Start with all files collapsed.
- On expand, fetch `/:owner/:repo/commit/:oid/diff?path=...`.
- Render:
  - patch text in a `<pre>`
  - or a summary message for binary / large files
- Cache the loaded patch in client state so collapsing and re-expanding does not re-fetch during the same page session.

For v2, plain text patch rendering is sufficient. Syntax highlighting is optional and can be deferred.

### V2 caching

Because a patch is immutable for `(repoId, commitOid, path)`, cache it through the existing JSON cache helper.

Suggested key:

- `/_cache/commit-patch?repo=<repoId>&oid=<oid>&path=<path>&v=1`

Suggested TTL:

- `3600` to `86400` seconds

## Deferred Alternatives

These are explicitly out of scope for v1 and v2 but remain possible later:

- Rename detection by similarity scoring.
- Combined merge diff.
- Precomputing commit deltas during unpack.
- Materializing commit diff metadata into SQLite.

The last option should only be considered if on-demand diff computation proves too slow in real workloads. If that path is taken, all SQLite access must go through `src/do/repo/db/dal.ts`.

## Recommended Rollout

### Phase 1

- Implement v1 changed-file list.
- Ship with truncation support.
- Observe latency and cache hit rate.

### Phase 2

- Add lazy per-file patch expansion.
- Keep file list SSR and patch loading client-driven.
- Skip binary and oversized files.

### Phase 3

- Re-evaluate whether rename detection or unpack-time materialization is worth the added complexity.

## Recommendation Summary

- **V1** should be a Worker-side tree diff against the first parent, cached by commit OID, with hard traversal caps and a truncated partial-success mode.
- **V2** should reuse the cached v1 file list and add lazy, path-specific patch expansion for small text files only.
- Neither v1 nor v2 needs new DO RPC methods, SQLite tables, or write-path changes.
