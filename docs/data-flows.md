# Data Flows

This document describes the primary data flows of the server: pushing (receive-pack), fetching (upload-pack), and the Web UI blob views.

## Push (git-receive-pack)

1. Client sends `POST /:owner/:repo/git-receive-pack`
2. Worker acquires a receive lease via `beginReceive()` RPC. If a lease is already active, returns `503 Retry-After: 10`.
3. Worker parses pkt-line commands and the packfile payload from the request body.
4. Worker writes the `.pack` to R2, builds `.idx` inline, and writes it to R2.
5. Worker commits refs and pack-catalog metadata atomically via `finalizeReceive()` RPC.
6. Returns a pkt-line `report-status` response with sideband progress.

### Metadata maintained by the DO

- SQLite tables (embedded in the DO):
  - `pack_catalog(pack_key, ...)` — authoritative pack metadata for read-path discovery and compaction

## Fetch (git-upload-pack v2)

1. Client sends capability advertisement request: `GET /:owner/:repo/info/refs?service=git-upload-pack`
2. For `POST /:owner/:repo/git-upload-pack` with a v2 body:
   - `ls-refs` command: reads the DO via RPC (`getHead()` and `listRefs()`) and responds with HEAD + refs
   - `fetch` command:
     - Negotiation phase (`done=false`): server returns an acknowledgments block only (ACK/NAK), no `packfile` section
     - Parses wants/haves and computes minimal closure using frontier-subtract approach with stop sets
     - Loads the active pack catalog via `src/git/object-store/catalog.ts#loadActivePackCatalog()`, memoized per request with limiter + soft budget
     - **Streaming pack assembly** (no buffering):
       - Single-pack: `streamPackFromR2()` streams directly from R2 with backpressure
       - Multi-pack union: `streamPackFromMultiplePacks()` with proper delta resolution
       - Uses `crypto.DigestStream` for incremental SHA-1 computation
       - Emits sideband-64k with progress messages on channel 2
     - If repository has no packs, returns `503` with `Retry-After: 5`
     - If closure traversal times out, tries a safe multi-pack union based on recent packs

## Web UI blob views

- `GET /:owner/:repo/blob?ref=...&path=...` (preview)
  - Resolves path to an OID via pack-first reads through the worker-local object store
  - If the file is "too large" (configurable threshold), shows a friendly message and links to raw
  - If not too large, fetches the object and renders text (with simple binary detection)

- `GET /:owner/:repo/raw?oid=...&name=...` (raw)
  - Reads the object via pack-first store, decompresses, and streams
  - Uses `Content-Disposition: inline` by default (add `&download=1` to force attachment)
  - Uses `text/plain; charset=utf-8` for safety (prevents HTML/JS execution)

## Merge commit exploration

- `GET /:owner/:repo/commits` (main page)
  - Displays commit history with expandable merge commits
  - Merge commits show a badge and are clickable to expand side branch history
- `GET /:owner/:repo/commits/fragments/:oid` (AJAX fragment)
  - Called when user clicks a merge commit row
  - Uses `listMergeSideFirstParent()` to traverse non-mainline parents
  - Algorithm:
    1. Probe mainline (parents[0]) to build a stop set
    2. Initialize frontier with side parents (parents[1..])
    3. Priority queue traversal by author date (newest first)
    4. Stop when: reached limit, hit mainline, timeout, or scan limit
  - Returns HTML fragment with commit rows for dynamic insertion
  - No caching at UI level (dynamic content)
