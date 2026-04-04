# API Endpoints Reference

## Git Smart HTTP v2 Endpoints

### Clone/Fetch Operations

```bash
# Clone a repository
git clone https://your-domain.com/owner/repo

# Pull updates
git pull https://your-domain.com/owner/repo
```

- **`GET /:owner/:repo/info/refs?service=git-upload-pack`**  
  Capability advertisement for fetch operations. Returns refs and capabilities.

  Advertised capabilities (protocol v2):
  - `version 2`
  - `agent=git-on-cloudflare/0.1`
  - `ls-refs`
  - `fetch`
  - `side-band-64k`
  - `ofs-delta`
  - `object-format=sha1`

  Note: `thin-pack` is not advertised.

  ls-refs arguments supported:
  - `ref-prefix <prefix>` — filter refs by one or more prefixes
  - `peel` — include `peeled:<oid>` attribute for annotated tags
  - `symrefs` — include `symref-target:<ref>` attribute on the `HEAD` line

  The `HEAD` line is emitted first when available and includes `symref-target` for compatibility. When `HEAD` is unborn, the line `unborn HEAD` is advertised.

- **`POST /:owner/:repo/git-upload-pack`**  
  Fetch objects (clone/pull). Handles pack negotiation and object transfer.

  Notes:
  - Streaming with side-band-64k progress.
  - During negotiation (`done=false`), the server returns an acknowledgments section only (no `packfile` section).
  - If the repository has no packs yet, the server returns `503 Service Unavailable` with headers `Retry-After: 5` and `X-Git-Error: repository-not-ready`.

### Push Operations

```bash
# Push without auth (if AUTH_ADMIN_TOKEN not set)
git push https://your-domain.com/owner/repo main

# Push with auth (if AUTH_ADMIN_TOKEN is set)
git push https://owner:token@your-domain.com/owner/repo main
```

- **`GET /:owner/:repo/info/refs?service=git-receive-pack`**  
  Capability advertisement for push operations.

- **`POST /:owner/:repo/git-receive-pack`**  
  Push objects. The Worker writes `.pack` and `.idx` to R2 and commits metadata atomically via DO RPCs. One active receive lease at a time; concurrent pushes receive `503 Retry-After: 10`. Requires authentication if `AUTH_ADMIN_TOKEN` is configured.

## Web UI Routes

- **`GET /`**  
  Home page listing all owners (if registry configured)

- **`GET /:owner`**  
  List repositories for an owner

- **`GET /:owner/:repo`**  
  Repository overview with branches, tags, and README

- **`GET /:owner/:repo/tree`**  
  Browse repository files  
  Query params: `?ref=branch&path=src/lib`

- **`GET /:owner/:repo/blob`**  
  View file contents with syntax highlighting  
  Query params: `?ref=branch&path=README.md`

- **`GET /:owner/:repo/commits`**  
  Commit history with pagination  
  Query params: `?ref=branch&per_page=50&page=0`

- **`GET /:owner/:repo/commits/fragments/:oid`**  
  Fetch side branch commits for a merge (AJAX endpoint)  
  Query params: `?limit=20`  
  Returns: HTML fragment of commit rows for dynamic insertion

- **`GET /:owner/:repo/commit/:oid`**  
  View single commit details

- **`GET /:owner/:repo/raw`**  
  Raw file by OID (inline by default)  
  Query params: `?oid=...&name=file.txt[&download=1]`  
  Notes:
  - Defaults to `Content-Disposition: inline` and `Content-Type: text/plain; charset=utf-8` for safety
  - Add `download=1` to force `attachment` and trigger a file download

- **`GET /:owner/:repo/rawpath`**  
  Raw file by path (primarily for Markdown images and assets)  
  Query params: `?ref=branch&path=src/file.ts&name=file.ts[&download=1]`  
  Notes:
  - Best-effort `Content-Type` is derived from the file extension for inline rendering
  - Requires a same-origin `Referer` header (hotlink protection). Requests from other origins will be rejected with 403.

### Web UI JSON

- **`GET /:owner/:repo/api/refs`**  
  Returns branch/tag refs for UI dropdowns (JSON).  
  Caching: `Cache-Control: public, max-age=60`.

## Authentication Management

### Web UI

- **`GET /auth`**  
  Authentication management interface

### API Endpoints

- **`GET /auth/api/users`**  
  List all owners and their token hashes  
  Requires: `Authorization: Bearer <AUTH_ADMIN_TOKEN>`

- **`POST /auth/api/users`**  
  Add owner/token  
  Body: `{ "owner": "alice", "token": "raw-token" }`  
  Requires: `Authorization: Bearer <AUTH_ADMIN_TOKEN>`

- **`DELETE /auth/api/users`**  
  Remove owner or specific token  
  Body: `{ "owner": "alice", "tokenHash": "..." }`  
  Requires: `Authorization: Bearer <AUTH_ADMIN_TOKEN>`

## Admin Endpoints

All admin endpoints require authentication with owner tokens.

### Admin UI

- **`GET /:owner/:repo/admin`**  
  Admin dashboard (HTML) showing refs/HEAD, compaction status, and pack stats.

### Repository Management

- **`GET /:owner/:repo/admin/refs`**  
  List all refs (JSON format)

- **`PUT /:owner/:repo/admin/refs`**  
  Update refs  
  Body: `[{ "name": "refs/heads/main", "oid": "..." }]`

- **`GET /:owner/:repo/admin/head`**  
  Get HEAD reference

- **`PUT /:owner/:repo/admin/head`**  
  Update HEAD  
  Body: `{ "target": "refs/heads/main" }`

### Registry Management

- **`GET /:owner/admin/registry`**  
  List repos for owner

- **`POST /:owner/admin/registry/sync`**  
  Sync/validate repo registry  
  Body: `{ "repos": ["repo1", "repo2"] }` (optional)

### Debug Endpoints

- **`GET /:owner/:repo/admin/debug-state`**  
  Dump Durable Object state (JSON)

- **`GET /:owner/:repo/admin/debug-commit/:commit`**  
  Check if a commit's tree is present  
  Params: `:commit` is a 40-hex SHA

- **`GET /:owner/:repo/admin/debug-oid/:oid`**  
  Check if a specific OID exists across loose, R2 loose, and packs  
  Params: `:oid` is a 40-hex SHA

### Compaction

- **`POST /:owner/:repo/admin/compact`**  
  Previews compaction (`dryRun=true`, the default) or enqueues compaction work (`dryRun=false`).

- **`DELETE /:owner/:repo/admin/compact`**  
  Clears recorded compaction request.

### Pack Management

- **`DELETE /:owner/:repo/admin/pack/:packKey`**  
  Remove a specific pack and its index/metadata.  
  Returns: `{ ok: boolean, removed: boolean, deletedPack: boolean, deletedIndex: boolean, deletedMetadata: boolean }`.

### Dangerous Operations

- **`DELETE /:owner/:repo/admin/purge`**  
  Purge all repository data (R2 objects and DO state).  
  Body: `{ "confirm": "purge-<owner>/<repo>" }`  
  Returns: JSON result.  
  Warning: Destructive. Requires explicit confirmation.

## Response Formats

### Git Protocol

- Binary pack format for `git-upload-pack` and `git-receive-pack`
- pkt-line format for protocol messages

### JSON API

All JSON endpoints return:

```json
{
  "field": "value"
  // Standard JSON responses
}
```

### Error Responses

```
401 Unauthorized - Missing or invalid authentication
429 Too Many Requests - Rate limited (Retry-After header)
404 Not Found - Repository or resource not found
400 Bad Request - Invalid request parameters
500 Internal Server Error - Server-side error
```
