# git-on-cloudflare

[![Deploy to Cloudflare Workers](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/zllovesuki/git-on-cloudflare)

**A Git Smart HTTP v2 server running entirely on Cloudflare Workers** — no VMs, no containers, just Durable Objects and R2.

Host unlimited private Git repositories at the edge with <50ms response times globally. Full Git compatibility, modern web UI, and usage-based pricing that actually makes sense.

## Key Features

- **Complete Git Smart HTTP v2 implementation** with pack protocol support (`ls-refs`, `fetch`, side-band-64k, ofs-delta)
- **Strong consistency** via Durable Objects for refs/HEAD (the hard part of distributed Git)
- **Two-tier caching** reducing latency from 200ms to <50ms for hot paths
- **Streaming pack assembly** from R2 with range reads for efficient clones
- **Streaming push pipeline** with atomic pack ingress and queue-driven compaction
- **Modern web UI** with Tailwind CSS v4, React SSR, and focused client islands
- **Interactive merge commit exploration** - expand merge commits to see side branch history
- **Safer raw views**: `text/plain` for `/raw` by default and same‑origin Referer check for `/rawpath` to prevent hotlinking

## Quick Demo

```bash
# Clone the project
git clone https://github.com/zllovesuki/git-on-cloudflare
cd git-on-cloudflare
npm install

# Start locally with Vite + Workers SSR (no Docker required)
npm run dev

# Push any repo to it
cd /your/existing/repo
git push http://localhost:8787/test/myrepo main
```

Visit `http://localhost:8787/test/myrepo` to browse your code. That's it — you now have a fully functional Git server.

TSX edits trigger Vite-powered Worker reloads and CSS changes hot-update through the client entry.

## Technical Architecture

This is a complete Git Smart HTTP v2 server built on Cloudflare's edge primitives:

### Core Design

- **Durable Objects** provide linearizable consistency for refs/HEAD without coordination
- **R2 storage** for pack files and objects with range-read support for streaming
- **Workers** handle the Git protocol, pack negotiation, smart HTTP transport, and React server rendering
- **Two-tier caching**: UI responses (60s-1hr TTL), Git objects (1 year, immutable)

### Performance Characteristics

- **Clone speeds**: 10-50 MB/s from any edge location
- **Push processing**: <5s for typical commits, large pushes handled incrementally
- **Response times**: <50ms for cached paths, <100ms globally for cold requests
- **Pack assembly**: Streaming from R2 using `.idx` range reads, with heuristics to load whole packs when beneficial
- **Centralized pack discovery**: Per-request coalesced discovery (DO metadata + best-effort R2 listing) reduces upstream calls
- **Memory efficiency**: Streaming implementation with crypto.DigestStream for incremental SHA-1 computation

### Implementation Details

- Complete Git pack protocol v2 with `ls-refs` and `fetch` commands
- Streaming receive writes packs directly to R2 with atomic metadata commit
- PBKDF2-SHA256 (100k iterations) for auth tokens
- Modern web UI with Tailwind CSS v4, React page components, and worker-side SSR
- SQLite-backed metadata inside Durable Objects using `drizzle-orm/durable-sqlite`
- Structured JSON logging with `LOG_LEVEL` (debug/info/warn/error)

## Deploy to Production

```bash
# Configure Cloudflare account
wrangler login

# Set admin token for auth UI
wrangler secret put AUTH_ADMIN_TOKEN

# Deploy to Workers
npm run deploy
```

Your Git server will deploy to your configured route or to `*.workers.dev`, depending on your Wrangler configuration. Push repos, browse code, manage auth — all from the edge.

> **Upgrading?** If upgrading from a commit before `a76650c`, read `MIGRATION-STREAMING-PUSH.md` for the required deployment sequence.

## Authentication

By default, repos are **completely open** — anyone can push and pull without authentication.

To enable push protection, set `AUTH_ADMIN_TOKEN`:

```bash
# Development
echo "AUTH_ADMIN_TOKEN=secret123" > .dev.vars

# Production
wrangler secret put AUTH_ADMIN_TOKEN
```

With auth enabled:

- **Reads remain public** (clone/pull/browse)
- **Pushes require authentication** (per-owner tokens)
- Manage tokens at `/auth` or via API
- Tokens use PBKDF2-SHA256 with 100k iterations

> [!TIP]
> For local `vite dev` testing, you may want to configure Git credentials up front instead of waiting for an interactive prompt. Miniflare currently has a bug where some backend `401 Unauthorized` responses can surface as a `500`, which prevents Git from prompting as it normally would against a deployed Worker.
>
> For example, if your owner is `rachel` and your token is `testtoken`, you can send the `Authorization` header explicitly:
>
> ```bash
> git -c http.extraHeader='Authorization: Basic <base64(rachel:testtoken)>' \
>   push http://127.0.0.1:5173/rachel/my-repo HEAD:refs/heads/main
> ```

Admin endpoints for hydration and repository management are protected via owner Basic auth and the admin bearer token for `/auth/api/*`. An admin dashboard is available at `/:owner/:repo/admin`.

## Configuration

Environment variables control pack management and unpacking behavior:

```bash
REPO_DO_IDLE_MINUTES=30      # Cleanup idle repos after 30 min
REPO_DO_MAINT_MINUTES=1440   # Run maintenance daily
REPO_KEEP_PACKS=10           # Long-term pack retention
REPO_PACKLIST_MAX=50         # Max recent packs considered for discovery
REPO_UNPACK_CHUNK_SIZE=100   # Objects per unpack slice
REPO_UNPACK_MAX_MS=2000      # Max CPU time per unpack slice
REPO_UNPACK_DELAY_MS=500     # Delay between slices (ms)
REPO_UNPACK_BACKOFF_MS=1000  # Backoff when under load (ms)
LOG_LEVEL=info               # debug|info|warn|error
```

See `wrangler.jsonc` for the complete configuration.

## Documentation

- [API Endpoints](docs/api-endpoints.md) - Complete HTTP API reference
- [Architecture Overview](docs/architecture.md) - Module structure and components
- [Storage Model](docs/storage.md) - Hybrid DO + R2 storage design
- [Data Flows](docs/data-flows.md) - Push, fetch, and web UI flows
- [Caching Strategy](docs/caching.md) - Two-tier caching implementation

## Limitations

- Receive-pack buffers the uploaded pack in memory inside the Durable Object. Practical pack size limit is ~100–128MB; very large pushes may fail. Split large pushes when needed.
- 30s CPU limit per request on the main fetch paths (unpack runs in alarm-driven slices)
- HTTP(S) only, no SSH protocol support
- No server-side hooks yet
- Thin-pack is not advertised; clients receive thick packs (side-band-64k, ofs-delta)

## Development

```bash
npm install
npm run dev             # Start local server
npm run test:workers    # Run Vitest tests
npm run test:auth       # Run Auth DO tests
npm run test            # Run AVA tests
```

## License

MIT
