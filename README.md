# git-on-cloudflare

[![Deploy to Cloudflare Workers](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/zllovesuki/git-on-cloudflare)

**A Git Smart HTTP v2 server running entirely on Cloudflare Workers** — no VMs, no containers, just Durable Objects and R2.

Host unlimited private Git repositories at the edge with <50ms response times globally. Full Git compatibility, modern web UI, and usage-based pricing that actually makes sense.

> **Upgrade Notice:** The streaming-push closure release removed all legacy receive paths and rollback machinery. If upgrading from a pre-streaming deployment, you **must** first deploy and validate the cutover release (commit `98ad7dd`) before deploying the current version. See `MIGRATION-STREAMING-PUSH.md` for the required deployment sequence.

> **tessera Ownership Upgrade Notice:** Legacy owner-token authentication has also been replaced by tessera browser sessions, D1-backed repository ownership, and personal access tokens. If upgrading an existing fork or self-hosted deployment from before the tessera migration, follow `MIGRATION-TESSERA-OIDC.md` before deploying latest `main`.

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
- Tessera OIDC browser sessions and personal access tokens for Git pushes
- Modern web UI with Tailwind CSS v4, React page components, and worker-side SSR
- SQLite-backed metadata inside Durable Objects using `drizzle-orm/durable-sqlite`
- Structured JSON logging with `LOG_LEVEL` (debug/info/warn/error)

## Deploy to Production

```bash
# Configure Cloudflare account
wrangler login

# Set session and tessera OIDC secrets
wrangler secret put SESSION_SECRET
wrangler secret put TESSERA_OIDC_CLIENT_SECRET

# Deploy to Workers
npm run deploy
```

Your Git server will deploy to your configured route or to `*.workers.dev`, depending on your Wrangler configuration. Push repos, browse code, and manage account tokens from the edge.

> **Upgrading an existing deployment?** Read `MIGRATION-STREAMING-PUSH.md` first if you are pre-streaming, then `MIGRATION-TESSERA-OIDC.md` if you are crossing the tessera ownership migration.

## Authentication

Authentication uses tessera OIDC for browser sessions and goc personal access tokens for Git over HTTP Basic.

```bash
# Development
cp .dev.vars.example .dev.vars

# Production
wrangler secret put SESSION_SECRET
wrangler secret put TESSERA_OIDC_CLIENT_SECRET
```

- Public repos can be cloned and browsed anonymously when present in the route cache.
- Private repos require a signed-in namespace member for web UI access.
- Git pushes require a PAT with push access; HTTP Basic username must match the namespace slug.
- Manage repositories and PATs at `/auth/account`.

On sign-in the server claims a namespace from the ID token's `preferred_username`
claim, falling back to the email local-part for providers such as Google that do
not emit `preferred_username`. Collisions are resolved with numeric suffixes. If
every candidate is taken, `/auth/account` shows a self-service claim form instead
of leaving the account unusable.

> [!TIP]
> For local `vite dev` testing, you may want to configure Git credentials up front instead of waiting for an interactive prompt. Miniflare currently has a bug where some backend `401 Unauthorized` responses can surface as a `500`, which prevents Git from prompting as it normally would against a deployed Worker.
>
> For example, if your namespace is `rachel` and your PAT is `goc_abcd1234_secret`, you can send the `Authorization` header explicitly:
>
> ```bash
> git -c http.extraHeader='Authorization: Basic <base64(rachel:goc_abcd1234_secret)>' \
>   push http://127.0.0.1:5173/rachel/my-repo HEAD:refs/heads/main
> ```

Admin endpoints for compaction and repository management require a signed-in tessera session with namespace membership. An admin dashboard is available at `/:owner/:repo/admin`.

## Configuration

Environment variables:

```bash
REPO_DO_IDLE_MINUTES=30      # Cleanup idle repos after 30 min
LOG_LEVEL=info               # debug|info|warn|error
SESSION_SECRET=...           # Browser session sealing secret
TESSERA_OIDC_ISSUER=...      # tessera issuer URL
TESSERA_OIDC_CLIENT_ID=...   # tessera client id
TESSERA_OIDC_CLIENT_SECRET=... # tessera client secret
```

See `.dev.vars.example` and `wrangler.jsonc` for the complete configuration.

## Documentation

- [API Endpoints](docs/api-endpoints.md) - Complete HTTP API reference
- [Architecture Overview](docs/architecture.md) - Module structure and components
- [Storage Model](docs/storage.md) - Hybrid DO + R2 storage design
- [Data Flows](docs/data-flows.md) - Push, fetch, and web UI flows
- [Caching Strategy](docs/caching.md) - Two-tier caching implementation
- [Streaming Push Migration Guide](MIGRATION-STREAMING-PUSH.md) - Required path for pre-streaming deployments
- [tessera Ownership Migration Guide](MIGRATION-TESSERA-OIDC.md) - Required path for deployments crossing the legacy auth cutover

## Limitations

- 30s CPU limit per request on fetch and receive paths
- HTTP(S) only, no SSH protocol support
- No server-side hooks yet
- Thin-pack is not advertised; clients receive thick packs (side-band-64k, ofs-delta)

## Development

```bash
npm install
npm run dev             # Start local server
npm run test:workers    # Run Vitest tests
npm run test:auth       # Run Auth DO tests
npm run test            # Run Node Vitest unit tests
```

## License

MIT
