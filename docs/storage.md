# Storage Model (DO + R2)

This project uses a hybrid storage approach to balance strong consistency for refs and metadata with cheap, scalable storage for pack data:

## Durable Objects (DO) storage

- Per-repo, strongly consistent state (the metadata authority)
- Stores:
  - `refs` (array of `{ name, oid }`)
  - `head` (object with `target`, optional `oid`, `unborn`)
  - Lease state (`receiveLease`, `compactLease`)
- Access patterns:
  - Always consistent; great for writes and metadata reads

### SQLite metadata in Durable Objects

- A small SQLite database is embedded in each Repository DO using `drizzle-orm/durable-sqlite`.
- Tables:
  - `pack_catalog(pack_key, ...)` — authoritative pack metadata. Drives read-path discovery and compaction planning.
- Migrations run during DO initialization via `migrate(db, migrations)` and Wrangler `new_sqlite_classes` (see `wrangler.jsonc` and `drizzle.config.ts`).

Note: All SQLite access goes through the data access layer (DAL) in `src/do/repo/db/dal.ts`. Avoid raw drizzle queries outside the DAL.

## R2 storage

- Large, cheap object store for the data plane
- Stores under a per-DO prefix: `do/<do-id>/...`
- Objects:
  - Pack files: `do/<id>/objects/pack/<name>.pack`
  - Pack indexes: `do/<id>/objects/pack/<name>.idx` — authoritative for pack membership and object lookup
- Access patterns:
  - Range reads for packfile assembly (cheap and efficient)
  - `.idx` fanout reads for object discovery and location

## Key conventions (src/keys.ts)

- `repoKey(owner, repo)` → `owner/repo`
- `doPrefix(doId)` → `do/<do-id>`
- `r2PackKey(prefix, name)` → `do/<id>/objects/pack/<name>.pack`
- `packIndexKey(packKey)` maps `.pack` → `.idx`
- `packKeyFromIndexKey(idxKey)` maps `.idx` → `.pack`
- `r2PackDirPrefix(prefix)` → `do/<id>/objects/pack/`

## Why this design

- DO provides strong consistency for refs and state transitions (e.g., atomic ref updates during push)
- R2 provides cheap, scalable storage for pack data, with range-read support ideal for fetch assembly
- Streaming receive writes packs directly to R2 with atomic metadata commit — no intermediate buffering or unpacking
