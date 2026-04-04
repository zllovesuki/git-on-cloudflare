# Streaming Push Migration Guide

Operator runbook covering the streaming push rollout lifecycle: cutover deploy, post-cutover verification, rollback procedure, and the final closure deploy.

## Prerequisites

1. **Queue binding exists.** `wrangler.jsonc` must have a `REPO_MAINT_QUEUE` producer binding pointing at a Cloudflare Queue (e.g., `git-on-cloudflare-repo-maint`). This queue drives background compaction. If missing, streaming repos will accept pushes but compaction will never run, and active pack count will grow unbounded.

2. **Compatibility date.** The `compatibility_date` in `wrangler.jsonc` should be recent enough to support Durable Object RPC, SQLite, and Queue consumers. Check that it matches the Vitest pool config.

## 1. Upgrade Path by Starting Version

| Upgrading from                               | Required steps                                                                                    |
| -------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| Before `c202a4a` (pre-phase-1)               | Deploy phases 1-4 first (`a76650c`). Run repos through phase 1-4 validation gates before cutover. |
| Between `c202a4a` and `a76650c` (phases 1-3) | Deploy `a76650c` (phase 4) first. Verify compaction works. Then deploy cutover.                   |
| At `a76650c` (phase 4)                       | Deploy cutover (`98ad7dd`) directly.                                                              |
| At `98ad7dd` (cutover, validated)            | Deploy closure directly.                                                                          |
| Fresh deployment (no existing data)          | Deploy latest directly. All repos are streaming by default.                                       |

**Do NOT skip the cutover release.** Deploying the closure release on a pre-cutover codebase will leave repos in an unrecoverable state. The cutover commit (`98ad7dd`) must be deployed and validated first.

**How to deploy a specific phase:** Check out the target commit, then deploy:

```bash
git checkout 98ad7dd   # cutover
npm install && npx wrangler deploy
```

## 2. Cutover Deploy (`98ad7dd`)

> **Historical note:** If you have already deployed and validated the cutover, skip to Section 4 (Closure Deploy).

### Credentials

Admin endpoints use owner Basic auth: the username is the owner name and the password is the owner's auth token (as configured in the auth Durable Object). If centralized auth is not enabled, credentials are not required.

```bash
# Set these once per session:
export DOMAIN="your-domain.example.com"
export OWNER="your-owner"
export AUTH_TOKEN="your-owner-auth-token"
```

### Primary procedure

> **Historical (pre-closure):** The steps below reference `/admin/storage-mode` endpoints which were removed in the closure release. This procedure is only relevant when deploying the cutover commit (`98ad7dd`).

1. **Deploy the cutover commit.**

   ```bash
   git checkout 98ad7dd
   npm install && npx wrangler deploy
   ```

2. **Verify new repos default to streaming.** Create a throwaway repo and check its mode:

   ```bash
   curl -s -u "$OWNER:$AUTH_TOKEN" \
     "https://$DOMAIN/test-owner/test-repo/admin/storage-mode" | jq .currentMode
   # Expected: "streaming"
   ```

3. **Check for shadow-read auto-normalization.** Any repo that was on `shadow-read` will normalize to `streaming` on its next DO access. Look for `mode:normalize-shadow-read` in Workers logs.

4. **Inventory and promote existing repos.** Enumerate repos under an owner, then check each one:

   ```bash
   # List repos under an owner:
   curl -s -u "$OWNER:$AUTH_TOKEN" "https://$DOMAIN/$OWNER/admin/registry" | jq '.repos[]'
   ```

   Check each repo's mode:

   ```bash
   export REPO="your-repo"
   curl -s -u "$OWNER:$AUTH_TOKEN" \
     "https://$DOMAIN/$OWNER/$REPO/admin/storage-mode" \
     | jq '{mode: .currentMode, packs: .activePackCount, canChange: .canChange, blockers: .blockers}'
   ```

   For `legacy` repos with active packs (`activePackCount > 0`):

   ```bash
   curl -s -X PUT -u "$OWNER:$AUTH_TOKEN" \
     "https://$DOMAIN/$OWNER/$REPO/admin/storage-mode" \
     -H "Content-Type: application/json" \
     -d '{"mode": "streaming"}'
   ```

5. **For repos that need rollback safety before promotion**, prepare backfill first:

   ```bash
   curl -s -X POST -u "$OWNER:$AUTH_TOKEN" \
     "https://$DOMAIN/$OWNER/$REPO/admin/storage-mode/backfill"
   ```

   Poll until complete, then promote.

### Special cases

- **Truly empty repos** (no refs, no packs, `packsetVersion === 0`): already default to `streaming`. No action needed.

- **Non-empty zero-pack repos** (refs exist but no active packs): these cannot be promoted to `streaming` at cutover. Push a pack first (via `git push` while in `legacy` mode) or purge and recreate.

- **Cold repos with no stored mode key**: existing repos that predate the `repoStorageMode` key default to `legacy` if they have any data signal, or `streaming` if truly empty. Operators must manually promote data-bearing repos.

### Post-cutover verification checklist

- [ ] New repo defaults to `streaming`
- [ ] Repos previously on `shadow-read` show `currentMode: "streaming"`
- [ ] `git push` to a streaming repo succeeds and creates a pack in R2
- [ ] `git clone` from a streaming repo succeeds
- [ ] Promoted repos show `activePackCount > 0` and no blockers
- [ ] Workers logs show no unexpected errors
- [ ] Repos left on `legacy` mode still accept pushes and serve fetches

## 3. Rollback Procedure (cutover only)

> **Note:** Rollback is only available at the cutover release. The closure release removes rollback support entirely.

Per-repo rollback does not require a full deploy rollback.

### When to roll back

- Persistent push failures (503s, pack write errors) on a streaming repo
- Data corruption signals: fetches return unexpected errors or wrong data
- Compaction stuck or failing repeatedly

### Steps

1. **Prepare rollback compatibility data** (if not already prepared):

   ```bash
   curl -s -X POST -u "$OWNER:$AUTH_TOKEN" \
     "https://$DOMAIN/$OWNER/$REPO/admin/storage-mode/backfill"
   ```

2. **Wait for backfill to complete.** Poll until `rollbackCompat.status` is `"ready"`:

   ```bash
   while true; do
     status=$(curl -s -u "$OWNER:$AUTH_TOKEN" \
       "https://$DOMAIN/$OWNER/$REPO/admin/storage-mode" | jq -r '.rollbackCompat.status')
     echo "backfill status: $status"
     [ "$status" = "ready" ] && break
     sleep 5
   done
   ```

3. **Revert to legacy:**

   ```bash
   curl -s -X PUT -u "$OWNER:$AUTH_TOKEN" \
     "https://$DOMAIN/$OWNER/$REPO/admin/storage-mode" \
     -H "Content-Type: application/json" \
     -d '{"mode": "legacy"}'
   ```

### Critical constraint: backfill staleness

Rollback backfill is keyed to `packsetVersion`. Any push or compaction after backfill preparation makes the backfill data stale. Re-run backfill before rolling back if pushes have landed.

### Full deploy rollback

If the cutover commit itself is broken, a full deploy rollback to `a76650c` (phase 4) is available as a last resort.

## 4. Closure Deploy

> **WARNING:** The closure release is destructive and irreversible. It removes all legacy code paths. Rollback to pre-streaming is not possible after deploying closure. The cutover release (`98ad7dd`) is the last safe checkpoint.

### When to deploy closure

- All repos stable on streaming for the rollback window duration
- At least one full push/fetch cycle across all active repos
- No rollback needed for any repo

### Steps

1. **Confirm stability.** Verify all repos are functioning correctly on streaming.

2. **Deploy the closure release.**

   ```bash
   npm install && npx wrangler deploy
   ```

3. After deployment, all repos are implicitly streaming. The storage-mode concept, admin storage-mode endpoints, and hydrate endpoints no longer exist.

### Post-closure verification

- [ ] `git push` to any repo succeeds (with sideband progress output)
- [ ] `git clone` from any repo succeeds
- [ ] Web UI browse, tree, blob, commits, and raw views all work
- [ ] Workers logs show no unexpected errors on push or fetch paths
- [ ] Compaction runs after pushes that exceed the fan-in threshold (check for `compaction:commit` in logs)

## 5. Monitoring

### Log messages to watch

| Log message                       | Level | Meaning                                                         |
| --------------------------------- | ----- | --------------------------------------------------------------- |
| `receive:finalize-committed`      | info  | Push committed successfully                                     |
| `receive:aborted`                 | info  | Push was aborted (client disconnect, validation failure)        |
| `compaction:commit`               | info  | Compaction completed and committed                              |
| `compaction:alarm-rearm-failed`   | warn  | Queue re-arm failed — compaction delayed but request is durable |
| `admin:compaction-enqueue-failed` | warn  | Admin-triggered compaction queue delivery failed                |

### Verifying compaction is working

After pushes to a repo, compaction runs when the active pack count exceeds the fan-in threshold (4). Check Workers logs for `compaction:commit` within a few minutes of a qualifying push. If absent:

1. Verify the queue binding exists: `npx wrangler queues list`
2. Check for `compaction:alarm-rearm-failed` in logs
3. Manually trigger compaction: `POST /:owner/:repo/admin/compact` with `{"dryRun": false}`

## Appendix: What the Closure Release Removed

- `RepoStorageMode` concept and all storage-mode switching (`GET/PUT /admin/storage-mode`)
- Rollback backfill machinery (`POST /admin/storage-mode/backfill`)
- Legacy DO-side receive (`POST /receive` on the DO)
- Background unpack (alarm-driven loose object extraction)
- Hydration (epoch-based pack building) and `/admin/hydrate` endpoints
- Legacy pack tracking (`packList`, `lastPackKey`, `lastPackOids`)
- `isomorphic-git` dependency
- Periodic maintenance (`runMaintenance`)
- Configuration: `REPO_UNPACK_*`, `REPO_KEEP_PACKS`, `REPO_PACKLIST_MAX`, `REPO_DO_MAINT_MINUTES`
- SQLite tables: `pack_objects`, `hydr_cover`, `hydr_pending`
- UI: StorageModeCard admin component

The system now has: streaming receive (always), queue-driven compaction, idle cleanup, and `pack_catalog` as the sole pack metadata authority.
