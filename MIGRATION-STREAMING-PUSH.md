# Streaming Push Migration Guide

Operator runbook for the streaming-by-default cutover. This document covers prerequisites, the upgrade path, cutover deploy steps, post-deploy verification, rollback procedure, monitoring, and the future closure deploy.

## Prerequisites

Before deploying the cutover commit, verify:

1. **Queue binding exists.** `wrangler.jsonc` must have a `REPO_MAINT_QUEUE` producer binding pointing at a Cloudflare Queue (e.g., `git-on-cloudflare-repo-maint`). This queue drives background compaction. If missing, streaming repos will accept pushes but compaction will never run, and active pack count will grow unbounded.

2. **Compatibility date.** The `compatibility_date` in `wrangler.jsonc` should be recent enough to support Durable Object RPC, SQLite, and Queue consumers. Check that it matches the Vitest pool config.

3. **You are at phase 4 or later.** The cutover commit requires all of phases 1-4 to be deployed. Check the table below.

## 1. Upgrade Path by Starting Version

| Upgrading from                               | Required steps                                                                                    |
| -------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| Before `c202a4a` (pre-phase-1)               | Deploy phases 1-4 first (`a76650c`). Run repos through phase 1-4 validation gates before cutover. |
| Between `c202a4a` and `a76650c` (phases 1-3) | Deploy `a76650c` (phase 4) first. Verify compaction works. Then deploy cutover.                   |
| At `a76650c` (phase 4, current)              | Deploy cutover directly.                                                                          |

**Do NOT skip intermediate phases.** Each validates correctness for the next:

- Phase 1 seeds `pack_catalog` and validates pack-first reads
- Phase 2 cuts over all read paths to pack-first
- Phase 3 enables streaming receive for canary repos
- Phase 4 enables queue-driven compaction

**How to deploy a specific phase:** Check out the target commit, then deploy:

```bash
git checkout a76650c   # or whichever target commit
npm install && npx wrangler deploy
```

## 2. Cutover Deploy

### Credentials

Admin endpoints use owner Basic auth: the username is the owner name and the password is the owner's auth token (as configured in the auth Durable Object). If centralized auth is not enabled, credentials are not required.

```bash
# Set these once per session:
export DOMAIN="your-domain.example.com"
export OWNER="your-owner"
export AUTH_TOKEN="your-owner-auth-token"
```

### Primary procedure

1. **Deploy the cutover commit.**

   ```bash
   git checkout <cutover-branch-or-commit>
   npm install && npx wrangler deploy
   ```

2. **Verify new repos default to streaming.** Create a throwaway repo and check its mode:

   ```bash
   curl -s -u "$OWNER:$AUTH_TOKEN" \
     "https://$DOMAIN/test-owner/test-repo/admin/storage-mode" | jq .currentMode
   # Expected: "streaming"
   ```

3. **Check for shadow-read auto-normalization.** Any repo that was on `shadow-read` will normalize to `streaming` on its next DO access. Look for the structured log message `mode:normalize-shadow-read` in your Workers logs. If you had repos on `shadow-read`, you should see these within minutes of the first request to each repo.

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

   Confirm the response has `"status": "ok"` and `"currentMode": "streaming"`.

5. **For repos that need rollback safety before promotion**, prepare backfill first:

   ```bash
   # Queue rollback compatibility backfill
   curl -s -X POST -u "$OWNER:$AUTH_TOKEN" \
     "https://$DOMAIN/$OWNER/$REPO/admin/storage-mode/backfill"
   # Expected: {"status": "queued", "jobId": "<uuid>", "targetPacksetVersion": <n>}
   ```

   Backfill is async — it processes objects in batches via the maintenance queue. Poll until complete before proceeding:

   ```bash
   while true; do
     status=$(curl -s -u "$OWNER:$AUTH_TOKEN" \
       "https://$DOMAIN/$OWNER/$REPO/admin/storage-mode" | jq -r '.rollbackCompat.status')
     echo "backfill status: $status"
     [ "$status" = "ready" ] && break
     sleep 5
   done
   ```

   Then promote:

   ```bash
   curl -s -X PUT -u "$OWNER:$AUTH_TOKEN" \
     "https://$DOMAIN/$OWNER/$REPO/admin/storage-mode" \
     -H "Content-Type: application/json" \
     -d '{"mode": "streaming"}'
   ```

### Special cases

- **Truly empty repos** (no refs, no packs, `packsetVersion === 0`): already default to `streaming`. No action needed. They return 503 on fetch because there's nothing to serve — this is expected.

- **Non-empty zero-pack repos** (refs exist but no active packs — e.g., loose-only repos from non-standard seeding): these **cannot** be promoted to `streaming`. The admin `PUT` endpoint will reject the transition with a blocker. The operator must either push a pack to the repo first (via `git push` while in `legacy` mode, which creates a pack) or purge and recreate the repo.

- **Cold repos with no stored mode key**: existing repos that predate the `repoStorageMode` key default to `legacy` if they have any data signal (`packsetVersion > 0`, refs, `lastPackKey`, or `packList`), or `streaming` if truly empty. This prevents silent auto-promotion. Operators must manually promote data-bearing repos.

### Post-deploy verification checklist

- [ ] New repo defaults to `streaming` (`GET /admin/storage-mode` returns `currentMode: "streaming"`)
- [ ] Repos previously on `shadow-read` show `currentMode: "streaming"` (check logs for `mode:normalize-shadow-read`)
- [ ] `git push` to a streaming repo succeeds and creates a pack in R2
- [ ] `git clone` from a streaming repo succeeds
- [ ] `GET /admin/storage-mode` for promoted repos shows `activePackCount > 0` and no blockers
- [ ] Workers logs show no unexpected errors on push or fetch paths
- [ ] Repos left on `legacy` mode still accept pushes and serve fetches normally

### Key fields in `GET /admin/storage-mode` response

| Field                   | What to check                                                                             |
| ----------------------- | ----------------------------------------------------------------------------------------- |
| `currentMode`           | `"streaming"` for promoted repos, `"legacy"` for repos still in rollback mode             |
| `activePackCount`       | Should be > 0 for non-empty repos                                                         |
| `canChange`             | `true` if mode transitions are unblocked                                                  |
| `blockers`              | Empty array when healthy; non-empty explains why transitions are blocked                  |
| `receiveActive`         | `true` during an active push — mode changes are blocked                                   |
| `compactionActive`      | `true` during compaction — mode changes are blocked                                       |
| `rollbackCompat.status` | `"ready"` if backfill is prepared; `"stale"` if a push/compaction happened after backfill |

## 3. Rollback Procedure

Per-repo rollback does not require a full deploy rollback.

### When to roll back

- Persistent push failures (503s, pack write errors) on a streaming repo
- Data corruption signals: fetches return unexpected errors or wrong data
- Compaction stuck or failing repeatedly (check Workers logs for compaction errors)

### Steps

1. **Prepare rollback compatibility data** (if not already prepared):

   ```bash
   curl -s -X POST -u "$OWNER:$AUTH_TOKEN" \
     "https://$DOMAIN/$OWNER/$REPO/admin/storage-mode/backfill"
   # Expected: {"status": "queued", "jobId": "<uuid>", "targetPacksetVersion": <n>}
   ```

2. **Wait for backfill to complete.** Backfill is async — poll until `rollbackCompat.status` is `"ready"`:

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

4. The repo immediately starts using legacy receive/unpack/hydration paths. The legacy alarm will schedule unpack and hydration work as needed.

### Critical constraint: backfill staleness

Rollback backfill is keyed to the current `packsetVersion`. **Any push or compaction after backfill preparation makes the backfill data stale.** If you plan to keep rollback as an option for a repo:

- Prepare backfill _before_ promoting to streaming, OR
- Re-run backfill after promoting but _before_ any new pushes land, OR
- Accept that after a push to a streaming repo, you must re-run backfill before rolling back.

The `GET /admin/storage-mode` response shows `rollbackCompat.status` — check for `"ready"` vs `"stale"`.

### Full deploy rollback

If the cutover commit itself is broken, a full deploy rollback to `a76650c` (phase 4) is available as a last resort. This is not required for per-repo rollback.

## 4. Monitoring

### Log messages to watch

| Log message                       | Level | Meaning                                                                             |
| --------------------------------- | ----- | ----------------------------------------------------------------------------------- |
| `mode:normalize-shadow-read`      | info  | A repo on `shadow-read` was auto-normalized to `streaming` — expected after cutover |
| `receive:finalize-committed`      | info  | Streaming push committed successfully                                               |
| `receive:aborted`                 | info  | Streaming push was aborted (client disconnect, validation failure)                  |
| `compaction:commit`               | info  | Compaction completed and committed                                                  |
| `compaction:alarm-rearm-failed`   | warn  | Queue re-arm failed — compaction delayed but `compactionWantedAt` is durable        |
| `admin:compaction-enqueue-failed` | warn  | Admin-triggered compaction queue delivery failed                                    |

### Per-repo mode check

There is no bulk "list all repos by mode" endpoint. To audit mode across repos, check each one individually via `GET /:owner/:repo/admin/storage-mode`. The owner registry (`GET /:owner/admin/registry`) can help enumerate repos under an owner.

### Verifying compaction is working

After the first push to a streaming repo, compaction runs when the active pack count exceeds the fan-in threshold (4). Check Workers logs for `compaction:commit` within a few minutes of a qualifying push. If absent:

1. Verify the queue binding exists: `npx wrangler queues list`
2. Check for `compaction:alarm-rearm-failed` in logs
3. Manually trigger compaction: `POST /:owner/:repo/admin/compact` with `{"dryRun": false}`

### Rollback window duration

There is no hard rule. A reasonable approach:

- Keep the rollback window open for at least one full push/fetch cycle across all active repos.
- Monitor for at least a few days of normal traffic before proceeding to the closure release.
- The rollback window costs nothing except keeping legacy code paths in the codebase.

## 5. Closure Deploy (future)

After the rollback window closes:

1. Confirm all repos stable on `streaming` for the window duration.
2. Deploy the closure release commit.
3. `/admin/hydrate`, `/admin/storage-mode/backfill`, and `PUT /admin/storage-mode` are removed.
4. Rollback to pre-streaming is no longer possible.
5. All repos are implicitly streaming.

See `docs/streaming-push.md` for the detailed closure release implementation checklist.
