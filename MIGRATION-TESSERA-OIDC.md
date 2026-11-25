# OIDC Ownership Migration Guide

Operator runbook for upgrading an existing git-on-cloudflare deployment from legacy owner tokens to OIDC browser sign-in, D1-backed repository ownership, and personal access tokens.

This guide is primarily for people running their own fork or clone. The upstream deployment has already completed this migration.

The implementation and variable names say `tessera` because upstream uses tessera as its OIDC provider. You can use another OIDC provider if it supports standard OIDC discovery, authorization code + PKCE, confidential clients, signed ID tokens, and a stable opaque `sub` claim. For automatic namespace claim/import, the provider should also emit a slug-safe `preferred_username` claim; otherwise, you must seed namespaces/memberships/repositories in D1 manually.

## Prerequisites

1. **Streaming push migration is complete.** If upgrading from a pre-streaming deployment, finish `MIGRATION-STREAMING-PUSH.md` first. Do not combine the storage migration and the OIDC ownership migration in one jump.

2. **Back up production state.** Before the final closure deploy, take backups or exports of D1, Auth Durable Object data if you have an operator path for it, and any deployment secrets you still need. The phase 3 closure deletes `AuthDurableObject` storage.

3. **OIDC client is registered.** Register git-on-cloudflare as an OIDC relying party with your provider.
   - Redirect URI: `https://<your-goc-host>/auth/callback`
   - Local dev redirect URI, if needed: `http://127.0.0.1:<port>/auth/callback` or the Vite URL printed by `npm run dev`
   - Scopes: `openid profile email`
   - Flow: authorization code with PKCE

4. **Provider claims are suitable.**
   - `sub` must be stable and opaque. git-on-cloudflare stores it in `users.tessera_sub` even if the provider is not tessera.
   - `preferred_username` should be a lowercase ASCII slug or at least normalize to one. It is used only as a namespace claim candidate, not as identity proof.

5. **Cloudflare bindings exist.** The post-closure application uses:
   - D1 binding `DB`
   - KV binding `ROUTES`
   - Queue binding `REPO_TASKS_QUEUE`
   - Durable Object binding `REPO_DO`
   - R2 binding `REPO_BUCKET`

   During phase 1 and early phase 2, do not remove the legacy `AUTH_DO` Durable Object binding or `OWNER_REGISTRY` KV binding yet. Phase 1 needs them for legacy owner-token compatibility and automatic repository backfill. They are removed only in phase 3.

## 1. Upgrade Path By Starting Version

| Upgrading from                | Required steps                                                                                                                                                                      |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Before `b70097c` (pre-OIDC)   | Deploy phase 1 (`b70097c`) first. Configure D1, ROUTES KV, OIDC/session secrets, then validate sign-in, namespace claim, and legacy backfill before continuing.                     |
| `b70097c` to before `b274ea0` | Deploy phase 2 first (`b274ea0`, then preferably `7e1cdb2`). Validate resolver, PAT Git access, admin session auth, private repo behavior, route-cache sync, and repository delete. |
| `b274ea0` / `7e1cdb2`         | Finish phase 2 validation, make sure active push users have PATs, then deploy phase 3 closure (`b894c74`).                                                                          |
| `b894c74` or newer            | You are on the post-closure auth model. Make sure D1 migrations and OIDC secrets are configured. Legacy owner-token data is already gone.                                           |
| Fresh deployment              | Deploy latest directly. Configure OIDC, sign in, create repositories from `/auth/account`, and create PATs for Git push.                                                            |

**Do not skip phase 1 or phase 2 on an existing deployment.** Latest `main` no longer contains AuthDO routes, legacy owner registry backfill, or legacy push-token compatibility. If you deploy latest directly from a pre-OIDC instance, your existing repos may not have D1 ownership rows and your old owner tokens will not work.

**How to deploy a specific phase:**

```bash
git checkout b70097c   # phase 1, identity + D1 + backfill
npm install
npm run db:migrate
npx wrangler deploy
```

Repeat with the next target commit after each validation gate passes.

## 2. Configure Cloudflare Resources

### D1

Create a D1 database for global ownership metadata and update `wrangler.jsonc` with your real `database_id`.

```bash
npx wrangler d1 create git-on-cloudflare
npm run db:migrate
```

The D1 binding must be named `DB`. It stores users, namespaces, repository route rows, PATs, and grants. RepoDO SQLite remains separate and still stores per-repository refs and pack catalog metadata.

### ROUTES KV

Create a KV namespace for route candidates and update `wrangler.jsonc`.

```bash
npx wrangler kv namespace create ROUTES
```

`ROUTES` stores records shaped like `repo-route:v1:<namespace>/<repo> -> { repositoryId, namespaceId, doName, updatedAt }`. It must not store repository visibility. D1 is the source of truth for existence, visibility, memberships, and PAT grants.

### Queue

Keep or create the queue used by repo tasks:

```bash
npx wrangler queues create git-on-cloudflare-repo-maint
```

The current binding is `REPO_TASKS_QUEUE`. The physical queue name may remain `git-on-cloudflare-repo-maint` for continuity.

Phase 1 (`b70097c`) and the first phase 2 commit (`b274ea0`) still call this producer binding `REPO_MAINT_QUEUE`. The phase 2 follow-up (`7e1cdb2`) renames the binding to `REPO_TASKS_QUEUE` without requiring a new physical queue. When checking out each phase commit, keep the binding name that commit's `wrangler.jsonc` expects.

### Secrets And Vars

Set production secrets:

```bash
wrangler secret put SESSION_SECRET
wrangler secret put TESSERA_OIDC_CLIENT_ID
wrangler secret put TESSERA_OIDC_CLIENT_SECRET
```

Set the issuer as a var or secret:

```bash
TESSERA_OIDC_ISSUER=https://auth.limic.dev
```

If you use another OIDC provider, keep the same environment variable names unless you patch the code. `TESSERA_OIDC_ISSUER` should point at the issuer root that serves `/.well-known/openid-configuration`.

For local development, copy `.dev.vars.example` to `.dev.vars` and set local provider values. Loopback `http://localhost` issuers are allowed only for local development.

## 3. Phase 1 Deploy (`b70097c`)

Phase 1 adds OIDC sign-in, D1 ownership tables, local sealed browser sessions, PAT management, and automatic legacy backfill. Git and public UI traffic still use the old route model during this phase.

### Procedure

1. Check out and deploy phase 1:

   ```bash
   git checkout b70097c
   npm install
   npm run db:migrate
   npx wrangler deploy
   ```

2. Open `/auth` and sign in with your OIDC provider.

3. Confirm `/auth/account` loads and shows your user id, namespace list, repositories, and PAT management UI.

4. On first sign-in, git-on-cloudflare attempts to claim a namespace from `preferred_username`. If the claim succeeds, it enqueues legacy backfill for repos whose old owner slug matches that namespace.

5. Watch Workers logs for successful OIDC and backfill events:
   - `oidc:callback-success`
   - `backfill:*`
   - `routes:put-ok`

### Phase 1 Validation

- [ ] `GET /auth` redirects signed-in users to `/auth/account`
- [ ] A row exists in D1 `users` for your OIDC `sub`
- [ ] A namespace row exists for your expected owner slug
- [ ] Your user has a `namespace_memberships` row for that namespace
- [ ] Existing legacy repos for that owner appear as D1 `repositories` rows
- [ ] `repositories.do_name` for imported repos is `<legacy-owner>/<repo>`
- [ ] `ROUTES` entries exist for public imported repos
- [ ] Existing clone/fetch behavior still works
- [ ] Existing legacy push tokens still work during compatibility
- [ ] You can create, list, and revoke a PAT from `/auth/account`

### Phase 1 Recovery

If automatic namespace claim does not happen:

- Check the `oidc:callback-success` log entry: `claimedNamespaceSlug` is non-null when a claim succeeded.
- Confirm your provider emits `preferred_username` or `email`. Providers such as Google omit `preferred_username`, so the email local-part is used as a fallback candidate.
- Confirm the value normalizes to a claimable slug under `src/shared/slugs.ts`; reserved names (`auth`, `api`, `_cache`, ...) are skipped and numeric suffixes are tried for collisions.
- If the account already exists without a namespace, sign in again. The claim is retried whenever the account has no namespace, not only on first user insert.
- If every candidate is taken, open `/auth/account` and use the claim form. Manual D1 seeding is no longer required.

If backfill stalls:

- Re-send the legacy backfill queue payload from the Cloudflare dashboard if you are still on phase 1/2 compatibility code.
- Keep the payload idempotent: same `userId`, same `namespaceSlug`, and the last cursor if you have one.
- Do not expose backfill as an end-user product workflow.

## 4. Phase 2 Deploy (`b274ea0`, Then `7e1cdb2`)

Phase 2 moves repo-serving routes onto D1-backed route resolution and authorization while compatibility fallback still exists. It also makes PATs the Git credential model, adds repository creation, enforces private/public visibility, moves admin to session membership, and hardens cache behavior.

### Procedure

1. Deploy phase 2:

   ```bash
   git checkout b274ea0
   npm install
   npm run db:migrate
   npx wrangler deploy
   ```

2. Prefer deploying the phase 2 follow-up before broad validation:

   ```bash
   git checkout 7e1cdb2
   npm install
   npm run db:migrate
   npx wrangler deploy
   ```

3. Sign in at `/auth`, create a PAT with `push` access for your namespace or a test repo, and store the plaintext token. It is shown once.

4. Test Git with Basic auth:

   ```bash
   git -c http.extraHeader='Authorization: Basic <base64(namespace:goc_prefix_secret)>' \
     ls-remote https://<your-goc-host>/<namespace>/<repo>
   ```

5. Test a push with a `push` PAT.

6. Test that a `pull` PAT can fetch but cannot push.

7. Test private repo behavior by flipping a repo to private from the admin/account UI and validating anonymous browse/clone no longer discloses it.

### Phase 2 Validation

- [ ] Public repo anonymous browse and clone still work
- [ ] Private repo anonymous browse returns non-disclosing 404
- [ ] Namespace member can browse private repo after sign-in
- [ ] Private repo clone/fetch works with a PAT grant
- [ ] Push requires PAT `level = "push"`
- [ ] Push with PAT `level = "pull"` fails
- [ ] Basic username must match the namespace slug
- [ ] `/auth/account` can create a new repository
- [ ] Newly created repos use opaque `doName = "repo:<id-suffix>"`
- [ ] Existing imported repos still use `doName = "<legacy-owner>/<repo>"`
- [ ] `/:owner/:repo/admin` requires a signed-in session and namespace membership
- [ ] PAT credentials do not authorize web admin routes
- [ ] Private responses use `Cache-Control: no-store`
- [ ] Public-to-private transition does not serve stale public cache entries
- [ ] Route-cache sync messages converge `ROUTES` after create or visibility changes
- [ ] Repository delete queues and completes if you use that admin action

### Cutover Readiness Checklist

Do not deploy phase 3 until all are true:

- [ ] Every active repository has a D1 `repositories` row
- [ ] Active push users have created PATs
- [ ] You have verified at least one clone/fetch and one push with PATs
- [ ] Admin UI works through OIDC session membership
- [ ] No normal traffic depends on legacy AuthDO owner tokens
- [ ] Workers logs do not show unresolved route/backfill errors for active repos
- [ ] You understand that the next phase deletes AuthDO storage

## 5. Phase 3 Closure Deploy (`b894c74`)

Phase 3 removes AuthDO, legacy owner registry compatibility, legacy route fallback, legacy backfill, and legacy owner-token auth. Existing imported repos continue to work only because their D1 rows store the old `doName = "<owner>/<repo>"`.

> **WARNING:** This closure is destructive. The `AuthDurableObject` delete migration removes stored AuthDO data. After this deploy, old owner tokens are gone and rollback to pre-OIDC auth is not a normal operational path.

### Procedure

1. Confirm the cutover readiness checklist above.

2. Deploy phase 3:

   ```bash
   git checkout b894c74
   npm install
   npm run db:migrate
   npx wrangler deploy
   ```

3. Verify `wrangler.jsonc` includes the ordered delete migration for `AuthDurableObject` and no longer binds `AUTH_DO` or `OWNER_REGISTRY`.

4. Run post-closure validation.

### Post-Closure Validation

- [ ] `/auth` is the OIDC sign-in/account entry point
- [ ] Legacy `/auth/legacy` and owner-token admin routes are absent
- [ ] `AuthDurableObject` is not exported by the Worker
- [ ] Anonymous public clone/fetch works for public repos with valid route cache entries
- [ ] Authenticated/PAT access can resolve repos through D1 fallback on KV miss
- [ ] Anonymous KV miss returns 404 instead of scanning D1
- [ ] Imported legacy repos with D1 rows still fetch and push through PATs
- [ ] New `repo:<uuid>` repos fetch, push, browse, and show admin state correctly
- [ ] Private repos remain non-disclosing to anonymous users
- [ ] Workers logs show no unexpected `git-acl:*`, `route:*`, or `oidc:*` failures

## 6. Deploying Latest After Closure

After `b894c74` has been deployed and validated, you can return to the current branch and deploy normally:

```bash
git checkout main
npm install
npm run db:migrate
npm run build
npx wrangler deploy
```

For a fresh deployment, latest `main` is the right starting point. There is no legacy import path in latest `main`; create repos from `/auth/account` after signing in, then create PATs for Git push.

## 7. Operational Notes

### OIDC Provider Compatibility

The code uses `openid-client` and should work with a standard OIDC provider. The required contract is:

- Issuer exposes OIDC discovery at `/.well-known/openid-configuration`
- Authorization endpoint supports authorization code + PKCE S256
- Token endpoint returns a signed ID token
- ID token validates for the configured client id audience
- ID token includes stable `sub`
- ID token should include `preferred_username` if you want first-login namespace claim

If using Auth0, Keycloak, Dex, Zitadel, Google Workspace, or another provider, map their user handle claim into `preferred_username` if possible. Keep it stable and slug-safe. Do not use email addresses as namespace slugs unless you intentionally transform them into the repository slug policy.

### Credentials After Migration

Git uses HTTP Basic:

- Username: namespace slug
- Password: goc PAT plaintext

`level = "push"` includes pull access. `level = "pull"` can clone/fetch but cannot push. PATs never grant web admin access.

### Cache And Privacy

`ROUTES` KV is a route accelerator and scanner shield, not an authorization source. D1 visibility and authorization checks must happen before any shared cache read. A stale KV entry or local Cache API entry must not preserve public access after a repo becomes private.

### Rollback Posture

Before phase 3, normal deploy rollback is available if you have not applied the AuthDO delete migration. After phase 3, treat failures as forward-fix unless you have an external backup and a deliberate restore plan. The production-safe rollback boundary is phase 2, not latest `main`.

## 8. Useful Commands

```bash
# Apply D1 migrations to production
npm run db:migrate

# Apply D1 migrations locally
npm run db:migrate:local

# Generate Worker binding types from local env placeholders
npm run types:generate

# Typecheck
npm run typecheck

# Worker integration tests
npm run test:workers
```

Targeted validation commands:

```bash
npm run test:auth
npx vitest run --config vitest.config.ts test/git-acl.worker.test.ts
npx vitest run --config vitest.config.ts test/repository-resolver.worker.test.ts
npx vitest run --config vitest.config.ts test/pat-verify.worker.test.ts
npx vitest run --config vitest.config.ts test/cache-policy.worker.test.ts
```

## Appendix: What The Closure Release Removed

- Auth Durable Object routes, state, binding, export, and tests
- Legacy owner-token Git push auth
- Legacy owner registry listing via `OWNER_REGISTRY`
- Automatic legacy backfill queue handler
- Legacy route fallback from missing D1 rows to reconstructed `owner/repo`
- AuthDO admin UI and compatibility routes
- `AUTH_ADMIN_TOKEN` and AuthDO-oriented configuration

The post-closure system uses: OIDC browser sessions, D1 users/namespaces/repositories/PATs, route-cache KV candidates, session-membership admin authorization, PAT-based Git authorization, RepoDO metadata authority, and R2 pack/object storage.
