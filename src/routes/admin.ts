import { AutoRouter } from "itty-router";
import { createLogger, getRepoStub, isValidOid, json, unauthorizedAdminBasic } from "@/common";
import { repoKey } from "@/keys";
import { verifyAuth } from "@/auth";
import { listReposForOwner, addRepoToOwner, removeRepoFromOwner } from "@/registry";
import { isJsonObject, safeParseJsonRequest, type JsonValue } from "@/web";

type RefPayload = {
  name: string;
  oid: string;
};

type HeadPayload = {
  target: string;
  oid?: string;
  unborn?: boolean;
};

type StorageModePayload = {
  mode: string;
};

type AdminRouteRequest = Request & {
  params: { owner: string; repo: string; [key: string]: string };
};

function isRefPayload(value: JsonValue): value is RefPayload {
  return isJsonObject(value) && typeof value.name === "string" && typeof value.oid === "string";
}

function isHeadPayload(value: JsonValue | null): value is HeadPayload {
  return (
    isJsonObject(value) &&
    typeof value.target === "string" &&
    (value.oid === undefined || typeof value.oid === "string") &&
    (value.unborn === undefined || typeof value.unborn === "boolean")
  );
}

function isStorageModePayload(value: JsonValue | null): value is StorageModePayload {
  return isJsonObject(value) && typeof value.mode === "string";
}

export function registerAdminRoutes(router: ReturnType<typeof AutoRouter>) {
  async function handleCompatCompactionPost(
    request: AdminRouteRequest,
    env: Env,
    ctx: ExecutionContext
  ) {
    const { owner, repo } = request.params;
    if (!(await verifyAuth(env, owner, request, true))) {
      return unauthorizedAdminBasic();
    }
    const body = await safeParseJsonRequest(request);
    const dryRun = !isJsonObject(body) || body.dryRun !== false;
    const repoId = repoKey(owner, repo);
    const stub = getRepoStub(env, repoId);
    const log = createLogger(env.LOG_LEVEL, {
      service: "AdminRoutes",
      repoId,
    });
    try {
      const res = dryRun ? await stub.previewCompaction() : await stub.requestCompaction();
      if (!dryRun && res.status === "queued" && res.shouldEnqueue) {
        const queueTask = env.REPO_MAINT_QUEUE.send({
          kind: "compaction",
          doId: stub.id.toString(),
          repoId,
        })
          .then(() => {
            log.info("admin:compaction-enqueue-requested", {
              doId: stub.id.toString(),
            });
          })
          .catch((error) => {
            // The DO has already recorded `compactionWantedAt`, so queue failure
            // delays background work but must not turn an accepted request into
            // a hard HTTP failure.
            log.warn("admin:compaction-enqueue-failed", {
              doId: stub.id.toString(),
              error: String(error),
            });
          });
        ctx.waitUntil(queueTask);
      }

      const status = dryRun || res.status !== "queued" ? 200 : 202;
      return json(res, status, { "Cache-Control": "no-cache" });
    } catch (e) {
      return json({ error: String(e) }, 500);
    }
  }

  async function handleCompatCompactionDelete(request: AdminRouteRequest, env: Env) {
    const { owner, repo } = request.params;
    if (!(await verifyAuth(env, owner, request, true))) {
      return unauthorizedAdminBasic();
    }
    const stub = getRepoStub(env, repoKey(owner, repo));
    try {
      const res = await stub.clearCompactionRequest();
      return json({ ok: true, ...res }, 200, { "Cache-Control": "no-cache" });
    } catch (e) {
      return json({ ok: false, error: String(e) }, 500);
    }
  }

  // Owner registry: list current repos from KV
  router.get(`/:owner/admin/registry`, async (request, env: Env) => {
    const { owner } = request.params;
    if (!(await verifyAuth(env, owner, request, true))) {
      return unauthorizedAdminBasic();
    }
    const repos = await listReposForOwner(env, owner);
    return json({ owner, repos });
  });

  // Both /hydrate and /compact resolve to the same compaction handler.
  // The /hydrate alias is preserved for backward compatibility with existing tooling.
  router.delete(`/:owner/:repo/admin/hydrate`, handleCompatCompactionDelete);
  router.post(`/:owner/:repo/admin/hydrate`, handleCompatCompactionPost);
  router.delete(`/:owner/:repo/admin/compact`, handleCompatCompactionDelete);
  router.post(`/:owner/:repo/admin/compact`, handleCompatCompactionPost);

  // Owner registry: backfill/sync membership
  // POST body: { repos?: string[] } — if provided, (re)validate those; otherwise, revalidate existing KV entries
  router.post(`/:owner/admin/registry/sync`, async (request, env: Env) => {
    const { owner } = request.params as { owner: string };
    if (!(await verifyAuth(env, owner, request, true))) {
      return unauthorizedAdminBasic();
    }
    const input = await safeParseJsonRequest(request);
    let targets =
      isJsonObject(input) && Array.isArray(input.repos)
        ? input.repos.filter((repo): repo is string => typeof repo === "string" && repo.length > 0)
        : [];
    if (targets.length === 0) {
      // revalidate existing KV entries only
      targets = await listReposForOwner(env, owner);
    }
    const updated: { added: string[]; removed: string[]; unchanged: string[] } = {
      added: [],
      removed: [],
      unchanged: [],
    };
    for (const repo of targets) {
      const stub = getRepoStub(env, repoKey(owner, repo));
      // consider present if refs has entries
      let present = false;
      try {
        const refs = await stub.listRefs();
        present = Array.isArray(refs) && refs.length > 0;
      } catch {}
      if (present) {
        await addRepoToOwner(env, owner, repo);
        updated.added.push(repo);
      } else {
        await removeRepoFromOwner(env, owner, repo);
        updated.removed.push(repo);
      }
    }
    return json({ owner, ...updated });
  });

  // Admin refs
  router.get(`/:owner/:repo/admin/refs`, async (request, env: Env) => {
    const { owner, repo } = request.params;
    if (!(await verifyAuth(env, owner, request, true))) {
      return unauthorizedAdminBasic();
    }
    const stub = getRepoStub(env, repoKey(owner, repo));
    try {
      const refs = await stub.listRefs();
      return json(refs);
    } catch {
      return json([]);
    }
  });

  router.put(`/:owner/:repo/admin/refs`, async (request, env: Env) => {
    const { owner, repo } = request.params;
    if (!(await verifyAuth(env, owner, request, true))) {
      return unauthorizedAdminBasic();
    }
    const stub = getRepoStub(env, repoKey(owner, repo));
    const body = await safeParseJsonRequest(request);
    if (!Array.isArray(body)) {
      return new Response("Invalid refs payload\n", { status: 400 });
    }
    const refs = body.filter(isRefPayload);
    if (refs.length !== body.length) {
      return new Response("Invalid refs payload\n", { status: 400 });
    }
    await stub.setRefs(refs);
    return new Response("OK\n");
  });

  // Admin head
  router.get(`/:owner/:repo/admin/head`, async (request, env: Env) => {
    const { owner, repo } = request.params;
    if (!(await verifyAuth(env, owner, request, true))) {
      return unauthorizedAdminBasic();
    }
    const stub = getRepoStub(env, repoKey(owner, repo));
    try {
      const head = await stub.getHead();
      return json(head);
    } catch {
      return new Response("Not found\n", { status: 404 });
    }
  });

  router.put(`/:owner/:repo/admin/head`, async (request, env: Env) => {
    const { owner, repo } = request.params;
    if (!(await verifyAuth(env, owner, request, true))) {
      return unauthorizedAdminBasic();
    }
    const stub = getRepoStub(env, repoKey(owner, repo));
    const body = await safeParseJsonRequest(request);
    if (!isHeadPayload(body)) {
      return new Response("Invalid head payload\n", { status: 400 });
    }
    await stub.setHead(body);
    return new Response("OK\n");
  });

  // Debug: dump DO state (JSON)
  router.get(`/:owner/:repo/admin/debug-state`, async (request, env: Env) => {
    const { owner, repo } = request.params as { owner: string; repo: string };
    if (!(await verifyAuth(env, owner, request, true))) {
      return unauthorizedAdminBasic();
    }
    const stub = getRepoStub(env, repoKey(owner, repo));
    try {
      const state = await stub.debugState();
      return json(state);
    } catch {
      return json({});
    }
  });

  router.get(`/:owner/:repo/admin/storage-mode`, async (request, env: Env) => {
    const { owner, repo } = request.params as { owner: string; repo: string };
    if (!(await verifyAuth(env, owner, request, true))) {
      return unauthorizedAdminBasic();
    }
    const stub = getRepoStub(env, repoKey(owner, repo));
    try {
      return json(await stub.getRepoStorageModeControl(), 200, { "Cache-Control": "no-cache" });
    } catch (error) {
      return json({ error: String(error) }, 500);
    }
  });

  router.put(`/:owner/:repo/admin/storage-mode`, async (request, env: Env) => {
    const { owner, repo } = request.params as { owner: string; repo: string };
    if (!(await verifyAuth(env, owner, request, true))) {
      return unauthorizedAdminBasic();
    }
    const body = await safeParseJsonRequest(request);
    if (!isStorageModePayload(body)) {
      return new Response("Invalid storage mode payload\n", { status: 400 });
    }
    const stub = getRepoStub(env, repoKey(owner, repo));
    try {
      const result = await stub.setRepoStorageModeGuarded(body.mode);
      const status =
        result.status === "ok" ? 200 : result.status === "unsupported_target_mode" ? 400 : 409;
      return json(result, status, { "Cache-Control": "no-cache" });
    } catch (error) {
      return json({ error: String(error) }, 500);
    }
  });

  router.post(`/:owner/:repo/admin/storage-mode/backfill`, async (request, env: Env) => {
    const { owner, repo } = request.params as { owner: string; repo: string };
    if (!(await verifyAuth(env, owner, request, true))) {
      return unauthorizedAdminBasic();
    }

    const repoId = repoKey(owner, repo);
    const stub = getRepoStub(env, repoId);
    try {
      const result = await stub.requestLegacyCompatBackfill();
      if (result.status === "queued" && result.shouldEnqueue) {
        await env.REPO_MAINT_QUEUE.send({
          kind: "legacy-backfill",
          repoId,
          jobId: result.jobId,
          targetPacksetVersion: result.targetPacksetVersion,
        });
      }
      return json(result, result.status === "queued" ? 202 : 200, {
        "Cache-Control": "no-cache",
      });
    } catch (error) {
      return json({ error: String(error) }, 500);
    }
  });

  // Debug: check a specific commit's tree presence
  router.get(`/:owner/:repo/admin/debug-commit/:commit`, async (request, env: Env) => {
    const { owner, repo, commit } = request.params as {
      owner: string;
      repo: string;
      commit: string;
    };
    if (!(await verifyAuth(env, owner, request, true))) {
      return unauthorizedAdminBasic();
    }
    if (!isValidOid(commit)) {
      return new Response("Invalid commit\n", { status: 400 });
    }
    const stub = getRepoStub(env, repoKey(owner, repo));
    try {
      const result = await stub.debugCheckCommit(commit);
      return json(result);
    } catch (e) {
      return json({ error: String(e) }, 500);
    }
  });

  // Debug: check if an OID exists in loose, R2 loose, and/or packs
  router.get(`/:owner/:repo/admin/debug-oid/:oid`, async (request, env: Env) => {
    const { owner, repo, oid } = request.params as {
      owner: string;
      repo: string;
      oid: string;
    };
    if (!(await verifyAuth(env, owner, request, true))) {
      return unauthorizedAdminBasic();
    }
    if (!isValidOid(oid)) {
      return new Response("Invalid OID\n", { status: 400 });
    }
    const stub = getRepoStub(env, repoKey(owner, repo));
    try {
      const result = await stub.debugCheckOid(oid);
      return json(result);
    } catch (e) {
      return json({ error: String(e) }, 500);
    }
  });

  // Admin: Remove a specific pack file
  router.delete(`/:owner/:repo/admin/pack/:packKey`, async (request, env: Env) => {
    const { owner, repo, packKey } = request.params as {
      owner: string;
      repo: string;
      packKey: string;
    };
    if (!(await verifyAuth(env, owner, request, true))) {
      return unauthorizedAdminBasic();
    }

    if (!packKey) {
      return json({ error: "Pack key is required" }, 400);
    }

    const stub = getRepoStub(env, repoKey(owner, repo));
    try {
      const result = await stub.removePack(packKey);
      if (result.rejected) {
        const error =
          result.rejected === "active-pack"
            ? "Active packs cannot be deleted until they are superseded"
            : "Only superseded packs can be deleted through this endpoint";
        return json(
          {
            ok: false,
            error,
            ...result,
          },
          409
        );
      }
      return json({ ok: result.removed, ...result });
    } catch (e) {
      return json({ ok: false, error: String(e) }, 500);
    }
  });

  // Admin: DANGEROUS - completely purge repo (all R2 objects + DO storage)
  router.delete(`/:owner/:repo/admin/purge`, async (request, env: Env) => {
    const { owner, repo } = request.params as { owner: string; repo: string };
    if (!(await verifyAuth(env, owner, request, true))) {
      return unauthorizedAdminBasic();
    }

    // Require explicit confirmation
    const body = await safeParseJsonRequest(request);
    const confirm = isJsonObject(body) && typeof body.confirm === "string" ? body.confirm : "";
    if (confirm !== `purge-${owner}/${repo}`) {
      return json(
        {
          error: "Confirmation required",
          hint: `Set confirm to "purge-${owner}/${repo}"`,
        },
        400
      );
    }

    const stub = getRepoStub(env, repoKey(owner, repo));
    try {
      const result = await stub.purgeRepo();

      // Remove from owner registry
      await removeRepoFromOwner(env, owner, repo);

      return json({ ok: true, ...result });
    } catch (e) {
      return json({ ok: false, error: String(e) }, 500);
    }
  });
}
