import { safeParseJsonRequest } from "@/shared/web";
import { validateSlugForRoute } from "@/shared/slugs";
import { json, newPrefixedId } from "@/worker/common";
import {
  findNamespaceBySlug,
  findRepositoryByNamespaceAndSlug,
  insertPatWithGrants,
  listNamespacesForUser,
  listPatsForUser,
  listRepositoriesForUser,
  revokePatById,
} from "@/worker/db/d1/dal";
import { sameOriginViolation } from "@/worker/auth/origin";
import { loadViewer } from "@/worker/auth/session";
import {
  generatePatPlaintext,
  hashPatPlaintext,
  validatePatName,
  viewerIsNamespaceMember,
} from "@/worker/auth/pat";
import type { AppRouter } from "./hono";
import { renderUiDocumentResponse } from "./uiResponse";
import { safeRedirect, summarizeTokens } from "./authShared";
import { PatCreateRequestSchema } from "./requestSchemas";

export function registerAuthTokenRoutes(router: AppRouter) {
  router.get(`/auth/account`, async (c) => {
    const viewer = await loadViewer(c);
    if (!viewer) return safeRedirect(c, "/auth");
    const db = c.var.db;
    const namespaces = await listNamespacesForUser(db, viewer.userId);
    const repositoryRows = await listRepositoriesForUser(db, viewer.userId);
    const tokenRows = await listPatsForUser(db, viewer.userId);
    const tokens = await summarizeTokens(db, tokenRows);
    return renderUiDocumentResponse(
      c.env,
      "account",
      {
        userId: viewer.userId,
        primaryNamespaceSlug: viewer.primaryNamespaceSlug,
        suggestedNamespaceSlug: viewer.suggestedNamespaceSlug,
        namespaces: namespaces.map((ns) => ({ id: ns.id, slug: ns.slug })),
        repositories: repositoryRows.map((row) => ({
          id: row.repository.id,
          slug: row.repository.slug,
          namespaceSlug: row.namespace.slug,
          visibility: row.repository.visibility,
          updatedAt: row.repository.updatedAt,
        })),
        tokens,
      },
      { failureBody: "Failed to render page\n", viewer }
    );
  });

  router.get(`/auth/api/tokens`, async (c) => {
    const viewer = await loadViewer(c);
    if (!viewer) return json({ error: "Unauthorized" }, 401);
    const db = c.var.db;
    const tokens = await listPatsForUser(db, viewer.userId);
    const summaries = await summarizeTokens(db, tokens);
    return json({ tokens: summaries });
  });

  router.post(`/auth/api/tokens`, async (c) => {
    const log = c.var.logFor({ service: "AuthPat" });
    const violation = sameOriginViolation(c);
    if (violation) {
      log.warn("pat:create-same-origin-violation");
      return violation;
    }
    const viewer = await loadViewer(c);
    if (!viewer) {
      log.info("pat:create-not-authenticated");
      return json({ error: "Unauthorized" }, 401);
    }

    // Required tagged-union body shape:
    //   { scope: "namespace", name, namespaceSlug, level }
    //   { scope: "repo", name, namespaceSlug, repoSlug, level }
    // `scope` and `level` are both required so contract drift surfaces as
    // a 400 instead of silently coercing. `level === "push"` includes pull
    // access by construction (see `pat_*_grants.level` CHECK).
    const rawBody = await safeParseJsonRequest(c.req.raw);
    const parsedBody = PatCreateRequestSchema.safeParse(rawBody);
    if (!parsedBody.success) {
      log.warn("pat:create-invalid-scope");
      return json({ error: "Body must include scope: 'namespace' or 'repo'" }, 400);
    }
    const body = parsedBody.data;

    const nameValidation = validatePatName(body.name);
    if (!nameValidation.ok) {
      log.warn("pat:create-invalid-name", { reason: nameValidation.reason });
      return json({ error: "Invalid token name" }, 400);
    }
    const level = body.level;
    if (level === null) {
      log.warn("pat:create-invalid-level");
      return json({ error: "Body must include level: 'pull' or 'push'" }, 400);
    }
    const slugValidation = validateSlugForRoute(body.namespaceSlug);
    if (!slugValidation.ok) {
      log.warn("pat:create-invalid-namespace-slug", { reason: slugValidation.reason });
      return json({ error: "Invalid namespace slug" }, 400);
    }
    let repoSlug: string | null = null;
    if (body.scope === "repo") {
      const repoSlugValidation = validateSlugForRoute(body.repoSlug);
      if (!repoSlugValidation.ok) {
        log.warn("pat:create-invalid-repo-slug", { reason: repoSlugValidation.reason });
        return json({ error: "Invalid repo slug" }, 400);
      }
      repoSlug = repoSlugValidation.slug;
    }
    const db = c.var.db;
    const namespace = await findNamespaceBySlug(db, slugValidation.slug);
    if (!namespace) {
      log.warn("pat:create-namespace-not-found", { namespaceSlug: slugValidation.slug });
      return json({ error: "Namespace not found" }, 404);
    }
    if (!(await viewerIsNamespaceMember(db, viewer.userId, namespace.id))) {
      log.warn("pat:create-not-member", {
        userId: viewer.userId,
        namespaceId: namespace.id,
      });
      return json({ error: "Forbidden" }, 403);
    }

    let repoId: string | null = null;
    if (body.scope === "repo" && repoSlug !== null) {
      const repository = await findRepositoryByNamespaceAndSlug(db, namespace.id, repoSlug);
      if (!repository) {
        log.warn("pat:create-repo-not-found", {
          namespaceId: namespace.id,
          repoSlug,
        });
        return json({ error: "Repo not found" }, 404);
      }
      repoId = repository.id;
    }

    const generated = generatePatPlaintext();
    const hash = await hashPatPlaintext(generated.plaintext);
    const now = Date.now();
    const patId = newPrefixedId("pat");
    await insertPatWithGrants(db, {
      pat: {
        id: patId,
        userId: viewer.userId,
        name: nameValidation.name,
        prefix: generated.publicPrefix,
        hash,
        createdAt: now,
        expiresAt: null,
        revokedAt: null,
        lastUsedAt: null,
      },
      namespaceGrants:
        body.scope === "namespace" ? [{ patId, namespaceId: namespace.id, level }] : [],
      repoGrants: body.scope === "repo" && repoId !== null ? [{ patId, repoId, level }] : [],
    });
    log.info("pat:create-ok", {
      userId: viewer.userId,
      patId,
      prefix: generated.publicPrefix,
      scope: body.scope,
      namespaceSlug: slugValidation.slug,
      repoSlug,
      level,
    });
    return json({ id: patId, plaintext: generated.plaintext, prefix: generated.publicPrefix });
  });

  router.delete(`/auth/api/tokens/:patId`, async (c) => {
    const log = c.var.logFor({ service: "AuthPat" });
    const violation = sameOriginViolation(c);
    if (violation) {
      log.warn("pat:revoke-same-origin-violation");
      return violation;
    }
    const viewer = await loadViewer(c);
    if (!viewer) return json({ error: "Unauthorized" }, 401);
    const patId = c.req.param("patId");
    const result = await revokePatById(c.var.db, patId, viewer.userId, Date.now());
    if (result.ok) {
      log.info("pat:revoke-ok", { userId: viewer.userId, patId });
      return json({ ok: true });
    }
    if (result.reason === "not-owner") {
      log.warn("pat:revoke-not-owner", { userId: viewer.userId, patId });
      return json({ error: "Forbidden" }, 403);
    }
    if (result.reason === "not-found") {
      log.warn("pat:revoke-not-found", { userId: viewer.userId, patId });
      return json({ error: "Not found" }, 404);
    }
    // Re-revoke is idempotent; surface as a 200 but record it for visibility.
    log.debug("pat:revoke-already-revoked", { userId: viewer.userId, patId });
    return json({ ok: true });
  });
}
