import { safeParseJsonRequest } from "@/shared/web";
import { validateSlugForRoute } from "@/shared/slugs";
import { json } from "@/worker/common";
import {
  claimNamespace,
  insertMembershipIfMissing,
  listNamespacesForUser,
} from "@/worker/db/d1/dal";
import { sameOriginViolation } from "@/worker/auth/origin";
import { generateNamespaceId, loadViewer } from "@/worker/auth/session";
import type { AppRouter } from "./hono";
import { NamespaceClaimRequestSchema } from "./requestSchemas";

// Self-service fallback for accounts whose automatic claim could not run
// (for example every candidate was already taken). The sign-in callback is
// still the primary path; this endpoint exists so a dead-end account never
// needs manual D1 surgery.
export function registerAuthNamespaceRoutes(router: AppRouter) {
  router.post(`/auth/api/namespaces`, async (c) => {
    const log = c.var.logFor({ service: "NamespaceClaim" });
    const violation = sameOriginViolation(c);
    if (violation) {
      log.warn("namespace-claim:same-origin-violation");
      return violation;
    }
    const viewer = await loadViewer(c);
    if (!viewer) {
      log.info("namespace-claim:not-authenticated");
      return json({ error: "Unauthorized" }, 401);
    }
    const rawBody = await safeParseJsonRequest(c.req.raw);
    const parsedBody = NamespaceClaimRequestSchema.safeParse(rawBody);
    const requestedSlug = parsedBody.success ? parsedBody.data.slug : "";
    const validation = validateSlugForRoute(requestedSlug);
    if (!validation.ok) {
      log.warn("namespace-claim:invalid-slug", { reason: validation.reason });
      return json({ ok: false, reason: "invalid-slug" } as const, 400);
    }
    const db = c.var.db;
    const existing = await listNamespacesForUser(db, viewer.userId);
    if (existing.length > 0) {
      log.info("namespace-claim:already-claimed", { userId: viewer.userId });
      return json({ ok: false, reason: "already-claimed" } as const, 409);
    }
    const now = Date.now();
    const claimed = await claimNamespace(db, {
      id: generateNamespaceId(),
      slug: validation.slug,
      createdBy: viewer.userId,
      createdAt: now,
    });
    if (!claimed) {
      log.info("namespace-claim:taken", { slug: validation.slug });
      return json({ ok: false, reason: "slug-taken" } as const, 409);
    }
    await insertMembershipIfMissing(db, {
      namespaceId: claimed.id,
      userId: viewer.userId,
      createdAt: now,
    });
    log.info("namespace-claim:ok", {
      userId: viewer.userId,
      namespaceId: claimed.id,
      slug: claimed.slug,
    });
    return json(
      {
        ok: true,
        namespace: { id: claimed.id, slug: claimed.slug },
      } as const,
      201
    );
  });
}
