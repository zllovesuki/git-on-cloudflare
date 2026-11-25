import { env, exports as workerExports } from "cloudflare:workers";
import { beforeAll, describe, expect, it } from "vitest";

import { newPrefixedId } from "@/worker/common";
import { createDb } from "@/worker/db/d1/client";
import {
  claimNamespace,
  findNamespaceBySlug,
  insertMembershipIfMissing,
  insertUserIfNew,
  listNamespacesForUser,
} from "@/worker/db/d1/dal";

import { ensureD1Migrations } from "./util/d1Setup";
import { mintSessionCookie } from "./util/repoSeed";

beforeAll(async () => {
  await ensureD1Migrations(env);
});

type ClaimOk = { ok: true; namespace: { id: string; slug: string } };
type ClaimFail = { ok: false; reason: "invalid-slug" | "slug-taken" | "already-claimed" };

let counter = 0;
function uniqueSlug(prefix: string): string {
  counter += 1;
  return `${prefix}-${counter}-${Math.random().toString(36).slice(2, 8)}`;
}

async function createUser(): Promise<{ userId: string; cookieHeader: string }> {
  const db = createDb(env.DB);
  const userId = newPrefixedId("user");
  await insertUserIfNew(db, {
    id: userId,
    tesseraSub: `t-${userId}`,
    createdAt: Date.now(),
  });
  return { userId, cookieHeader: await mintSessionCookie(env, userId) };
}

async function createUserWithNamespace(
  slug: string
): Promise<{ userId: string; namespaceId: string; cookieHeader: string }> {
  const { userId, cookieHeader } = await createUser();
  const db = createDb(env.DB);
  const claimed = await claimNamespace(db, {
    id: newPrefixedId("ns"),
    slug,
    createdBy: userId,
    createdAt: Date.now(),
  });
  if (!claimed) throw new Error(`namespace ${slug} unexpectedly taken`);
  await insertMembershipIfMissing(db, {
    namespaceId: claimed.id,
    userId,
    createdAt: Date.now(),
  });
  return { userId, namespaceId: claimed.id, cookieHeader };
}

async function postClaim(
  cookieHeader: string | null,
  body: Record<string, unknown>,
  options: { origin?: string | null } = {}
): Promise<{ status: number; payload: ClaimOk | ClaimFail | { error: string } }> {
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  const origin = options.origin === undefined ? "https://example.com" : options.origin;
  if (origin !== null) headers.Origin = origin;
  if (cookieHeader) headers.Cookie = cookieHeader;
  const res = await workerExports.default.fetch("https://example.com/auth/api/namespaces", {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  });
  return {
    status: res.status,
    payload: (await res.json()) as ClaimOk | ClaimFail | { error: string },
  };
}

describe("POST /auth/api/namespaces", () => {
  it("claims a namespace and membership for the signed-in user", async () => {
    const { userId, cookieHeader } = await createUser();
    const slug = uniqueSlug("claim");
    const result = await postClaim(cookieHeader, { slug });
    expect(result.status).toBe(201);
    expect(result.payload).toMatchObject({ ok: true, namespace: { slug } });

    const db = createDb(env.DB);
    const namespace = await findNamespaceBySlug(db, slug);
    expect(namespace?.createdBy).toBe(userId);
    expect((await listNamespacesForUser(db, userId)).map((row) => row.slug)).toEqual([slug]);
  });

  it("requires an authenticated session", async () => {
    const result = await postClaim(null, { slug: uniqueSlug("anon") });
    expect(result.status).toBe(401);
    expect(result.payload).toEqual({ error: "Unauthorized" });
  });

  it("requires a same-origin request", async () => {
    const { cookieHeader } = await createUser();
    const res = await workerExports.default.fetch("https://example.com/auth/api/namespaces", {
      method: "POST",
      headers: { "Content-Type": "application/json", Cookie: cookieHeader },
      body: JSON.stringify({ slug: uniqueSlug("csrf") }),
    });
    expect(res.status).toBe(403);
  });

  it("rejects invalid and reserved slugs", async () => {
    const { cookieHeader } = await createUser();
    const invalid = await postClaim(cookieHeader, { slug: "Bad_Slug!" });
    expect(invalid.status).toBe(400);
    expect(invalid.payload).toEqual({ ok: false, reason: "invalid-slug" });

    const reserved = await postClaim(cookieHeader, { slug: "auth" });
    expect(reserved.status).toBe(400);
    expect(reserved.payload).toEqual({ ok: false, reason: "invalid-slug" });
  });

  it("reports a taken slug without creating a namespace", async () => {
    const slug = uniqueSlug("taken");
    await createUserWithNamespace(slug);
    const { userId, cookieHeader } = await createUser();
    const result = await postClaim(cookieHeader, { slug });
    expect(result.status).toBe(409);
    expect(result.payload).toEqual({ ok: false, reason: "slug-taken" });
    const db = createDb(env.DB);
    expect((await listNamespacesForUser(db, userId)).length).toBe(0);
  });

  it("does not claim a second namespace for an account that already has one", async () => {
    const existing = uniqueSlug("existing");
    const { userId, cookieHeader } = await createUserWithNamespace(existing);
    const result = await postClaim(cookieHeader, { slug: uniqueSlug("second") });
    expect(result.status).toBe(409);
    expect(result.payload).toEqual({ ok: false, reason: "already-claimed" });
    const db = createDb(env.DB);
    expect((await listNamespacesForUser(db, userId)).map((row) => row.slug)).toEqual([existing]);
  });
});
