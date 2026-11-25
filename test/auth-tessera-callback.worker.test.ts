import { applyD1Migrations } from "cloudflare:test";
import { env, exports as workerExports } from "cloudflare:workers";
import { afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";

import { createDb } from "@/worker/db/d1/client";
import {
  findNamespaceBySlug,
  findUserByTesseraSub,
  insertMembershipIfMissing,
  listNamespacesForUser,
} from "@/worker/db/d1/dal";
import { __test as oidcTest } from "@/worker/auth/oidc";
import { OIDC_TX_COOKIE_HEADER_NAME, SESSION_COOKIE_HEADER_NAME } from "@/worker/auth/cookies";

import { fakeProvider } from "./util/oidcFake";
import { readAppD1Migrations } from "./util/d1Migrations";
import { oidcTransactionCookieHeader } from "./util/authCookies";

beforeAll(async () => {
  await applyD1Migrations(env.DB, readAppD1Migrations());
});

const REDIRECT_URI = "https://example.com/auth/callback";

function preloadProvider() {
  oidcTest.setProviderForTesting(
    {
      issuer: env.TESSERA_OIDC_ISSUER,
      clientId: env.TESSERA_OIDC_CLIENT_ID,
      clientSecret: env.TESSERA_OIDC_CLIENT_SECRET,
    },
    fakeProvider({
      authorizationEndpoint: "https://auth.example.com/oauth2/authorize",
      tokenEndpoint: "https://auth.example.com/oauth2/token",
      jwksUri: "https://auth.example.com/.well-known/jwks.json",
    })
  );
}

async function buildTransactionCookie(state: string, nonce: string, codeVerifier: string) {
  return await oidcTransactionCookieHeader(env.TESSERA_OIDC_CLIENT_SECRET, {
    state,
    nonce,
    codeVerifier,
    redirectUri: REDIRECT_URI,
    createdAt: Date.now(),
  });
}

type FakeClaims = { sub: string; preferred_username?: string; email?: string };

function stubGrantWithClaims(claims: FakeClaims, options?: { rejectAs?: "client" | "other" }) {
  oidcTest.setAuthorizationCodeGrantImpl(async () => {
    if (options?.rejectAs === "client") {
      throw new (await import("openid-client")).ClientError("invalid_grant", { cause: claims });
    }
    if (options?.rejectAs === "other") {
      throw new Error("network down");
    }
    const tokens = {
      access_token: "fake-access",
      token_type: "Bearer",
      claims: () => ({
        sub: claims.sub,
        preferred_username: claims.preferred_username,
        email: claims.email,
      }),
    } as unknown as Awaited<ReturnType<typeof import("openid-client").authorizationCodeGrant>>;
    return tokens;
  });
}

async function callCallback(args: { state: string; cookie?: string; code?: string }) {
  const url = new URL("https://example.com/auth/callback");
  url.searchParams.set("code", args.code ?? "fake-code");
  url.searchParams.set("state", args.state);
  return await workerExports.default.fetch(url.toString(), {
    redirect: "manual",
    headers: args.cookie ? { Cookie: args.cookie } : undefined,
  });
}

beforeEach(() => {
  preloadProvider();
});

afterEach(() => {
  oidcTest.clearProviderCache();
  oidcTest.setAuthorizationCodeGrantImpl(null);
});

describe("/auth/callback", () => {
  it("creates user, namespace, membership, and session for a fresh sub with valid pref-name", async () => {
    const sub = "sub-fresh-1";
    const state = "state-fresh-1";
    stubGrantWithClaims({ sub, preferred_username: "fresh-rachel" });
    const cookie = await buildTransactionCookie(state, "nonce-1", "verifier-1");
    const res = await callCallback({ state, cookie });
    expect(res.status).toBe(302);
    expect(res.headers.get("location")).toBe("/auth/account");
    const cookies = res.headers.get("set-cookie") ?? "";
    expect(cookies).toContain(`${SESSION_COOKIE_HEADER_NAME}=goc_sess_`);
    const db = createDb(env.DB);
    const user = await findUserByTesseraSub(db, sub);
    expect(user).toBeDefined();
    const namespace = await findNamespaceBySlug(db, "fresh-rachel");
    expect(namespace).toBeDefined();
    expect(namespace!.createdBy).toBe(user!.id);
    const namespaces = await listNamespacesForUser(db, user!.id);
    expect(namespaces.map((row) => row.slug)).toEqual(["fresh-rachel"]);
  });

  it("suffixes the pref-name when the base slug is taken by another user", async () => {
    const occupant = "user-occupant";
    const db = createDb(env.DB);
    // Seed an existing namespace owned by an unrelated user.
    await db.batch([
      db.insert((await import("@/worker/db/d1/schema")).users).values({
        id: occupant,
        tesseraSub: "sub-occupant",
        createdAt: Date.now(),
      }),
      db.insert((await import("@/worker/db/d1/schema")).namespaces).values({
        id: "ns-taken",
        slug: "taken",
        createdBy: occupant,
        createdAt: Date.now(),
      }),
    ]);
    const sub = "sub-loser";
    const state = "state-taken";
    stubGrantWithClaims({ sub, preferred_username: "taken" });
    const cookie = await buildTransactionCookie(state, "nonce-2", "verifier-2");
    const res = await callCallback({ state, cookie });
    expect(res.status).toBe(302);
    expect(res.headers.get("location")).toBe("/auth/account");
    const dbAfter = createDb(env.DB);
    const user = await findUserByTesseraSub(dbAfter, sub);
    expect(user).toBeDefined();
    expect(user!.id).not.toBe(occupant);
    const namespace = await findNamespaceBySlug(dbAfter, "taken");
    expect(namespace?.createdBy).toBe(occupant);
    expect((await listNamespacesForUser(dbAfter, user!.id)).map((row) => row.slug)).toEqual([
      "taken-2",
    ]);
  });

  it("normalizes a non-slug pref-name instead of dropping the claim", async () => {
    const sub = "sub-invalid-1";
    const state = "state-invalid";
    stubGrantWithClaims({ sub, preferred_username: "Invalid_Slug!" });
    const cookie = await buildTransactionCookie(state, "nonce-3", "verifier-3");
    const res = await callCallback({ state, cookie });
    expect(res.status).toBe(302);
    expect(res.headers.get("location")).toBe("/auth/account");
    const db = createDb(env.DB);
    const user = await findUserByTesseraSub(db, sub);
    expect(user).toBeDefined();
    expect((await listNamespacesForUser(db, user!.id)).map((row) => row.slug)).toEqual([
      "invalid-slug",
    ]);
  });

  it("claims from the email local-part when the provider omits preferred_username", async () => {
    const sub = "sub-google-email";
    const state = "state-google-email";
    stubGrantWithClaims({ sub, email: "pei.li.philip@example.com" });
    const cookie = await buildTransactionCookie(state, "nonce-google", "verifier-google");
    const res = await callCallback({ state, cookie });
    expect(res.status).toBe(302);
    expect(res.headers.get("location")).toBe("/auth/account");
    const db = createDb(env.DB);
    const user = await findUserByTesseraSub(db, sub);
    expect(user).toBeDefined();
    expect((await listNamespacesForUser(db, user!.id)).map((row) => row.slug)).toEqual([
      "pei-li-philip",
    ]);
  });

  it("retries the claim for an existing user that still has no namespace", async () => {
    const sub = "sub-returning";
    const state1 = "state-r1";
    stubGrantWithClaims({ sub });
    const cookie1 = await buildTransactionCookie(state1, "n1", "v1");
    expect((await callCallback({ state: state1, cookie: cookie1 })).status).toBe(302);
    const db = createDb(env.DB);
    const user = await findUserByTesseraSub(db, sub);
    expect(user).toBeDefined();
    expect((await listNamespacesForUser(db, user!.id)).length).toBe(0);
    // A later sign-in with a usable naming claim must repair the account
    // without manual D1 edits.
    stubGrantWithClaims({ sub, preferred_username: "newslug" });
    const state2 = "state-r2";
    const cookie2 = await buildTransactionCookie(state2, "n2", "v2");
    const res = await callCallback({ state: state2, cookie: cookie2 });
    expect(res.status).toBe(302);
    expect((await listNamespacesForUser(db, user!.id)).map((row) => row.slug)).toEqual(["newslug"]);
  });

  it("does not claim a second namespace for a user that already has one", async () => {
    const sub = "sub-settled";
    const state1 = "state-settled-1";
    stubGrantWithClaims({ sub, preferred_username: "settled" });
    const cookie1 = await buildTransactionCookie(state1, "n", "v");
    expect((await callCallback({ state: state1, cookie: cookie1 })).status).toBe(302);
    const db = createDb(env.DB);
    const user = await findUserByTesseraSub(db, sub);
    expect(user).toBeDefined();
    stubGrantWithClaims({ sub, preferred_username: "settled-second" });
    const state2 = "state-settled-2";
    const cookie2 = await buildTransactionCookie(state2, "n", "v");
    expect((await callCallback({ state: state2, cookie: cookie2 })).status).toBe(302);
    expect((await listNamespacesForUser(db, user!.id)).map((row) => row.slug)).toEqual(["settled"]);
    expect(await findNamespaceBySlug(db, "settled-second")).toBeUndefined();
  });

  it("clears the OIDC transaction cookie when runtime config is missing", async () => {
    const cookie = await buildTransactionCookie("state-cfg", "n", "v");
    // Force loadOidcConfig() into the "missing_client_secret" branch by
    // blanking the binding for the duration of this test. Restoring at the
    // end so other tests still have a complete config.
    const original = env.TESSERA_OIDC_CLIENT_SECRET;
    env.TESSERA_OIDC_CLIENT_SECRET = "";
    try {
      const url = new URL("https://example.com/auth/callback");
      url.searchParams.set("code", "anything");
      url.searchParams.set("state", "state-cfg");
      const res = await workerExports.default.fetch(url.toString(), {
        redirect: "manual",
        headers: { Cookie: cookie },
      });
      expect(res.status).toBe(302);
      expect(res.headers.get("location")).toBe("/auth?error=oidc_unavailable");
      const setCookie = res.headers.get("set-cookie") ?? "";
      expect(setCookie).toContain(`${OIDC_TX_COOKIE_HEADER_NAME}=`);
      expect(setCookie.toLowerCase()).toContain("max-age=0");
    } finally {
      env.TESSERA_OIDC_CLIENT_SECRET = original;
    }
  });

  it("fails before D1 writes when SESSION_SECRET is missing", async () => {
    const sub = "sub-missing-session-secret";
    const state = "state-missing-session-secret";
    stubGrantWithClaims({ sub, preferred_username: "missing-session-secret" });
    const cookie = await buildTransactionCookie(state, "n", "v");
    const original = env.SESSION_SECRET;
    env.SESSION_SECRET = "";
    try {
      const res = await callCallback({ state, cookie });
      expect(res.status).toBe(302);
      expect(res.headers.get("location")).toBe("/auth?error=session_create_failed");
      const setCookie = res.headers.get("set-cookie") ?? "";
      expect(setCookie).toContain(`${OIDC_TX_COOKIE_HEADER_NAME}=`);
      expect(setCookie.toLowerCase()).toContain("max-age=0");
      const db = createDb(env.DB);
      expect(await findUserByTesseraSub(db, sub)).toBeUndefined();
    } finally {
      env.SESSION_SECRET = original;
    }
  });

  it("redirects to /auth?error=missing_state when the OIDC cookie is absent", async () => {
    const res = await callCallback({ state: "anything" });
    expect(res.status).toBe(302);
    expect(res.headers.get("location")).toBe("/auth?error=missing_state");
  });

  it("redirects to /auth?error=invalid_state when the OIDC cookie signature is invalid", async () => {
    const res = await callCallback({
      state: "anything",
      cookie: `${OIDC_TX_COOKIE_HEADER_NAME}=not-signed`,
    });
    expect(res.status).toBe(302);
    expect(res.headers.get("location")).toBe("/auth?error=invalid_state");
    const setCookie = res.headers.get("set-cookie") ?? "";
    expect(setCookie).toContain(`${OIDC_TX_COOKIE_HEADER_NAME}=`);
    expect(setCookie.toLowerCase()).toContain("max-age=0");
  });

  it("redirects to /auth?error=invalid_state when state does not match the cookie", async () => {
    const cookie = await buildTransactionCookie("state-A", "n", "v");
    const res = await callCallback({ state: "state-B", cookie });
    expect(res.status).toBe(302);
    expect(res.headers.get("location")).toBe("/auth?error=invalid_state");
  });

  it("redirects to /auth?error=invalid_id_token when the grant raises ClientError", async () => {
    const sub = "sub-grant-err";
    stubGrantWithClaims({ sub }, { rejectAs: "client" });
    const cookie = await buildTransactionCookie("state-grant", "n", "v");
    const res = await callCallback({ state: "state-grant", cookie });
    expect(res.status).toBe(302);
    expect(res.headers.get("location")).toBe("/auth?error=invalid_id_token");
  });

  it("seeds membership for the existing namespace owner on returning sign-in", async () => {
    const sub = "sub-existing-member";
    const userId = "user-existing-member";
    const namespaceId = "ns-existing-member";
    const db = createDb(env.DB);
    const schema = await import("@/worker/db/d1/schema");
    await db.batch([
      db.insert(schema.users).values({ id: userId, tesseraSub: sub, createdAt: Date.now() }),
      db.insert(schema.namespaces).values({
        id: namespaceId,
        slug: "preexisting",
        createdBy: userId,
        createdAt: Date.now(),
      }),
    ]);
    await insertMembershipIfMissing(db, {
      namespaceId,
      userId,
      createdAt: Date.now(),
    });
    stubGrantWithClaims({ sub, preferred_username: "preexisting" });
    const state = "state-pre";
    const cookie = await buildTransactionCookie(state, "n", "v");
    const res = await callCallback({ state, cookie });
    expect(res.status).toBe(302);
    expect((await listNamespacesForUser(db, userId)).map((row) => row.slug)).toEqual([
      "preexisting",
    ]);
  });

  it("creates a fresh namespace claim without creating route-cache entries", async () => {
    const slug = "callback-direct";
    const repo = "not-created-by-callback";
    const routeKey = `repo-route:v1:${slug}/${repo}`;
    await env.ROUTES.delete(routeKey);
    stubGrantWithClaims({ sub: "sub-callback-direct", preferred_username: slug });
    const cookie = await buildTransactionCookie("state-callback-direct", "n", "v");
    const res = await callCallback({ state: "state-callback-direct", cookie });
    expect(res.status).toBe(302);
    await new Promise((resolve) => setTimeout(resolve, 250));
    expect(await env.ROUTES.get(routeKey)).toBeNull();
  });
});
