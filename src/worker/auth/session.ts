import type { Viewer } from "@/client/server/viewer";
import { newPrefixedId } from "@/worker/common";
import { findUserById, listNamespacesForUser } from "@/worker/db/d1/dal";
import type { UserRow } from "@/worker/db/d1/schema";
import type { AppContext } from "@/worker/routes/hono";
import { z } from "zod";

import { clearSessionCookie, getSessionCookie, setSessionCookie } from "./cookies";

// Session cookie value shape: `goc_sess_<base64url AES-GCM blob>`.
// The sealed blob contains the user id and expiry and is authenticated with
// SESSION_SECRET. Validation decrypts the blob, checks expiry, and loads the
// current user row. Rotating SESSION_SECRET makes existing cookies fail.
const SESSION_TOKEN_PREFIX = "goc_sess_";
const SESSION_TTL_MS = 30 * 24 * 60 * 60 * 1000; // 30 days
const SESSION_PURPOSE = "goc-browser-session-v1";
const SESSION_VERSION = 1;
const AES_GCM_IV_LENGTH = 12;
const AES_KEY_BIT_LENGTH = 256;

const SessionPayloadSchema = z.object({
  version: z.literal(SESSION_VERSION),
  userId: z.string(),
  createdAt: z.number(),
  expiresAt: z.number(),
  // Prefill hint for the account page's self-service namespace claim form.
  // Optional so sessions minted before this field existed keep validating.
  suggestedNamespaceSlug: z.string().max(40).optional(),
});

type SessionPayload = z.infer<typeof SessionPayloadSchema>;

export type ActiveSession = { user: UserRow; payload: SessionPayload };

export type SessionConfigResult =
  | { ok: true; secret: string }
  | { ok: false; reason: "missing_session_secret" };

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

function base64UrlEncode(bytes: Uint8Array | ArrayBuffer): string {
  const view = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  let binary = "";
  for (let i = 0; i < view.length; i += 1) {
    binary += String.fromCharCode(view[i]!);
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function base64UrlDecode(input: string): Uint8Array<ArrayBuffer> {
  const padded = input
    .replace(/-/g, "+")
    .replace(/_/g, "/")
    .padEnd(input.length + ((4 - (input.length % 4)) % 4), "=");
  const binary = atob(padded);
  const bytes = new Uint8Array(new ArrayBuffer(binary.length));
  for (let i = 0; i < binary.length; i += 1) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
}

function randomBytes(length: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(new ArrayBuffer(length));
  crypto.getRandomValues(bytes);
  return bytes;
}

async function deriveSessionKey(secret: string): Promise<CryptoKey> {
  const baseKey = await crypto.subtle.importKey(
    "raw",
    textEncoder.encode(secret),
    { name: "HKDF" },
    false,
    ["deriveKey"]
  );
  return crypto.subtle.deriveKey(
    {
      name: "HKDF",
      hash: "SHA-256",
      salt: new Uint8Array(0),
      info: textEncoder.encode(SESSION_PURPOSE),
    },
    baseKey,
    { name: "AES-GCM", length: AES_KEY_BIT_LENGTH },
    false,
    ["encrypt", "decrypt"]
  );
}

async function sealSession(secret: string, payload: SessionPayload): Promise<string> {
  const key = await deriveSessionKey(secret);
  const iv = randomBytes(AES_GCM_IV_LENGTH);
  const plaintext = textEncoder.encode(JSON.stringify(payload));
  const ciphertext = await crypto.subtle.encrypt({ name: "AES-GCM", iv }, key, plaintext);
  const sealed = new Uint8Array(iv.length + ciphertext.byteLength);
  sealed.set(iv, 0);
  sealed.set(new Uint8Array(ciphertext), iv.length);
  return `${SESSION_TOKEN_PREFIX}${base64UrlEncode(sealed)}`;
}

type UnsealSessionResult =
  | { ok: true; payload: SessionPayload }
  | { ok: false; reason: "malformed" | "decrypt_failed" | "invalid_payload" | "expired" };

async function unsealSession(
  secret: string,
  token: string,
  now: number = Date.now()
): Promise<UnsealSessionResult> {
  if (!token.startsWith(SESSION_TOKEN_PREFIX)) return { ok: false, reason: "malformed" };
  let raw: Uint8Array<ArrayBuffer>;
  try {
    raw = base64UrlDecode(token.slice(SESSION_TOKEN_PREFIX.length));
  } catch {
    return { ok: false, reason: "malformed" };
  }
  if (raw.length <= AES_GCM_IV_LENGTH) return { ok: false, reason: "malformed" };
  const iv = raw.subarray(0, AES_GCM_IV_LENGTH);
  const ciphertext = raw.subarray(AES_GCM_IV_LENGTH);
  let plaintextBuffer: ArrayBuffer;
  try {
    const key = await deriveSessionKey(secret);
    plaintextBuffer = await crypto.subtle.decrypt({ name: "AES-GCM", iv }, key, ciphertext);
  } catch {
    return { ok: false, reason: "decrypt_failed" };
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(textDecoder.decode(plaintextBuffer));
  } catch {
    return { ok: false, reason: "invalid_payload" };
  }
  const payload = SessionPayloadSchema.safeParse(parsed);
  if (!payload.success) return { ok: false, reason: "invalid_payload" };
  if (payload.data.expiresAt <= now) return { ok: false, reason: "expired" };
  return { ok: true, payload: payload.data };
}

export function loadSessionConfig(env: Env): SessionConfigResult {
  const secret = env.SESSION_SECRET?.trim();
  if (!secret) return { ok: false, reason: "missing_session_secret" };
  return { ok: true, secret };
}

export async function createSessionForUser(
  env: Env,
  c: AppContext,
  userId: string,
  now: number = Date.now(),
  suggestedNamespaceSlug?: string
): Promise<{ token: string }> {
  const config = loadSessionConfig(env);
  if (!config.ok) {
    throw new Error(config.reason);
  }
  const token = await sealSession(config.secret, {
    version: SESSION_VERSION,
    userId,
    createdAt: now,
    expiresAt: now + SESSION_TTL_MS,
    suggestedNamespaceSlug,
  });
  setSessionCookie(c, token);
  return { token };
}

async function readActiveSessionUncached(c: AppContext): Promise<ActiveSession | null> {
  const token = getSessionCookie(c);
  if (!token) return null;
  const config = loadSessionConfig(c.env);
  if (!config.ok) return null;
  try {
    const unsealed = await unsealSession(config.secret, token, Date.now());
    if (!unsealed.ok) return null;
    const user = await findUserById(c.var.db, unsealed.payload.userId);
    if (!user) return null;
    return { user, payload: unsealed.payload };
  } catch {
    return null;
  }
}

// Resolve the active session from the cookie. The sealed payload supplies
// only the user id and expiry; D1 is consulted for the current user row so
// deleted or missing users fail closed. The in-flight promise is stored on
// the request context so multiple access checks share one decrypt + D1 read.
export async function readActiveSession(c: AppContext): Promise<ActiveSession | null> {
  const cached = c.var.activeSessionPromise;
  if (cached) return await cached;
  const promise = readActiveSessionUncached(c);
  c.set("activeSessionPromise", promise);
  return await promise;
}

// Lift an active session into a Viewer, resolving the user's primary
// namespace (if any). Used by route handlers that need to render the
// signed-in shell or gate access to /auth/account.
export async function loadViewer(c: AppContext): Promise<Viewer | null> {
  const cached = c.var.viewerPromise;
  if (cached) return await cached;
  const promise = loadViewerUncached(c);
  c.set("viewerPromise", promise);
  return await promise;
}

async function loadViewerUncached(c: AppContext): Promise<Viewer | null> {
  const active = await readActiveSession(c);
  if (!active) return null;
  let primaryNamespaceSlug: string | undefined;
  try {
    const namespaces = await listNamespacesForUser(c.var.db, active.user.id);
    primaryNamespaceSlug = namespaces[0]?.slug;
  } catch {
    primaryNamespaceSlug = undefined;
  }
  return {
    userId: active.user.id,
    primaryNamespaceSlug,
    suggestedNamespaceSlug: primaryNamespaceSlug
      ? undefined
      : active.payload.suggestedNamespaceSlug,
  };
}

// Sign-out clears the browser cookie. Because sessions are sealed stateless
// cookies, a copied cookie cannot be server-revoked without adding a
// revocation store; rotating SESSION_SECRET is the coarse invalidation tool.
export async function endSession(c: AppContext): Promise<void> {
  clearSessionCookie(c);
  const signedOut = Promise.resolve(null);
  c.set("activeSessionPromise", signedOut);
  c.set("viewerPromise", signedOut);
}

export function generateUserId(): string {
  return newPrefixedId("user");
}

export function generateNamespaceId(): string {
  return newPrefixedId("ns");
}

// Exposed for tests that need to validate sealed-session behavior without
// reaching into module internals.
export const __test = { sealSession, unsealSession };
