import * as oidc from "openid-client";
import { z } from "zod";

// tessera relying-party module. Mirrors flamemail's OIDC pattern:
//   - openid-client v6 owns discovery, PKCE, authorize URL, token exchange
//   - Loopback HTTP issuer is allowed only for local dev (tessera-dev runs on
//     http://localhost:5174). Production deployments use https://auth.limic.dev.
//   - The OIDC transaction (state, nonce, PKCE verifier, redirect) is encoded
//     into a Hono signed `__Host-goc_oidc` cookie using
//     a purpose-derived key from TESSERA_OIDC_CLIENT_SECRET, so we do not
//     need a separate cookie-signing secret. The payload is
//     integrity-protected but not encrypted; it is short-lived, HttpOnly,
//     Secure, and only used to complete the same browser's authorization-code
//     callback.
//   - A small `__test` namespace lets vitest swap the discovery cache and the
//     token-exchange call without spinning a real OIDC provider.

const STATE_TTL_MS = 5 * 60 * 1000;
const DISCOVERY_CACHE_TTL_MS = 5 * 60 * 1000;
const TRANSACTION_COOKIE_PURPOSE = "goc-oidc-transaction-cookie-v1";
const TRANSACTION_COOKIE_KEY_BITS = 256;

const TransactionPayloadSchema = z.object({
  state: z.string(),
  nonce: z.string(),
  codeVerifier: z.string(),
  redirectUri: z.string(),
  createdAt: z.number(),
});

export type TransactionPayload = z.infer<typeof TransactionPayloadSchema>;

export interface OidcConfig {
  issuer: string;
  clientId: string;
  clientSecret: string;
}

export type OidcConfigError =
  | "missing_issuer"
  | "insecure_issuer"
  | "missing_client_id"
  | "missing_client_secret";

export type OidcConfigResult =
  | { ok: true; config: OidcConfig }
  | { ok: false; reason: OidcConfigError };

// `URL#hostname` for `http://[::1]:5174` returns `[::1]` (with brackets), so
// we accept both forms. Lowercased for case-insensitive comparison.
const LOOPBACK_HOSTNAMES: ReadonlySet<string> = new Set(["localhost", "127.0.0.1", "::1", "[::1]"]);

function isLoopbackHostname(hostname: string): boolean {
  return LOOPBACK_HOSTNAMES.has(hostname.toLowerCase());
}

function isAllowedUrl(url: URL): boolean {
  if (url.protocol === "https:") return true;
  return url.protocol === "http:" && isLoopbackHostname(url.hostname);
}

function normalizeIssuer(rawIssuer: string): string | null {
  let url: URL;
  try {
    url = new URL(rawIssuer);
  } catch {
    return null;
  }
  if (url.search || url.hash || url.username || url.password) return null;
  const pathname = url.pathname.replace(/\/+$/, "");
  return `${url.protocol}//${url.host}${pathname}`;
}

export function loadOidcConfig(env: Env): OidcConfigResult {
  const rawIssuer = env.TESSERA_OIDC_ISSUER?.trim();
  if (!rawIssuer) return { ok: false, reason: "missing_issuer" };
  const issuer = normalizeIssuer(rawIssuer);
  if (!issuer) return { ok: false, reason: "insecure_issuer" };
  if (!isAllowedUrl(new URL(issuer))) return { ok: false, reason: "insecure_issuer" };
  const clientId = env.TESSERA_OIDC_CLIENT_ID?.trim();
  if (!clientId) return { ok: false, reason: "missing_client_id" };
  const clientSecret = env.TESSERA_OIDC_CLIENT_SECRET?.trim();
  if (!clientSecret) return { ok: false, reason: "missing_client_secret" };
  return { ok: true, config: { issuer, clientId, clientSecret } };
}

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

export interface PkcePair {
  verifier: string;
  challenge: string;
}

export async function generatePkcePair(): Promise<PkcePair> {
  const verifier = oidc.randomPKCECodeVerifier();
  const challenge = await oidc.calculatePKCECodeChallenge(verifier);
  return { verifier, challenge };
}

export function encodeTransactionPayload(payload: TransactionPayload): string {
  return base64UrlEncode(textEncoder.encode(JSON.stringify(payload)));
}

export async function deriveTransactionCookieSecret(
  clientSecret: string
): Promise<Uint8Array<ArrayBuffer>> {
  const baseKey = await crypto.subtle.importKey(
    "raw",
    textEncoder.encode(clientSecret),
    { name: "HKDF" },
    false,
    ["deriveBits"]
  );
  const derived = await crypto.subtle.deriveBits(
    {
      name: "HKDF",
      hash: "SHA-256",
      salt: new Uint8Array(0),
      info: textEncoder.encode(TRANSACTION_COOKIE_PURPOSE),
    },
    baseKey,
    TRANSACTION_COOKIE_KEY_BITS
  );
  return new Uint8Array(derived);
}

export type DecodeTransactionPayloadError = "malformed" | "invalid_payload" | "expired";

export type DecodeTransactionPayloadResult =
  | { ok: true; payload: TransactionPayload }
  | { ok: false; reason: DecodeTransactionPayloadError };

export function decodeTransactionPayload(
  encoded: string,
  now: number = Date.now()
): DecodeTransactionPayloadResult {
  let raw: Uint8Array<ArrayBuffer>;
  try {
    raw = base64UrlDecode(encoded);
  } catch {
    return { ok: false, reason: "malformed" };
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(textDecoder.decode(raw));
  } catch {
    return { ok: false, reason: "invalid_payload" };
  }
  const payload = TransactionPayloadSchema.safeParse(parsed);
  if (!payload.success) return { ok: false, reason: "invalid_payload" };
  if (now - payload.data.createdAt > STATE_TTL_MS) return { ok: false, reason: "expired" };
  return { ok: true, payload: payload.data };
}

export interface OidcProvider {
  configuration: oidc.Configuration;
}

export type OidcDiscoveryError = "discovery_failed" | "invalid_endpoint";

export type OidcDiscoveryResult =
  | { ok: true; provider: OidcProvider }
  | { ok: false; reason: OidcDiscoveryError };

interface CachedOidcProvider {
  provider: OidcProvider;
  clientSecret: string;
  expiresAt: number;
}

const oidcProviderByIssuerAndClient = new Map<string, CachedOidcProvider>();

function isAllowedDiscoveredEndpoint(rawUrl: string | undefined): boolean {
  if (!rawUrl) return false;
  let url: URL;
  try {
    url = new URL(rawUrl);
  } catch {
    return false;
  }
  if (url.hash) return false;
  return isAllowedUrl(url);
}

function cacheKey(config: OidcConfig): string {
  return `${config.issuer}|${config.clientId}`;
}

function discoveryOptions(config: OidcConfig): oidc.DiscoveryRequestOptions | undefined {
  const issuerUrl = new URL(config.issuer);
  if (issuerUrl.protocol !== "http:") return undefined;
  // openid-client requires this opt-in for plaintext issuers, used only for
  // loopback dev environments. The issuer policy above already gated us to
  // `isAllowedUrl`, so this is not an additional security boundary.
  return { execute: [oidc.allowInsecureRequests] };
}

function hasRequiredSecureEndpoints(configuration: oidc.Configuration): boolean {
  const metadata = configuration.serverMetadata();
  return (
    isAllowedDiscoveredEndpoint(metadata.authorization_endpoint) &&
    isAllowedDiscoveredEndpoint(metadata.token_endpoint) &&
    isAllowedDiscoveredEndpoint(metadata.jwks_uri)
  );
}

export async function discoverOidcProvider(
  config: OidcConfig,
  now: number = Date.now()
): Promise<OidcDiscoveryResult> {
  const key = cacheKey(config);
  const cached = oidcProviderByIssuerAndClient.get(key);
  if (cached && cached.clientSecret === config.clientSecret && cached.expiresAt > now) {
    return { ok: true, provider: cached.provider };
  }
  if (cached) oidcProviderByIssuerAndClient.delete(key);

  let configuration: oidc.Configuration;
  try {
    configuration = await oidc.discovery(
      new URL(config.issuer),
      config.clientId,
      undefined,
      oidc.ClientSecretPost(config.clientSecret),
      discoveryOptions(config)
    );
  } catch {
    return { ok: false, reason: "discovery_failed" };
  }
  if (!hasRequiredSecureEndpoints(configuration)) {
    return { ok: false, reason: "invalid_endpoint" };
  }
  const provider: OidcProvider = { configuration };
  oidcProviderByIssuerAndClient.set(key, {
    provider,
    clientSecret: config.clientSecret,
    expiresAt: now + DISCOVERY_CACHE_TTL_MS,
  });
  return { ok: true, provider };
}

export interface TokenExchangeResponse {
  claims: oidc.IDToken;
}

export type ExchangeError = "token_exchange_failed" | "invalid_id_token";

export type ExchangeResult =
  | { ok: true; tokens: TokenExchangeResponse }
  | { ok: false; reason: ExchangeError };

// Indirected so vitest can swap the network call without subclassing
// `oidc.Configuration` (which is a closed shape in openid-client v6).
type GrantImpl = typeof oidc.authorizationCodeGrant;
let authorizationCodeGrantImpl: GrantImpl = oidc.authorizationCodeGrant;

export async function exchangeAuthorizationCode(
  provider: OidcProvider,
  options: { callbackUrl: string; codeVerifier: string; state: string; nonce: string }
): Promise<ExchangeResult> {
  let tokens: oidc.TokenEndpointResponse & oidc.TokenEndpointResponseHelpers;
  try {
    tokens = await authorizationCodeGrantImpl(
      provider.configuration,
      new URL(options.callbackUrl),
      {
        expectedNonce: options.nonce,
        expectedState: options.state,
        pkceCodeVerifier: options.codeVerifier,
      }
    );
  } catch (error) {
    if (error instanceof oidc.ClientError) {
      return { ok: false, reason: "invalid_id_token" };
    }
    return { ok: false, reason: "token_exchange_failed" };
  }
  const claims = tokens.claims();
  if (!claims) return { ok: false, reason: "invalid_id_token" };
  return { ok: true, tokens: { claims } };
}

export interface VerifiedIdToken {
  sub: string;
  preferredUsername?: string;
  // Providers such as Google never emit `preferred_username`. Their ID
  // tokens do carry a verified `email`, which is used only as a namespace
  // claim candidate (never as identity proof) when no username is present.
  email?: string;
}

export type VerifyError = "missing_sub";

export type VerifyResult =
  | { ok: true; verified: VerifiedIdToken }
  | { ok: false; reason: VerifyError };

// goc trusts any verified `sub` (no operator allowlist; this is a multi-user
// surface, not an admin tool). `preferred_username` is captured here as a
// candidate slug; ownership is established through the namespace claim, not
// through the claim value.
export function verifyIdTokenClaims(claims: oidc.IDToken): VerifyResult {
  if (typeof claims.sub !== "string" || claims.sub.length === 0) {
    return { ok: false, reason: "missing_sub" };
  }
  const preferredUsername =
    typeof claims.preferred_username === "string" ? claims.preferred_username : undefined;
  const email = typeof claims.email === "string" ? claims.email : undefined;
  return { ok: true, verified: { sub: claims.sub, preferredUsername, email } };
}

export function buildAuthorizeUrl(
  provider: OidcProvider,
  options: { redirectUri: string; state: string; nonce: string; codeChallenge: string }
): string {
  return oidc
    .buildAuthorizationUrl(provider.configuration, {
      code_challenge: options.codeChallenge,
      code_challenge_method: "S256",
      nonce: options.nonce,
      redirect_uri: options.redirectUri,
      scope: "openid profile email",
      state: options.state,
    })
    .toString();
}

export function buildCallbackUrl(requestUrl: string): string {
  return `${new URL(requestUrl).origin}/auth/callback`;
}

export function generateState(): string {
  return oidc.randomState();
}

export function generateNonce(): string {
  return oidc.randomNonce();
}

// Test-only seam. Production code never imports `__test`. Tests use it to
// preload a fake `Configuration` into the discovery cache and to swap the
// `authorizationCodeGrant` call so they can exercise the callback handler
// without standing up a live OIDC provider.
export const __test = {
  setAuthorizationCodeGrantImpl(impl: GrantImpl | null): void {
    authorizationCodeGrantImpl = impl ?? oidc.authorizationCodeGrant;
  },
  setProviderForTesting(config: OidcConfig, provider: OidcProvider): void {
    oidcProviderByIssuerAndClient.set(cacheKey(config), {
      provider,
      clientSecret: config.clientSecret,
      expiresAt: Number.MAX_SAFE_INTEGER,
    });
  },
  clearProviderCache(): void {
    oidcProviderByIssuerAndClient.clear();
  },
};
