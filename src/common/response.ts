/**
 * Lightweight response helpers to keep handlers concise and consistent.
 * Prefer these over ad-hoc new Response(...) in endpoint code.
 */

export function json(data: unknown, status = 200, headers: HeadersInit = {}) {
  const h = new Headers(headers);
  if (!h.has("Content-Type")) h.set("Content-Type", "application/json");
  return new Response(JSON.stringify(data), { status, headers: h });
}

export function text(body: string, status = 200, headers: HeadersInit = {}) {
  const h = new Headers(headers);
  if (!h.has("Content-Type")) h.set("Content-Type", "text/plain; charset=utf-8");
  return new Response(body, { status, headers: h });
}

export function clientAbortedResponse(headers: HeadersInit = {}): Response {
  return text("client aborted\n", 499, headers);
}

/**
 * Extract a Bearer token from the Authorization header.
 * Returns an empty string when missing or malformed.
 */
export function getBearerToken(req: Request): string {
  const h = req.headers.get("Authorization") || "";
  return h.replace(/^Bearer\s+/i, "");
}

/**
 * 401 Unauthorized response with WWW-Authenticate: Bearer
 */
export function unauthorizedBearer(headers: HeadersInit = {}): Response {
  const h = new Headers(headers);
  h.set("WWW-Authenticate", "Bearer");
  return text("Unauthorized\n", 401, h);
}

/**
 * 401 Unauthorized response with WWW-Authenticate: Basic
 */
export function unauthorizedBasic(realm = "Git"): Response {
  return text("Unauthorized\n", 401, {
    "WWW-Authenticate": `Basic realm="${realm}", charset="UTF-8"`,
  });
}

/**
 * 401 Unauthorized response for Admin UI Routes
 */
export function unauthorizedAdminBasic(): Response {
  return unauthorizedBasic("Git Admin");
}

/**
 * 429 Too Many Attempts response with optional Retry-After seconds header.
 */
export function tooManyAttempts(retryAfterSeconds?: number, headers: HeadersInit = {}): Response {
  const h = new Headers(headers);
  if (typeof retryAfterSeconds === "number" && Number.isFinite(retryAfterSeconds)) {
    h.set("Retry-After", String(Math.ceil(retryAfterSeconds)));
  }
  return text("Too many attempts\n", 429, h);
}
