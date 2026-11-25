// Shared slug policy used by namespace and repository slugs. Browser-safe so
// the same validator runs in SSR pages and Worker validation paths.
//
// The regex enforces lowercase ASCII alphanumerics with internal dashes.
// Length is bounded at 1..40. Reserved slugs cover Worker-owned routes and
// common static paths so that creating a namespace cannot shadow existing
// surfaces.

const SLUG_PATTERN = /^[a-z0-9](?:[a-z0-9-]{0,38}[a-z0-9])?$/;
export const SLUG_MAX_LENGTH = 40;

export const RESERVED_SLUGS: readonly string[] = [
  "auth",
  "sign-in",
  "sign-out",
  "api",
  "assets",
  "_cache",
  "_routes",
  "favicon.ico",
  "robots.txt",
];

const reservedSet: ReadonlySet<string> = new Set(RESERVED_SLUGS);

export function isValidSlug(input: string): boolean {
  if (typeof input !== "string") return false;
  if (input.length === 0 || input.length > SLUG_MAX_LENGTH) return false;
  if (!SLUG_PATTERN.test(input)) return false;
  if (reservedSet.has(input)) return false;
  return true;
}

export type SlugValidation =
  | { ok: true; slug: string }
  | { ok: false; reason: "format" | "reserved" | "length" };

// Best-effort conversion of a human-readable label (OIDC username, email
// local-part, etc.) into a slug-shaped string. This deliberately keeps
// reserved words intact so callers can decide whether to skip or suffix
// them; use `namespaceSlugCandidates` when you need claimable values.
export function normalizeSlugLabel(input: string): string | null {
  if (typeof input !== "string") return null;
  const normalized = input
    .normalize("NFKD")
    // Drop combining marks so "émigré" becomes "emigre" rather than
    // "migr" after the ASCII-only replacement below.
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  if (!normalized) return null;
  const bounded = normalized.slice(0, SLUG_MAX_LENGTH).replace(/-+$/g, "");
  return bounded.length > 0 ? bounded : null;
}

// Ordered candidates for an automatic namespace claim. The first entry is
// the unmodified normalized label; later entries add a numeric suffix so a
// collision ("alice" already taken) never leaves a fresh account without a
// namespace. Candidates that fail the shared slug policy (reserved words,
// empty results) are skipped.
export function namespaceSlugCandidates(input: string, max = 5): string[] {
  const base = normalizeSlugLabel(input);
  if (!base) return [];
  const candidates: string[] = [];
  const push = (slug: string) => {
    if (candidates.length < max && isValidSlug(slug) && !candidates.includes(slug)) {
      candidates.push(slug);
    }
  };
  push(base);
  for (let i = 2; candidates.length < max && i <= max + 20; i += 1) {
    const suffix = `-${i}`;
    const stem = base.slice(0, SLUG_MAX_LENGTH - suffix.length).replace(/-+$/g, "");
    if (!stem) break;
    push(`${stem}${suffix}`);
  }
  return candidates;
}

// Granular result so the route handler can decide which message to surface
// (taken vs format vs reserved) without re-checking the regex.
export function validateSlugForRoute(input: string): SlugValidation {
  if (typeof input !== "string" || input.length === 0) {
    return { ok: false, reason: "length" };
  }
  if (input.length > SLUG_MAX_LENGTH) return { ok: false, reason: "length" };
  if (reservedSet.has(input)) return { ok: false, reason: "reserved" };
  if (!SLUG_PATTERN.test(input)) return { ok: false, reason: "format" };
  return { ok: true, slug: input };
}
