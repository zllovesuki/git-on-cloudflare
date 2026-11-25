import { namespaceSlugCandidates } from "@/shared/slugs";

import type { VerifiedIdToken } from "./oidc";

// Derive namespace claim candidates from verified ID-token claims.
//
// `preferred_username` is the preferred source when the provider emits it
// (tessera, Keycloak, Auth0 with a mapped claim, ...). Google Workspace and
// plain Google OIDC do not emit it, so fall back to the email local-part.
// Both values are only naming hints; ownership is established by the
// namespace row's `created_by`, not by the claim itself.
export function namespaceClaimCandidates(claims: VerifiedIdToken, max = 5): string[] {
  const preferred = claims.preferredUsername?.trim();
  if (preferred) {
    const candidates = namespaceSlugCandidates(preferred, max);
    if (candidates.length > 0) return candidates;
  }
  const emailLocalPart = claims.email?.split("@")[0]?.trim();
  if (emailLocalPart) {
    return namespaceSlugCandidates(emailLocalPart, max);
  }
  return [];
}
