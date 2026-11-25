// Browser-safe viewer shape used by the SSR shell. The Worker computes this
// from the session cookie + D1 in `src/worker/auth/session.ts` and threads
// it into `renderUiDocumentResponse`. SSR components must not reach for
// session state directly — keep this presentational.
export type Viewer = {
  userId: string;
  // Slug of the user's "home" namespace. Optional because a user can exist
  // without a namespace when every automatic claim candidate was taken or
  // the identity provider supplied no username/email claim.
  primaryNamespaceSlug?: string;
  // First automatic claim candidate (from `preferred_username` or the email
  // local-part). Used only to prefill the self-service claim form.
  suggestedNamespaceSlug?: string;
};
