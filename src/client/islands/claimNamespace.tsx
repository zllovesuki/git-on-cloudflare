/// <reference lib="dom" />

import { useState } from "react";
import { ShieldCheck } from "lucide-react";

import { hydrateIsland } from "@/client/hydrate";
import { Button, EmptyState, ErrorBanner, Input } from "@/client/components/ui";

export type ClaimNamespaceIslandProps = {
  suggestedNamespaceSlug?: string;
};

type ClaimResult =
  | { ok: true; namespace: { id: string; slug: string } }
  | { ok: false; reason: "invalid-slug" | "slug-taken" | "already-claimed" };

function describeClaimReason(reason: Exclude<ClaimResult, { ok: true }>["reason"]): string {
  switch (reason) {
    case "invalid-slug":
      return "Use lowercase letters, digits, and dashes (1-40 chars, not a reserved word).";
    case "slug-taken":
      return "That namespace is already claimed. Pick another one.";
    case "already-claimed":
      return "This account already has a namespace. Reload the page to see it.";
  }
}

// Shown only when the sign-in callback could not claim a namespace
// automatically. The first automatic candidate is prefilled, so most users
// can simply press the button.
export function ClaimNamespaceIsland({ suggestedNamespaceSlug }: ClaimNamespaceIslandProps) {
  const [slug, setSlug] = useState(suggestedNamespaceSlug ?? "");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const canSubmit = !submitting && slug.trim().length > 0;

  return (
    <div className="grid gap-4">
      <EmptyState
        icon={<ShieldCheck className="h-5 w-5 text-zinc-400" aria-hidden="true" />}
        title="Choose your namespace"
        detail="It becomes your public @handle and owns your repositories."
      />
      {error ? <ErrorBanner>{error}</ErrorBanner> : null}
      <form
        className="mx-auto grid w-full max-w-sm gap-4"
        onSubmit={async (event) => {
          event.preventDefault();
          if (!canSubmit) return;
          setSubmitting(true);
          setError(null);
          try {
            const res = await fetch("/auth/api/namespaces", {
              method: "POST",
              credentials: "same-origin",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ slug: slug.trim().toLowerCase() }),
            });
            const body = (await res.json().catch(() => null)) as ClaimResult | null;
            if (!body) {
              setError("Could not claim the namespace. Try again.");
              return;
            }
            if (!body.ok) {
              setError(describeClaimReason(body.reason));
              return;
            }
            window.location.assign("/auth/account");
          } catch (err) {
            setError(String(err));
          } finally {
            setSubmitting(false);
          }
        }}
      >
        <Input
          label="Namespace"
          type="text"
          value={slug}
          onChange={(event) => setSlug(event.target.value)}
          placeholder="your-name"
          helperText="Lowercase ASCII, dashes between letters/digits. 1-40 chars."
          required
        />
        <Button type="submit" variant="primary" size="md" disabled={!canSubmit}>
          Claim namespace
        </Button>
      </form>
    </div>
  );
}

export function initClaimNamespaceIsland() {
  hydrateIsland("claim-namespace", ClaimNamespaceIsland);
}
