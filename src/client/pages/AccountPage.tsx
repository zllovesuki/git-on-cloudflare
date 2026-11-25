import { IslandHost } from "@/client/server/IslandHost";
import { ClaimNamespaceIsland } from "@/client/islands/claimNamespace";
import { RepositoriesIsland } from "@/client/islands/repositories";
import { TokensIsland, type TokensIslandSummary } from "@/client/islands/tokens";

export type AccountNamespace = {
  id: string;
  slug: string;
};

export type AccountRepository = {
  id: string;
  slug: string;
  namespaceSlug: string;
  visibility: "public" | "private";
  updatedAt: number;
};

export type AccountPageProps = {
  userId: string;
  primaryNamespaceSlug?: string;
  suggestedNamespaceSlug?: string;
  namespaces: AccountNamespace[];
  repositories: AccountRepository[];
  tokens: TokensIslandSummary[];
};

function truncateUserId(userId: string): string {
  if (userId.length <= 12) return userId;
  return `${userId.slice(0, 8)}…${userId.slice(-4)}`;
}

export function AccountPage({
  userId,
  primaryNamespaceSlug,
  suggestedNamespaceSlug,
  namespaces,
  repositories,
  tokens,
}: AccountPageProps) {
  const handle = primaryNamespaceSlug ? `@${primaryNamespaceSlug}` : "Identity not yet claimed";
  return (
    <div className="mx-auto max-w-3xl py-8">
      <header className="mb-12">
        <p className="m-0 mb-2 text-xs font-medium uppercase tracking-widest text-accent-500 dark:text-accent-400">
          Identity
        </p>
        <div className="flex flex-col items-baseline justify-between gap-2 sm:flex-row sm:gap-6">
          <h1 className="m-0 font-display text-4xl font-semibold tracking-tight text-zinc-900 dark:text-zinc-100">
            {handle}
          </h1>
          <span
            className="font-mono text-[11px] uppercase tracking-widest text-zinc-500"
            title={userId}
          >
            id {truncateUserId(userId)}
          </span>
        </div>
      </header>

      <section className="mb-10">
        <h2 className="m-0 mb-3 text-xs font-semibold uppercase tracking-wider text-zinc-500">
          Namespaces
        </h2>
        {namespaces.length === 0 ? (
          <IslandHost name="claim-namespace" props={{ suggestedNamespaceSlug }}>
            <ClaimNamespaceIsland suggestedNamespaceSlug={suggestedNamespaceSlug} />
          </IslandHost>
        ) : (
          <div className="flex flex-wrap gap-2">
            {namespaces.map((ns) => (
              <a
                key={ns.id}
                href={`/${ns.slug}`}
                className="inline-flex items-center rounded-lg border border-zinc-300 bg-white/60 px-3 py-1.5 font-mono text-sm text-zinc-700 no-underline transition-colors hover:border-accent-500/40 hover:text-accent-600 dark:border-zinc-800/60 dark:bg-zinc-900/30 dark:text-zinc-300 dark:hover:text-accent-400"
              >
                @{ns.slug}
              </a>
            ))}
          </div>
        )}
      </section>

      <section className="mb-10">
        <h2 className="m-0 mb-3 text-xs font-semibold uppercase tracking-wider text-zinc-500">
          Repositories
        </h2>
        <IslandHost name="repositories" props={{ primaryNamespaceSlug, repositories }}>
          <RepositoriesIsland
            primaryNamespaceSlug={primaryNamespaceSlug}
            repositories={repositories}
          />
        </IslandHost>
      </section>

      <section id="tokens" className="scroll-mt-20">
        <h2 className="m-0 mb-1.5 text-xs font-semibold uppercase tracking-wider text-zinc-500">
          Tokens
        </h2>
        <p className="m-0 mb-4 text-sm text-zinc-500">
          Personal access tokens scope to a namespace or a single repo. The plaintext is shown only
          once at creation.
        </p>
        <IslandHost name="tokens" props={{ primaryNamespaceSlug, repositories, tokens }}>
          <TokensIsland
            primaryNamespaceSlug={primaryNamespaceSlug}
            repositories={repositories}
            tokens={tokens}
          />
        </IslandHost>
      </section>
    </div>
  );
}
