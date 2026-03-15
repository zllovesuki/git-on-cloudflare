import { EmptyState } from "@/ui/components/EmptyState";

export type OwnerPageProps = {
  owner: string;
  repos: string[];
};

export function OwnerPage({ owner, repos }: OwnerPageProps) {
  return (
    <div className="animate-slide-up">
      <header className="page-header">
        <div>
          <span className="mb-1 inline-block text-xs font-semibold uppercase tracking-wider text-indigo-500 dark:text-indigo-400">
            Owner
          </span>
          <div className="text-lg font-semibold">{owner}</div>
        </div>
        <div></div>
      </header>
      <h2>Repositories</h2>
      <div className="repo-grid">
        {repos.length ? (
          repos.map((repo) => (
            <a
              key={repo}
              href={`/${owner}/${repo}`}
              className="group block card p-5 transition-all hover:border-indigo-500/30 hover:shadow-md dark:hover:border-indigo-500/30"
            >
              <div className="flex items-center gap-3">
                <span className="inline-grid h-9 w-9 shrink-0 place-items-center rounded-xl bg-zinc-100 text-zinc-500 transition-colors group-hover:bg-indigo-50 group-hover:text-indigo-500 dark:bg-zinc-800 dark:text-zinc-400 dark:group-hover:bg-indigo-900/20 dark:group-hover:text-indigo-400">
                  <i
                    className="bi bi-journal-code"
                    style={{ fontSize: "1.1rem" }}
                    aria-hidden="true"
                  ></i>
                </span>
                <div className="text-lg font-medium">
                  <span className="text-zinc-500 dark:text-zinc-400">{owner}</span>
                  <span className="mx-1 text-zinc-300 dark:text-zinc-600">/</span>
                  <span className="transition-colors group-hover:text-indigo-500 dark:group-hover:text-indigo-400">
                    {repo}
                  </span>
                </div>
              </div>
            </a>
          ))
        ) : (
          <div className="col-span-full">
            <EmptyState
              iconClass="bi bi-journal-code"
              title="No repositories yet"
              detail="Push a repository to get started."
              large
            />
          </div>
        )}
      </div>
    </div>
  );
}
