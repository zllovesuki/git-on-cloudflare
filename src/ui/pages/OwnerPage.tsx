import { Folder } from "lucide-react";
import { EmptyState } from "@/ui/components/EmptyState";
import { PageHeader } from "@/ui/components/ui/page-header";

export type OwnerPageProps = {
  owner: string;
  repos: string[];
};

export function OwnerPage({ owner, repos }: OwnerPageProps) {
  return (
    <div className="animate-slide-up">
      <PageHeader>
        <div>
          <span className="mb-1 inline-block text-xs font-semibold uppercase tracking-wider text-accent-500 dark:text-accent-400">
            Owner
          </span>
          <div className="text-lg font-semibold">{owner}</div>
        </div>
        <div></div>
      </PageHeader>
      <h2>Repositories</h2>
      <div className="mt-6 grid gap-6 md:grid-cols-2">
        {repos.length ? (
          repos.map((repo, i) => (
            <a
              key={repo}
              href={`/${owner}/${repo}`}
              className="group block animate-slide-up opacity-0 rounded-2xl border border-zinc-200 dark:border-zinc-800/60 bg-white dark:bg-zinc-900/50 p-5 hover:-translate-y-0.5 transition-transform cursor-pointer hover:border-zinc-300 dark:hover:border-zinc-700/60 hover:shadow-sm"
              style={{ animationDelay: `${Math.min(i, 7) * 60}ms` }}
            >
              <div className="flex items-center gap-3">
                <span className="inline-grid h-9 w-9 shrink-0 place-items-center rounded-xl bg-zinc-100 text-zinc-500 transition-colors group-hover:bg-accent-50 group-hover:text-accent-500 dark:bg-zinc-800 dark:text-zinc-400 dark:group-hover:bg-accent-900/20 dark:group-hover:text-accent-400">
                  <Folder className="h-[1.1rem] w-[1.1rem]" aria-hidden="true" />
                </span>
                <div className="text-lg font-medium">
                  <span className="text-zinc-500 dark:text-zinc-400">{owner}</span>
                  <span className="mx-1 text-zinc-300 dark:text-zinc-600">/</span>
                  <span className="transition-colors group-hover:text-accent-500 dark:group-hover:text-accent-400">
                    {repo}
                  </span>
                </div>
              </div>
            </a>
          ))
        ) : (
          <div className="col-span-full">
            <EmptyState
              icon={
                <Folder className="h-7 w-7 text-zinc-500 dark:text-zinc-400" aria-hidden="true" />
              }
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
