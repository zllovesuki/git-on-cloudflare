import { MarkdownContent } from "@/ui/components/MarkdownContent";
import { type Progress, ProgressBanner } from "@/ui/components/ProgressBanner";
import { RepoNav } from "@/ui/components/RepoNav";
import { EmptyState } from "@/ui/components/EmptyState";

type RefLink = {
  name: string;
  displayName: string;
};

export type OverviewPageProps = {
  owner: string;
  repo: string;
  refShort: string;
  refEnc: string;
  branches: RefLink[];
  tags: RefLink[];
  readmeMd?: string;
  progress?: Progress;
};

export function OverviewPage({
  owner,
  repo,
  refShort,
  refEnc,
  branches,
  tags,
  readmeMd,
  progress,
}: OverviewPageProps) {
  return (
    <>
      <RepoNav owner={owner} repo={repo} refEnc={refEnc} showRefDropdown={false} />
      <ProgressBanner progress={progress} />
      <div className="space-y-6 animate-slide-up">
        <section className="rounded-2xl border border-indigo-500/20 bg-white p-6 dark:bg-zinc-900/80">
          <span className="mb-2 inline-block text-xs font-semibold uppercase tracking-wider text-indigo-500 dark:text-indigo-400">
            Repository
          </span>
          <h2 className="m-0">Overview</h2>
          <p className="muted mt-1 mb-0">
            Default branch: <code>{refShort}</code>
          </p>
        </section>
        <div className="repo-grid">
          <div className="card p-5">
            <span className="mb-1 inline-block text-xs font-semibold uppercase tracking-wider text-zinc-500 dark:text-zinc-400">
              Refs
            </span>
            <h3 className="flex items-center gap-2">
              <i className="bi bi-git inline h-4 w-4 text-indigo-500" aria-hidden="true"></i>
              Branches
            </h3>
            <div className="list">
              {branches.length ? (
                branches.map((branch) => (
                  <div key={branch.name}>
                    <a href={`/${owner}/${repo}/tree?ref=${branch.name}`}>{branch.displayName}</a>
                  </div>
                ))
              ) : (
                <EmptyState iconClass="bi bi-git" title="No branches yet" />
              )}
            </div>
          </div>
          <div className="card p-5">
            <span className="mb-1 inline-block text-xs font-semibold uppercase tracking-wider text-zinc-500 dark:text-zinc-400">
              Refs
            </span>
            <h3 className="flex items-center gap-2">
              <i className="bi bi-tag-fill inline h-4 w-4 text-indigo-500" aria-hidden="true"></i>
              Tags
            </h3>
            <div className="list">
              {tags.length ? (
                tags.map((tag) => (
                  <div key={tag.name}>
                    <a href={`/${owner}/${repo}/tree?ref=${tag.name}`}>{tag.displayName}</a>
                  </div>
                ))
              ) : (
                <EmptyState iconClass="bi bi-tag-fill" title="No tags yet" />
              )}
            </div>
          </div>
        </div>
        {readmeMd ? (
          <div className="mt-8 rounded-2xl border border-zinc-200 bg-white p-6 dark:border-zinc-800/60 dark:bg-zinc-900/50">
            <MarkdownContent
              markdown={readmeMd}
              context={{ owner, repo, ref: refShort, baseDir: "" }}
            />
          </div>
        ) : null}
      </div>
    </>
  );
}
