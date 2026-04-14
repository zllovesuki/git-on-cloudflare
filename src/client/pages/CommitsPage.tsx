import { Pager } from "@/client/components/Pager";
import { type Progress, ProgressBanner } from "@/client/components/ProgressBanner";
import { RepoNav } from "@/client/components/RepoNav";
import { MergeExpanderIsland } from "@/client/islands/merge-expander";
import { IslandHost } from "@/client/server/IslandHost";

type CommitView = {
  oid: string;
  shortOid: string;
  firstLine: string;
  authorName: string;
  when: string;
  isMerge?: boolean;
};

type PagerModel = {
  perPageLinks: Array<{ text: string; href: string }>;
  newerHref?: string;
  olderHref?: string;
};

export type CommitsPageProps = {
  owner: string;
  repo: string;
  ref: string;
  refEnc: string;
  commits: CommitView[];
  pager?: PagerModel;
  progress?: Progress;
};

export function CommitsPage({
  owner,
  repo,
  ref,
  refEnc,
  commits,
  pager,
  progress,
}: CommitsPageProps) {
  const refLabel =
    ref.length === 40 ? (
      <>
        <span className="font-mono sm:hidden">{ref.slice(0, 12)}…</span>
        <span className="hidden font-mono sm:inline">{ref}</span>
      </>
    ) : (
      <span className="font-mono">{ref}</span>
    );

  return (
    <div>
      <RepoNav owner={owner} repo={repo} refEnc={refEnc} currentTab="commits" />
      <ProgressBanner progress={progress} />
      <span className="mb-1 inline-block text-xs font-semibold uppercase tracking-wider text-accent-500 dark:text-accent-400">
        History
      </span>
      <h2 className="font-display tracking-tight">Commits on {refLabel}</h2>
      <Pager pager={pager} />
      <IslandHost name="merge-expander" props={{ owner, repo, commits }}>
        <MergeExpanderIsland owner={owner} repo={repo} commits={commits} />
      </IslandHost>
    </div>
  );
}
