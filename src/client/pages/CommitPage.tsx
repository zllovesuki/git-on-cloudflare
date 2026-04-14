import { RepoNav } from "@/client/components/RepoNav";
import { Card } from "@/client/components/ui/card";
import { CommitDiffExpanderIsland } from "@/client/islands/commit-diff-expander";
import { IslandHost } from "@/client/server/IslandHost";

type Parent = {
  oid: string;
  short: string;
};

type DiffEntry = {
  path: string;
  changeType: "A" | "M" | "D";
  oldOid?: string;
  newOid?: string;
  oldMode?: string;
  newMode?: string;
};

type DiffSummary = {
  added: number;
  modified: number;
  deleted: number;
  total: number;
};

export type CommitPageProps = {
  owner: string;
  repo: string;
  commitOid: string;
  refEnc: string;
  commitShort: string;
  authorName: string;
  authorEmail: string;
  when: string;
  parents: Parent[];
  treeShort: string;
  message: string;
  diffBaseRefEnc: string;
  diffCompareMode: "root" | "first-parent";
  diffEntries: DiffEntry[];
  diffSummary: DiffSummary;
  diffTruncated: boolean;
  diffTruncateReason: "" | "max_files" | "max_tree_pairs" | "time_budget" | "soft_budget";
};

export function CommitPage({
  owner,
  repo,
  commitOid,
  refEnc,
  commitShort,
  authorName,
  authorEmail,
  when,
  parents,
  treeShort,
  message,
  diffBaseRefEnc,
  diffCompareMode,
  diffEntries,
  diffSummary,
  diffTruncated,
  diffTruncateReason,
}: CommitPageProps) {
  return (
    <div>
      <RepoNav owner={owner} repo={repo} refEnc={refEnc} currentTab="commits" />
      <span className="mb-1 inline-block text-xs font-semibold uppercase tracking-wider text-accent-500 dark:text-accent-400">
        Commit Detail
      </span>
      <h2 className="font-display tracking-tight">Commit {commitShort}</h2>

      <div className="space-y-4">
        <Card>
          <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-2 text-sm">
            <dt className="font-medium text-zinc-500 dark:text-zinc-400">Author</dt>
            <dd>
              {authorName} &lt;{authorEmail}&gt;{" "}
              <span className="text-zinc-500 dark:text-zinc-400">{when}</span>
            </dd>
            <dt className="font-medium text-zinc-500 dark:text-zinc-400">Parents</dt>
            <dd>
              {parents.length ? (
                parents.map((parent, index) => (
                  <span key={parent.oid}>
                    {index > 0 ? ", " : null}
                    <a href={`/${owner}/${repo}/commit/${parent.oid}`}>{parent.short}</a>
                  </span>
                ))
              ) : (
                <span className="text-zinc-500 dark:text-zinc-400">(root commit)</span>
              )}
            </dd>
            <dt className="font-medium text-zinc-500 dark:text-zinc-400">Tree</dt>
            <dd>
              <a href={`/${owner}/${repo}/tree?ref=${refEnc}`}>{treeShort}</a>
            </dd>
          </dl>
        </Card>
        <Card>
          <pre className="!m-0 whitespace-pre-wrap !rounded-none !border-0 !bg-transparent !p-0">
            {message}
          </pre>
        </Card>
      </div>

      <h3>Files changed</h3>
      <IslandHost
        name="commit-diff-expander"
        props={{
          owner,
          repo,
          commitOid,
          refEnc,
          diffBaseRefEnc,
          diffCompareMode,
          diffEntries,
          diffSummary,
          diffTruncated,
          diffTruncateReason,
          parentsCount: parents.length,
        }}
      >
        <CommitDiffExpanderIsland
          owner={owner}
          repo={repo}
          commitOid={commitOid}
          refEnc={refEnc}
          diffBaseRefEnc={diffBaseRefEnc}
          diffCompareMode={diffCompareMode}
          diffEntries={diffEntries}
          diffSummary={diffSummary}
          diffTruncated={diffTruncated}
          diffTruncateReason={diffTruncateReason}
          parentsCount={parents.length}
        />
      </IslandHost>
    </div>
  );
}
