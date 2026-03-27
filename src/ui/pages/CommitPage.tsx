import { RepoNav } from "@/ui/components/RepoNav";
import { CommitDiffExpanderIsland } from "@/ui/islands/commit-diff-expander";
import { IslandHost } from "@/ui/server/IslandHost";

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
    <div className="animate-slide-up">
      <RepoNav owner={owner} repo={repo} refEnc={refEnc} currentTab="commits" />
      <span className="mb-1 inline-block text-xs font-semibold uppercase tracking-wider text-accent-500 dark:text-accent-400">
        Commit Detail
      </span>
      <h2>Commit {commitShort}</h2>
      <p>
        <strong>Author:</strong> {authorName} &lt;{authorEmail}&gt;{" "}
        <span className="text-zinc-500 dark:text-zinc-400">{when}</span>
      </p>
      <p>
        <strong>Parents:</strong>{" "}
        {parents.length ? (
          parents.map((parent, index) => (
            <span key={parent.oid}>
              {index > 0 ? ", " : null}
              <a href={`/${owner}/${repo}/commit/${parent.oid}`}>{parent.short}</a>
            </span>
          ))
        ) : (
          <span className="text-zinc-500 dark:text-zinc-400">(none)</span>
        )}
      </p>
      <p>
        <strong>Tree:</strong> <a href={`/${owner}/${repo}/tree?ref=${refEnc}`}>{treeShort}</a>
      </p>
      <pre>{message}</pre>
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
