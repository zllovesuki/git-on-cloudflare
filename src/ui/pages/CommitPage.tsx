import { RepoNav } from "@/ui/components/RepoNav";

type Parent = {
  oid: string;
  short: string;
};

export type CommitPageProps = {
  owner: string;
  repo: string;
  refEnc: string;
  commitShort: string;
  authorName: string;
  authorEmail: string;
  when: string;
  parents: Parent[];
  treeShort: string;
  message: string;
};

export function CommitPage({
  owner,
  repo,
  refEnc,
  commitShort,
  authorName,
  authorEmail,
  when,
  parents,
  treeShort,
  message,
}: CommitPageProps) {
  return (
    <>
      <RepoNav owner={owner} repo={repo} refEnc={refEnc} currentTab="commits" />
      <span className="mb-1 inline-block text-xs font-semibold uppercase tracking-wider text-indigo-500 dark:text-indigo-400">
        Commit Detail
      </span>
      <h2>Commit {commitShort}</h2>
      <p>
        <strong>Author:</strong> {authorName} &lt;{authorEmail}&gt;{" "}
        <span className="muted">{when}</span>
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
          <span className="muted">(none)</span>
        )}
      </p>
      <p>
        <strong>Tree:</strong> <a href={`/${owner}/${repo}/tree?ref=${refEnc}`}>{treeShort}</a>
      </p>
      <pre>{message}</pre>
    </>
  );
}
