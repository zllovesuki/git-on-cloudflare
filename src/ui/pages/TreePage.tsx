import { Breadcrumbs } from "@/ui/components/Breadcrumbs";
import { EmptyState } from "@/ui/components/EmptyState";
import { type Progress, ProgressBanner } from "@/ui/components/ProgressBanner";
import { RepoNav } from "@/ui/components/RepoNav";

type TreeEntry = {
  name: string;
  href: string;
  isDir: boolean;
  iconClass: string;
  shortOid: string;
};

type Breadcrumb = {
  name: string;
  href: string | null;
};

export type TreePageProps = {
  owner: string;
  repo: string;
  refEnc: string;
  entries: TreeEntry[];
  breadcrumbs?: Breadcrumb[];
  parentHref?: string | null;
  progress?: Progress;
};

export function TreePage({
  owner,
  repo,
  refEnc,
  entries,
  breadcrumbs,
  parentHref,
  progress,
}: TreePageProps) {
  return (
    <>
      <RepoNav owner={owner} repo={repo} refEnc={refEnc} currentTab="browse" />
      <ProgressBanner progress={progress} />
      <div className="animate-slide-up">
        <span className="mb-1 inline-block text-xs font-semibold uppercase tracking-wider text-indigo-500 dark:text-indigo-400">
          Browse
        </span>
        <h2>Tree</h2>
        <Breadcrumbs items={breadcrumbs} parentHref={parentHref} />
        <table className="mt-4 overflow-hidden rounded-2xl border border-zinc-200 shadow-xs dark:border-zinc-800/60">
          <thead>
            <tr>
              <th>Name</th>
              <th>OID</th>
            </tr>
          </thead>
          <tbody>
            {entries.length ? (
              entries.map((entry) => (
                <tr key={`${entry.href}-${entry.name}`}>
                  <td>
                    <i
                      className={`${entry.iconClass} mr-1.5 inline-block align-[-2px] ${entry.isDir ? "text-amber-600 dark:text-amber-400" : "text-zinc-500 dark:text-zinc-400"}`}
                      aria-hidden="true"
                    ></i>{" "}
                    <a href={entry.href}>{entry.name}</a>
                  </td>
                  <td className="muted">{entry.shortOid}</td>
                </tr>
              ))
            ) : (
              <tr>
                <td colSpan={2}>
                  <EmptyState iconClass="bi bi-folder" title="This tree is empty" />
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </>
  );
}
