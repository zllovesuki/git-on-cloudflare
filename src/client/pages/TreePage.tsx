import type { FileIconName } from "@/web";
import { Breadcrumbs } from "@/client/components/Breadcrumbs";
import { EmptyState } from "@/client/components/EmptyState";
import { FileIcon } from "@/client/components/FileIcon";
import { type Progress, ProgressBanner } from "@/client/components/ProgressBanner";
import { RepoNav } from "@/client/components/RepoNav";

type TreeEntry = {
  name: string;
  href: string;
  isDir: boolean;
  iconName: FileIconName;
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
      <div>
        <span className="mb-1 inline-block text-xs font-semibold uppercase tracking-wider text-accent-500 dark:text-accent-400">
          Browse
        </span>
        <h2 className="font-display tracking-tight">Tree</h2>
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
                    <FileIcon
                      name={entry.iconName}
                      className={`mr-1.5 inline-block h-4 w-4 align-[-2px] ${entry.isDir ? "text-amber-600 dark:text-amber-400" : "text-zinc-500 dark:text-zinc-400"}`}
                    />{" "}
                    <a href={entry.href}>{entry.name}</a>
                  </td>
                  <td className="text-zinc-500 dark:text-zinc-400">{entry.shortOid}</td>
                </tr>
              ))
            ) : (
              <tr>
                <td colSpan={2}>
                  <EmptyState
                    icon={
                      <FileIcon
                        name="folder"
                        className="h-5 w-5 text-zinc-500 dark:text-zinc-400"
                      />
                    }
                    title="This tree is empty"
                  />
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </>
  );
}
