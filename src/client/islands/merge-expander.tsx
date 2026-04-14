/// <reference lib="dom" />

import type { MouseEvent } from "react";
import { Fragment, useState } from "react";

import { hydrateIsland } from "@/client/hydrate";
import { CommitRow } from "@/client/components/CommitRow";

type CommitView = {
  oid: string;
  shortOid: string;
  firstLine: string;
  authorName: string;
  when: string;
  isMerge?: boolean;
};

export type MergeExpanderProps = {
  owner: string;
  repo: string;
  commits: CommitView[];
};

function MergeStatusRow({ message }: { message: string }) {
  return (
    <tr>
      <td colSpan={4} className="text-zinc-500 dark:text-zinc-400">
        {message}
      </td>
    </tr>
  );
}

export function MergeExpanderIsland({ owner, repo, commits }: MergeExpanderProps) {
  const [expandedByOid, setExpandedByOid] = useState<Record<string, boolean>>({});
  const [loadingByOid, setLoadingByOid] = useState<Record<string, boolean>>({});
  const [errorByOid, setErrorByOid] = useState<Record<string, string | null>>({});
  const [mergeRowsByOid, setMergeRowsByOid] = useState<Record<string, CommitView[]>>({});

  async function toggleMerge(oid: string) {
    if (loadingByOid[oid]) {
      return;
    }

    if (expandedByOid[oid]) {
      setExpandedByOid((current) => ({ ...current, [oid]: false }));
      return;
    }

    if (mergeRowsByOid[oid]) {
      setExpandedByOid((current) => ({ ...current, [oid]: true }));
      setErrorByOid((current) => ({ ...current, [oid]: null }));
      return;
    }

    setLoadingByOid((current) => ({ ...current, [oid]: true }));
    setExpandedByOid((current) => ({ ...current, [oid]: true }));
    setErrorByOid((current) => ({ ...current, [oid]: null }));

    try {
      const response = await fetch(`/${owner}/${repo}/commits/fragments/${oid}?limit=20`, {
        headers: { Accept: "application/json" },
      });
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }

      const data = (await response.json()) as { commits?: CommitView[] };
      setMergeRowsByOid((current) => ({ ...current, [oid]: data.commits || [] }));
    } catch (error) {
      setErrorByOid((current) => ({
        ...current,
        [oid]: error instanceof Error ? error.message : "Failed to load merge commits",
      }));
    } finally {
      setLoadingByOid((current) => ({ ...current, [oid]: false }));
    }
  }

  function onMergeRowClick(oid: string) {
    return (event: MouseEvent<HTMLTableRowElement>) => {
      const target = event.target as HTMLElement;
      if (target.closest("a, button")) {
        return;
      }

      void toggleMerge(oid);
    };
  }

  return (
    <table
      id="commits-table"
      data-owner={owner}
      data-repo={repo}
      className="mt-4 overflow-hidden rounded-2xl border border-zinc-200 shadow-xs dark:border-zinc-800/60"
    >
      <thead>
        <tr>
          <th>OID</th>
          <th>Message</th>
          <th>Author</th>
          <th>Date</th>
        </tr>
      </thead>
      <tbody>
        {commits.length ? (
          commits.map((commit) => {
            const isMerge = Boolean(commit.isMerge);
            const mergeOid = commit.oid;
            const isExpanded = Boolean(expandedByOid[mergeOid]);
            const isLoading = Boolean(loadingByOid[mergeOid]);
            const mergeRows = mergeRowsByOid[mergeOid] || [];
            const error = errorByOid[mergeOid];
            const mergeRowClass = isMerge
              ? "cursor-pointer bg-accent-50/40 hover:bg-accent-50/60 dark:bg-accent-900/15 dark:hover:bg-accent-900/25"
              : "";

            return (
              <Fragment key={commit.oid}>
                <CommitRow
                  owner={owner}
                  repo={repo}
                  commit={commit}
                  isMerge={isMerge}
                  rowClass={mergeRowClass}
                  toggleOid={isMerge ? mergeOid : undefined}
                  mergeExpanded={isExpanded}
                  onToggle={isMerge ? onMergeRowClick(mergeOid) : undefined}
                />
                {isMerge && isExpanded && isLoading ? (
                  <MergeStatusRow message="Loading..." />
                ) : null}
                {isMerge && isExpanded && !isLoading && error ? (
                  <MergeStatusRow message={`Failed to load merge commits: ${error}`} />
                ) : null}
                {isMerge && isExpanded && !isLoading && !error && !mergeRows.length ? (
                  <MergeStatusRow message="(No commits to show for this merge yet)" />
                ) : null}
                {isMerge && isExpanded && !isLoading && !error
                  ? mergeRows.map((entry) => (
                      <CommitRow
                        key={entry.oid}
                        owner={owner}
                        repo={repo}
                        commit={entry}
                        compact
                        mergeOf={mergeOid}
                      />
                    ))
                  : null}
              </Fragment>
            );
          })
        ) : (
          <tr>
            <td colSpan={4} className="text-zinc-500 dark:text-zinc-400">
              (none)
            </td>
          </tr>
        )}
      </tbody>
    </table>
  );
}

export function initMergeExpander() {
  hydrateIsland<MergeExpanderProps>("merge-expander", MergeExpanderIsland);
}
