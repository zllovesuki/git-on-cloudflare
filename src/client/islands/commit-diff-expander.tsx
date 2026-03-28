/// <reference lib="dom" />

import type { CommitFilePatchResult } from "@/git";
import { Fragment, useEffect, useState } from "react";

import { hydrateIsland } from "@/client/hydrate";
import { inferHljsLang } from "@/web/syntax.ts";

export type CommitDiffEntryView = {
  path: string;
  changeType: "A" | "M" | "D";
  oldOid?: string;
  newOid?: string;
  oldMode?: string;
  newMode?: string;
};

export type CommitDiffSummaryView = {
  added: number;
  modified: number;
  deleted: number;
  total: number;
};

export type CommitDiffExpanderProps = {
  owner: string;
  repo: string;
  commitOid: string;
  refEnc: string;
  diffBaseRefEnc: string;
  diffCompareMode: "root" | "first-parent";
  diffEntries: CommitDiffEntryView[];
  diffSummary: CommitDiffSummaryView;
  diffTruncated: boolean;
  diffTruncateReason: "" | "max_files" | "max_tree_pairs" | "time_budget" | "soft_budget";
  parentsCount: number;
};

const CHANGE_META: Record<
  CommitDiffEntryView["changeType"],
  { label: string; cls: string; dot: string }
> = {
  A: {
    label: "Added",
    cls: "diff-badge-add",
    dot: "●",
  },
  M: {
    label: "Modified",
    cls: "diff-badge-mod",
    dot: "●",
  },
  D: {
    label: "Deleted",
    cls: "diff-badge-del",
    dot: "●",
  },
};

function diffHref(
  entry: CommitDiffEntryView,
  owner: string,
  repo: string,
  refEnc: string,
  diffBaseRefEnc: string
) {
  if (entry.changeType === "D") {
    if (!diffBaseRefEnc) return null;
    return `/${owner}/${repo}/blob?ref=${diffBaseRefEnc}&path=${encodeURIComponent(entry.path)}`;
  }
  return `/${owner}/${repo}/blob?ref=${refEnc}&path=${encodeURIComponent(entry.path)}`;
}

function patchSummary(patch: CommitFilePatchResult): string {
  if (patch.skipReason === "binary") {
    return "Binary file — patch preview is unavailable.";
  }
  if (patch.skipReason === "too_many_lines") {
    return "File has too many lines to render a patch preview.";
  }
  if (patch.skipReason === "too_large") {
    return "File is too large to render a patch preview.";
  }
  return "Patch preview is unavailable.";
}

/** Classify a raw diff line for styling. */
function lineType(line: string): "add" | "del" | "hunk" | "ctx" {
  if (line.startsWith("+")) return "add";
  if (line.startsWith("-")) return "del";
  if (line.startsWith("@@")) return "hunk";
  return "ctx";
}

const LINE_TYPE_CLASS: Record<ReturnType<typeof lineType>, string> = {
  add: "diff-line-add",
  del: "diff-line-del",
  hunk: "diff-line-hunk",
  ctx: "",
};

/** Render a patch string as syntax-highlighted, line-colored diff table. */
function DiffPatchView({ patch, filePath }: { patch: string; filePath: string }) {
  const [renderedPatch, setRenderedPatch] = useState(() => buildDiffPatchMarkup(patch, filePath));

  useEffect(() => {
    let cancelled = false;

    setRenderedPatch(buildDiffPatchMarkup(patch, filePath));

    const lang = inferHljsLang(filePath);
    if (!lang) {
      return () => {
        cancelled = true;
      };
    }

    void import("@/client/components/highlight")
      .then(({ highlightCode }) => {
        if (cancelled) {
          return;
        }

        setRenderedPatch(buildDiffPatchMarkup(patch, filePath, highlightCode));
      })
      .catch((error) => {
        console.error("Failed to load syntax highlighter", error);
      });

    return () => {
      cancelled = true;
    };
  }, [filePath, patch]);

  return (
    <div className="diff-patch-container">
      <pre className="diff-patch-pre">
        <table className={`diff-patch-table hljs ${renderedPatch.languageClass}`}>
          <tbody>
            {renderedPatch.rows.map((row, i) => (
              <tr key={i} className={LINE_TYPE_CLASS[row.type]}>
                <td className="diff-gutter">{row.prefix}</td>
                <td
                  className="diff-code"
                  dangerouslySetInnerHTML={{ __html: row.html || "&nbsp;" }}
                />
              </tr>
            ))}
          </tbody>
        </table>
      </pre>
    </div>
  );
}

function buildDiffPatchMarkup(
  patch: string,
  filePath: string,
  highlight?: (code: string, language?: string | null) => { html: string; languageClass: string }
) {
  const lang = inferHljsLang(filePath);
  const rawLines = patch.split("\n");

  // Remove trailing empty line from split
  if (rawLines.length > 1 && rawLines[rawLines.length - 1] === "") {
    rawLines.pop();
  }

  // Split lines into segments: header (before first hunk) and code lines
  const rows: Array<{
    type: ReturnType<typeof lineType>;
    prefix: string;
    html: string;
  }> = [];

  let inHeader = true;

  for (const raw of rawLines) {
    if (inHeader) {
      if (raw.startsWith("@@")) {
        inHeader = false;
        rows.push({ type: "hunk", prefix: "", html: escapeHtml(raw) });
      } else {
        // diff header lines (--- a/... +++ b/... etc.)
        rows.push({ type: "hunk", prefix: "", html: escapeHtml(raw) });
      }
      continue;
    }

    const lt = lineType(raw);
    if (lt === "hunk") {
      rows.push({ type: "hunk", prefix: "", html: escapeHtml(raw) });
      continue;
    }

    // Strip prefix character for highlighting, keep it for gutter
    const prefix = raw.length > 0 ? raw[0] : " ";
    const code = raw.length > 1 ? raw.slice(1) : "";
    const highlighted = highlight ? highlight(code, lang) : null;

    rows.push({ type: lt, prefix, html: highlighted ? highlighted.html : escapeHtml(code) });
  }

  return {
    rows,
    languageClass: lang ? `language-${lang}` : "language-plaintext",
  };
}

function escapeHtml(str: string): string {
  return str
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

export function CommitDiffExpanderIsland({
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
  parentsCount,
}: CommitDiffExpanderProps) {
  const [expandedByPath, setExpandedByPath] = useState<Record<string, boolean>>({});
  const [loadingByPath, setLoadingByPath] = useState<Record<string, boolean>>({});
  const [errorByPath, setErrorByPath] = useState<Record<string, string | null>>({});
  const [patchByPath, setPatchByPath] = useState<Record<string, CommitFilePatchResult | undefined>>(
    {}
  );

  async function togglePatch(path: string) {
    if (loadingByPath[path]) {
      return;
    }

    if (expandedByPath[path]) {
      setExpandedByPath((current) => ({ ...current, [path]: false }));
      return;
    }

    setExpandedByPath((current) => ({ ...current, [path]: true }));
    setErrorByPath((current) => ({ ...current, [path]: null }));

    if (patchByPath[path]) {
      return;
    }

    setLoadingByPath((current) => ({ ...current, [path]: true }));
    try {
      const response = await fetch(
        `/${owner}/${repo}/commit/${commitOid}/diff?path=${encodeURIComponent(path)}`,
        {
          headers: { Accept: "application/json" },
        }
      );
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      const data = (await response.json()) as CommitFilePatchResult;
      setPatchByPath((current) => ({ ...current, [path]: data }));
    } catch (error) {
      setErrorByPath((current) => ({
        ...current,
        [path]: error instanceof Error ? error.message : "Failed to load patch",
      }));
    } finally {
      setLoadingByPath((current) => ({ ...current, [path]: false }));
    }
  }

  return (
    <div className="diff-viewer">
      {/* Summary bar */}
      <div className="diff-summary">
        <div className="diff-summary-stats">
          <strong>
            {diffSummary.total} file{diffSummary.total !== 1 ? "s" : ""} changed
          </strong>
          {diffSummary.added > 0 ? (
            <span className="diff-stat-add">+{diffSummary.added} added</span>
          ) : null}
          {diffSummary.modified > 0 ? (
            <span className="diff-stat-mod">~{diffSummary.modified} modified</span>
          ) : null}
          {diffSummary.deleted > 0 ? (
            <span className="diff-stat-del">-{diffSummary.deleted} deleted</span>
          ) : null}
        </div>
        {diffCompareMode === "first-parent" && parentsCount > 1 ? (
          <span className="diff-compare-label">Compared against first parent</span>
        ) : null}
      </div>

      {/* Truncation warning */}
      {diffTruncated ? (
        <div className="rounded-xl border border-amber-500/20 bg-amber-500/10 p-4 text-sm text-amber-300 mx-4 mt-3 mb-0">
          Showing a partial diff list due to runtime limits
          {diffTruncateReason ? ` (${diffTruncateReason})` : ""}.
        </div>
      ) : null}

      {/* File list */}
      {diffEntries.length ? (
        <div className="diff-file-list">
          {diffEntries.map((entry) => {
            const href = diffHref(entry, owner, repo, refEnc, diffBaseRefEnc);
            const isExpanded = Boolean(expandedByPath[entry.path]);
            const isLoading = Boolean(loadingByPath[entry.path]);
            const error = errorByPath[entry.path];
            const patch = patchByPath[entry.path];
            const meta = CHANGE_META[entry.changeType];

            return (
              <div key={`${entry.changeType}:${entry.path}`} className="diff-file-entry">
                {/* File row */}
                <div className="diff-file-row">
                  <div className="diff-file-info">
                    <span className={`diff-badge ${meta.cls}`} title={meta.label}>
                      {entry.changeType}
                    </span>
                    <span className="diff-file-path">
                      {href ? <a href={href}>{entry.path}</a> : <span>{entry.path}</span>}
                    </span>
                  </div>
                  <button
                    className={`diff-toggle-btn ${isExpanded ? "active" : ""}`}
                    type="button"
                    onClick={() => void togglePatch(entry.path)}
                  >
                    <span className="diff-toggle-icon">{isExpanded ? "▾" : "▸"}</span>
                    {isExpanded ? "Hide patch" : "Show patch"}
                  </button>
                </div>

                {/* Expanded patch area */}
                {isExpanded ? (
                  <div className="diff-expand-area">
                    {isLoading ? (
                      <div className="diff-loading">
                        <span className="diff-spinner" />
                        Loading patch…
                      </div>
                    ) : null}
                    {!isLoading && error ? (
                      <div className="rounded-xl border border-red-500/20 bg-red-500/10 p-4 text-sm text-red-400 mb-0">
                        Failed to load patch: {error}
                      </div>
                    ) : null}
                    {!isLoading && !error && patch?.patch ? (
                      <DiffPatchView patch={patch.patch} filePath={entry.path} />
                    ) : null}
                    {!isLoading && !error && patch && !patch.patch ? (
                      <div className="diff-skip-msg">{patchSummary(patch)}</div>
                    ) : null}
                  </div>
                ) : null}
              </div>
            );
          })}
        </div>
      ) : (
        <div className="diff-empty">No changed files were detected for this commit.</div>
      )}
    </div>
  );
}

export function initCommitDiffExpander() {
  hydrateIsland<CommitDiffExpanderProps>("commit-diff-expander", CommitDiffExpanderIsland);
}
