import type { KeyboardEventHandler, MouseEventHandler } from "react";

type CommitView = {
  oid: string;
  shortOid: string;
  firstLine: string;
  authorName: string;
  when: string;
};

type CommitRowProps = {
  owner: string;
  repo: string;
  commit: CommitView;
  compact?: boolean;
  isMerge?: boolean;
  rowClass?: string;
  mergeOf?: string;
  toggleOid?: string;
  mergeExpanded?: boolean;
  onToggle?: MouseEventHandler<HTMLTableRowElement>;
};

export function CommitRow({
  owner,
  repo,
  commit,
  compact,
  isMerge,
  rowClass,
  mergeOf,
  toggleOid,
  mergeExpanded,
  onToggle,
}: CommitRowProps) {
  const classes = [
    compact
      ? "text-sm subrow bg-accent-50/20 dark:bg-accent-900/10 border-l-2 border-b border-b-zinc-200 border-l-accent-400 dark:border-b-zinc-800 dark:border-l-accent-700"
      : "",
    rowClass || "",
  ]
    .filter(Boolean)
    .join(" ");

  const onKeyDown: KeyboardEventHandler<HTMLTableRowElement> | undefined = onToggle
    ? (event) => {
        if (event.key !== "Enter" && event.key !== " ") {
          return;
        }
        event.preventDefault();
        onToggle(event as unknown as Parameters<NonNullable<typeof onToggle>>[0]);
      }
    : undefined;

  return (
    <tr
      className={classes || undefined}
      data-merge-of={mergeOf || undefined}
      data-merge-oid={toggleOid || undefined}
      onClick={onToggle}
      onKeyDown={onKeyDown}
      role={onToggle ? "button" : undefined}
      tabIndex={onToggle ? 0 : undefined}
      aria-expanded={onToggle ? mergeExpanded : undefined}
    >
      <td>
        <a href={`/${owner}/${repo}/commit/${commit.oid}`}>{commit.shortOid}</a>
      </td>
      <td className={compact ? "pl-6" : undefined}>
        {isMerge ? (
          <span
            className={`merge-badge mr-2 inline-block rounded px-2 py-0.5 text-xs ${
              mergeExpanded
                ? "bg-amber-200 text-amber-900 dark:border dark:border-amber-500/40 dark:bg-amber-900/40 dark:text-amber-200"
                : "bg-sky-100 text-sky-900 dark:border dark:border-sky-500/40 dark:bg-sky-500/20 dark:text-sky-300"
            }`}
          >
            Merge
          </span>
        ) : null}
        {commit.firstLine}
      </td>
      <td className="text-zinc-500 dark:text-zinc-400">{commit.authorName}</td>
      <td className="text-zinc-500 dark:text-zinc-400">{commit.when}</td>
    </tr>
  );
}
