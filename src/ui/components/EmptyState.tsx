type EmptyStateProps = {
  iconClass: string;
  title: string;
  detail?: string;
  large?: boolean;
};

export function EmptyState({ iconClass, title, detail, large = false }: EmptyStateProps) {
  return (
    <div
      className={`flex flex-col items-center text-center ${large ? "gap-3 py-10" : "gap-2 py-6"}`}
    >
      <span
        className={`inline-grid place-items-center rounded-full bg-zinc-100 dark:bg-zinc-800/60 ${large ? "h-14 w-14" : "h-10 w-10"}`}
      >
        <i
          className={`${iconClass} text-zinc-400 dark:text-zinc-600`}
          style={{ fontSize: large ? "1.75rem" : "1.25rem" }}
          aria-hidden="true"
        ></i>
      </span>
      <p className="mb-0 text-sm text-zinc-500 dark:text-zinc-400">{title}</p>
      {detail ? <p className="mb-0 text-xs text-zinc-400 dark:text-zinc-500">{detail}</p> : null}
    </div>
  );
}
