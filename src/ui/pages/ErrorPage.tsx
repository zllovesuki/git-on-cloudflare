export type ErrorPageProps = {
  message?: string;
  stack?: string;
  owner?: string;
  repo?: string;
  refEnc?: string;
  path?: string;
};

export function ErrorPage({ message, stack, owner, repo, refEnc, path }: ErrorPageProps) {
  return (
    <div className="space-y-6 animate-slide-up">
      <div className="flex flex-col items-center gap-4 py-10 text-center">
        <span className="inline-grid h-14 w-14 place-items-center rounded-full bg-red-50 dark:bg-red-900/20">
          <i
            className="bi bi-exclamation-triangle-fill text-red-500 dark:text-red-400"
            style={{ fontSize: "1.75rem" }}
            aria-hidden="true"
          ></i>
        </span>
        <div>
          <span className="mb-2 block text-xs font-semibold uppercase tracking-wider text-red-500 dark:text-red-400">
            Error
          </span>
          <h2 className="m-0 text-xl font-semibold">Something went wrong</h2>
          {message ? (
            <p className="mt-2 text-sm text-zinc-500 dark:text-zinc-400">{message}</p>
          ) : null}
        </div>
      </div>
      {stack ? (
        <details className="card p-4">
          <summary className="cursor-pointer select-none">Stack trace</summary>
          <pre className="mt-3 overflow-auto whitespace-pre-wrap">{stack}</pre>
        </details>
      ) : null}
      <div className="flex flex-wrap gap-3">
        <a className="btn" href="/">
          Home
        </a>
        {owner && repo ? (
          <a className="btn secondary" href={`/${owner}/${repo}`}>
            Repository
          </a>
        ) : null}
        {owner && repo && refEnc ? (
          <a
            className="btn secondary"
            href={`/${owner}/${repo}/tree?ref=${refEnc}${path ? `&path=${encodeURIComponent(path)}` : ""}`}
          >
            Browse
          </a>
        ) : null}
      </div>
    </div>
  );
}
