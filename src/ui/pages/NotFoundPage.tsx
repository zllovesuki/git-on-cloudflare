export function NotFoundPage() {
  return (
    <div className="flex flex-col items-center justify-center gap-4 py-16 text-center animate-slide-up">
      <span className="inline-grid h-16 w-16 place-items-center rounded-full bg-zinc-100 dark:bg-zinc-800/60">
        <i
          className="bi bi-question-circle text-zinc-400 dark:text-zinc-600"
          style={{ fontSize: "2rem" }}
          aria-hidden="true"
        ></i>
      </span>
      <div>
        <span className="mb-2 block text-xs font-semibold uppercase tracking-wider text-zinc-500 dark:text-zinc-400">
          404
        </span>
        <h1 className="mb-2 text-xl font-semibold">Not Found</h1>
        <p className="text-sm text-zinc-500 dark:text-zinc-400">
          The page you are looking for doesn't exist.
        </p>
      </div>
      <a className="btn" href="/">
        Go home
      </a>
    </div>
  );
}
