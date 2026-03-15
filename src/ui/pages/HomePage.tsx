export function HomePage() {
  return (
    <div className="animate-slide-up space-y-6">
      <section className="rounded-2xl border border-indigo-500/20 bg-white p-6 dark:bg-zinc-900/80">
        <span className="mb-2 inline-block text-xs font-semibold uppercase tracking-wider text-indigo-500 dark:text-indigo-400">
          Welcome
        </span>
        <h2 className="m-0 text-lg font-semibold">git-on-cloudflare</h2>
        <p className="muted mt-1 mb-0">
          A Git Smart HTTP v2 server running entirely on Cloudflare Workers.
        </p>
      </section>
      <div className="space-y-4">
        <div className="card p-6">
          <span className="mb-2 inline-block text-xs font-semibold uppercase tracking-wider text-indigo-500 dark:text-indigo-400">
            Quick Start
          </span>
          <h3 className="flex items-center gap-2">
            <i
              className="bi bi-rocket-takeoff inline h-4 w-4 text-indigo-500"
              aria-hidden="true"
            ></i>
            Get Started
          </h3>
          <ul className="list space-y-2">
            <li>
              <a
                href="https://git-on-cloudflare.com/rachel/git-on-cloudflare"
                target="_blank"
                rel="noreferrer"
              >
                Quickstart
              </a>
            </li>
            <li>
              <a href="/auth">Manage Auth</a>
            </li>
          </ul>
        </div>
      </div>
    </div>
  );
}
