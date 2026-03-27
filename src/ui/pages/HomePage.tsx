import { GitBranch, Shield } from "lucide-react";

export function HomePage() {
  return (
    <div className="space-y-10">
      <section className="animate-slide-up opacity-0 pt-6" style={{ animationDelay: "0ms" }}>
        <span className="mb-3 inline-block text-xs font-medium uppercase tracking-widest text-zinc-500">
          Git hosting on the edge
        </span>
        <h1 className="text-3xl font-extrabold tracking-tight sm:text-4xl">git-on-cloudflare</h1>
        <p className="mt-3 max-w-xl text-lg text-zinc-500 dark:text-zinc-400">
          A full Git Smart HTTP v2 server running entirely on Cloudflare Workers. Clone, push, and
          browse repositories at the edge.
        </p>
      </section>

      <section className="animate-slide-up opacity-0" style={{ animationDelay: "60ms" }}>
        <div className="rounded-xl border border-zinc-200 dark:border-zinc-800/60 bg-white dark:bg-zinc-900/50 p-4 font-mono text-sm">
          <span className="select-none text-zinc-500">$ </span>
          <span className="text-zinc-800 dark:text-zinc-100">
            git clone https://git-on-cloudflare.com/
          </span>
          <span className="text-accent-400">&lt;owner&gt;</span>
          <span className="text-zinc-800 dark:text-zinc-100">/</span>
          <span className="text-accent-400">&lt;repo&gt;</span>
        </div>
      </section>

      <section className="animate-slide-up opacity-0" style={{ animationDelay: "120ms" }}>
        <dl className="grid gap-4 sm:grid-cols-2">
          <a
            href="https://git-on-cloudflare.com/rachel/git-on-cloudflare"
            target="_blank"
            rel="noreferrer"
            className="group flex items-start gap-3 rounded-xl p-4 no-underline transition-colors hover:bg-zinc-100 dark:hover:bg-zinc-900/60"
          >
            <span className="mt-0.5 inline-grid h-8 w-8 shrink-0 place-items-center rounded-lg bg-accent-500/10 text-accent-400">
              <GitBranch className="h-4 w-4" aria-hidden="true" />
            </span>
            <div>
              <dt className="text-sm font-semibold text-zinc-800 dark:text-zinc-200 group-hover:text-accent-400">
                Browse source
              </dt>
              <dd className="mt-0.5 text-sm text-zinc-500">
                Explore the git-on-cloudflare repository
              </dd>
            </div>
          </a>
          <a
            href="/auth"
            className="group flex items-start gap-3 rounded-xl p-4 no-underline transition-colors hover:bg-zinc-100 dark:hover:bg-zinc-900/60"
          >
            <span className="mt-0.5 inline-grid h-8 w-8 shrink-0 place-items-center rounded-lg bg-accent-500/10 text-accent-400">
              <Shield className="h-4 w-4" aria-hidden="true" />
            </span>
            <div>
              <dt className="text-sm font-semibold text-zinc-800 dark:text-zinc-200 group-hover:text-accent-400">
                Manage auth
              </dt>
              <dd className="mt-0.5 text-sm text-zinc-500">Configure owners and access tokens</dd>
            </div>
          </a>
        </dl>
      </section>
    </div>
  );
}
