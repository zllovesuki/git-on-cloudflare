import { ArrowRight } from "lucide-react";

export function HomePage() {
  return (
    <div className="space-y-12 pt-8 sm:pt-12">
      <section className="animate-slide-up opacity-0" style={{ animationDelay: "0ms" }}>
        <span className="mb-3 inline-block text-xs font-medium uppercase tracking-widest text-zinc-500">
          Git hosting on the edge
        </span>
        <h1 className="font-display text-4xl font-extrabold tracking-tight sm:text-5xl lg:text-6xl">
          git-on-cloudflare
        </h1>
        <p className="mt-4 max-w-lg text-lg text-zinc-500 dark:text-zinc-400">
          A full Git Smart HTTP v2 server running entirely on Cloudflare Workers. Clone, push, and
          browse repositories — at the edge.
        </p>
      </section>

      <section className="animate-slide-up opacity-0" style={{ animationDelay: "60ms" }}>
        <div className="overflow-hidden rounded-xl border border-zinc-200 dark:border-zinc-800/60 bg-white dark:bg-zinc-900/50 p-5 font-mono text-sm sm:text-base">
          <div className="mb-3 flex items-center gap-2">
            <span className="h-3 w-3 rounded-full bg-red-500/60" />
            <span className="h-3 w-3 rounded-full bg-amber-500/60" />
            <span className="h-3 w-3 rounded-full bg-green-500/60" />
          </div>
          <div>
            <span className="select-none text-zinc-500">$ </span>
            <span className="text-zinc-800 dark:text-zinc-100">
              git clone https://git.edgenative.dev/rachel/git-on-cloudflare
            </span>
          </div>
        </div>
      </section>

      <section className="animate-slide-up opacity-0" style={{ animationDelay: "120ms" }}>
        <dl className="grid gap-x-12 gap-y-6 sm:grid-cols-2">
          <a
            href="https://git.edgenative.dev/rachel/git-on-cloudflare"
            target="_blank"
            rel="noreferrer"
            className="group block rounded-xl border border-zinc-200 dark:border-zinc-800/60 bg-white dark:bg-zinc-900/50 p-4 no-underline transition-colors hover:border-zinc-300 dark:hover:border-zinc-700 hover:shadow-sm"
          >
            <dt className="text-sm font-semibold text-zinc-800 dark:text-zinc-200 group-hover:text-accent-500 dark:group-hover:text-accent-400">
              Browse source
            </dt>
            <dd className="mt-1 flex items-center justify-between text-sm text-zinc-500">
              <span>
                Explore the{" "}
                <span className="text-accent-500 dark:text-accent-400">git-on-cloudflare</span>{" "}
                repository — code, commits, and trees.
              </span>
              <ArrowRight
                className="ml-2 h-4 w-4 flex-shrink-0 text-zinc-400 transition-transform group-hover:translate-x-0.5"
                aria-hidden="true"
              />
            </dd>
          </a>
          <a
            href="/auth"
            className="group block rounded-xl border border-zinc-200 dark:border-zinc-800/60 bg-white dark:bg-zinc-900/50 p-4 no-underline transition-colors hover:border-zinc-300 dark:hover:border-zinc-700 hover:shadow-sm"
          >
            <dt className="text-sm font-semibold text-zinc-800 dark:text-zinc-200 group-hover:text-accent-500 dark:group-hover:text-accent-400">
              Manage auth
            </dt>
            <dd className="mt-1 flex items-center justify-between text-sm text-zinc-500">
              <span>
                Configure <span className="text-accent-500 dark:text-accent-400">owners</span> and{" "}
                <span className="text-accent-500 dark:text-accent-400">access tokens</span> for push
                access.
              </span>
              <ArrowRight
                className="ml-2 h-4 w-4 flex-shrink-0 text-zinc-400 transition-transform group-hover:translate-x-0.5"
                aria-hidden="true"
              />
            </dd>
          </a>
        </dl>
      </section>
    </div>
  );
}
