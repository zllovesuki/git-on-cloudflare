import type { ReactNode } from "react";

import { IslandHost } from "@/ui/server/IslandHost";
import { ThemeToggleIsland } from "@/ui/islands/theme-toggle";

type AppLayoutProps = {
  children: ReactNode;
};

export function AppLayout({ children }: AppLayoutProps) {
  return (
    <>
      <div className="ambient-glow" aria-hidden="true"></div>
      <div className="relative z-10 min-h-screen flex flex-col">
        <header className="sticky top-0 z-50 w-full border-b border-zinc-200/80 bg-white/80 backdrop-blur-xl dark:border-zinc-800/60 dark:bg-zinc-950/80">
          <div className="container flex items-center justify-between gap-4 py-3">
            <nav className="flex items-center gap-1" aria-label="Primary">
              <a href="/" className="flex items-center gap-2.5 transition-opacity hover:opacity-80">
                <span className="inline-grid h-8 w-8 place-items-center rounded-lg bg-indigo-600">
                  <i
                    className="bi bi-git text-white"
                    style={{ fontSize: "1rem" }}
                    aria-hidden="true"
                  ></i>
                </span>
                <span>
                  <strong className="block text-sm font-semibold text-zinc-900 dark:text-zinc-100">
                    git-on-cloudflare
                  </strong>
                  <small className="hidden text-xs text-zinc-500 dark:text-zinc-400 sm:block">
                    Git hosting on Cloudflare Workers
                  </small>
                </span>
              </a>
              <a
                href="/auth"
                className="ml-3 flex items-center gap-1.5 rounded-lg px-3 py-1.5 text-sm font-medium text-zinc-500 transition-colors hover:bg-zinc-100 hover:text-zinc-800 dark:text-zinc-400 dark:hover:bg-zinc-800 dark:hover:text-zinc-200"
              >
                <i className="bi bi-shield-lock w-3.5 h-3.5" aria-hidden="true"></i>
                Auth
              </a>
            </nav>
            <IslandHost name="theme-toggle" props={{}}>
              <ThemeToggleIsland />
            </IslandHost>
          </div>
        </header>
        <main className="container flex-1 py-6 animate-fade-in">{children}</main>
        <footer className="mt-auto border-t border-zinc-200/80 py-6 text-center text-xs text-zinc-500 dark:border-zinc-800/60 dark:text-zinc-500">
          <p className="mb-0 flex items-center justify-center gap-1">
            Made with
            <i className="bi bi-heart inline h-3 w-3 text-indigo-500" aria-hidden="true"></i>
            on Cloudflare
          </p>
          <div className="mt-1 flex justify-center gap-4">
            <a
              href="https://github.com/zllovesuki/git-on-cloudflare"
              className="text-zinc-500 underline decoration-zinc-300 underline-offset-2 transition-colors hover:text-indigo-500 dark:decoration-zinc-700 dark:hover:text-indigo-400"
            >
              GitHub
            </a>
            <a
              href="https://git-on-cloudflare.com/rachel/git-on-cloudflare"
              className="text-zinc-500 underline decoration-zinc-300 underline-offset-2 transition-colors hover:text-indigo-500 dark:decoration-zinc-700 dark:hover:text-indigo-400"
            >
              git-on-cloudflare
            </a>
          </div>
          <p className="mt-2">
            Part of
            <a
              href="https://devbin.tools"
              className="ml-1 text-zinc-500 underline decoration-zinc-300 underline-offset-2 transition-colors hover:text-indigo-500 dark:decoration-zinc-700 dark:hover:text-indigo-400"
            >
              devbin.tools
            </a>
          </p>
        </footer>
      </div>
    </>
  );
}
