import { GitBranch, Shield } from "lucide-react";

import { IslandHost } from "@/ui/server/IslandHost";
import { ThemeToggleIsland } from "@/ui/islands/theme-toggle";

type HeaderProps = {
  currentView?: string;
};

const navLinkClass = (isActive: boolean): string =>
  [
    "flex items-center gap-1.5 rounded-lg px-3 py-1.5 text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent-500/50 focus-visible:ring-offset-2 focus-visible:ring-offset-white dark:focus-visible:ring-offset-zinc-950",
    isActive
      ? "bg-accent-500/10 text-accent-500 dark:text-accent-400"
      : "text-zinc-500 hover:bg-zinc-100 hover:text-zinc-800 dark:text-zinc-400 dark:hover:bg-zinc-800 dark:hover:text-zinc-200",
  ].join(" ");

export function Header({ currentView }: HeaderProps) {
  return (
    <header className="sticky top-0 z-50 w-full border-b border-zinc-200/80 bg-white/80 backdrop-blur-sm dark:border-zinc-800/60 dark:bg-zinc-950/95">
      <div className="mx-auto flex w-full max-w-7xl items-center justify-between gap-4 px-4 py-3 sm:px-6">
        <nav className="flex items-center gap-1.5" aria-label="Primary">
          <a href="/" className="flex items-center gap-3 transition-opacity hover:opacity-80">
            <span className="inline-grid h-9 w-9 place-items-center rounded-lg bg-gradient-to-br from-accent-500 to-accent-600 shadow-sm shadow-accent-500/10">
              <GitBranch className="h-5 w-5 text-white" aria-hidden="true" />
            </span>
            <span className="hidden sm:block">
              <strong className="block text-sm font-semibold text-zinc-900 dark:text-zinc-100">
                git-on-cloudflare
              </strong>
              <small className="block text-xs text-zinc-500">
                Git hosting on Cloudflare Workers
              </small>
            </span>
          </a>
          <a href="/auth" className={navLinkClass(currentView === "auth")}>
            <Shield className="h-3.5 w-3.5" aria-hidden="true" />
            Auth
          </a>
        </nav>
        <IslandHost name="theme-toggle" props={{}}>
          <ThemeToggleIsland />
        </IslandHost>
      </div>
    </header>
  );
}
