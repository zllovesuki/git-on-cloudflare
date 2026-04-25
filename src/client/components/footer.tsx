import { Heart } from "lucide-react";

export function Footer() {
  return (
    <footer className="shrink-0 border-t border-zinc-200 dark:border-zinc-800/60">
      <div className="mx-auto flex max-w-7xl flex-col items-center gap-2 px-4 py-3 text-center sm:flex-row sm:items-center sm:justify-between sm:gap-0 sm:px-6 sm:py-2 sm:text-left">
        <a
          href="https://edgenative.dev"
          target="_blank"
          rel="noopener noreferrer"
          className="m-0 flex items-center gap-1 text-xs text-zinc-500 underline decoration-zinc-300 underline-offset-2 transition-colors hover:text-accent-600 dark:decoration-zinc-700 dark:hover:text-accent-400"
        >
          Made with
          <Heart className="inline h-3 w-3 text-accent-500" aria-hidden="true" />
          on Cloudflare
        </a>
        <a
          href="https://git.edgenative.dev/rachel/git-on-cloudflare"
          target="_blank"
          rel="noopener noreferrer"
          className="text-xs text-zinc-500 underline decoration-zinc-300 underline-offset-2 transition-colors hover:text-accent-600 dark:decoration-zinc-700 dark:hover:text-accent-400"
        >
          Source code
        </a>
      </div>
    </footer>
  );
}
