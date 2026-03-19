import { Heart } from "lucide-react";

export function Footer() {
  return (
    <footer className="mt-auto border-t border-zinc-200/80 py-6 text-center text-xs text-zinc-500 dark:border-zinc-800/60 dark:text-zinc-500">
      <p className="mb-0 flex items-center justify-center gap-1">
        Made with
        <Heart className="inline h-3 w-3 text-accent-500" aria-hidden="true" />
        on Cloudflare
      </p>
      <div className="mt-1 flex justify-center gap-4">
        <a
          href="https://github.com/zllovesuki/git-on-cloudflare"
          className="text-zinc-500 underline decoration-zinc-300 underline-offset-2 transition-colors hover:text-accent-500 dark:decoration-zinc-700 dark:hover:text-accent-400"
        >
          GitHub
        </a>
        <a
          href="https://git-on-cloudflare.com/rachel/git-on-cloudflare"
          className="text-zinc-500 underline decoration-zinc-300 underline-offset-2 transition-colors hover:text-accent-500 dark:decoration-zinc-700 dark:hover:text-accent-400"
        >
          git-on-cloudflare
        </a>
      </div>
      <p className="mt-2">
        Part of
        <a
          href="https://devbin.tools"
          className="ml-1 text-zinc-500 underline decoration-zinc-300 underline-offset-2 transition-colors hover:text-accent-500 dark:decoration-zinc-700 dark:hover:text-accent-400"
        >
          devbin.tools
        </a>
      </p>
    </footer>
  );
}
