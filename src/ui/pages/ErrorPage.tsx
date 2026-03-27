import { TriangleAlert } from "lucide-react";
import { Button } from "@/ui/components/ui/button";

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
          <TriangleAlert className="h-7 w-7 text-red-500 dark:text-red-400" aria-hidden="true" />
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
        <details className="rounded-2xl border border-zinc-200 dark:border-zinc-800/60 bg-white dark:bg-zinc-900/50 p-4">
          <summary className="cursor-pointer select-none">Stack trace</summary>
          <pre className="mt-3 overflow-auto whitespace-pre-wrap">{stack}</pre>
        </details>
      ) : null}
      <div className="flex flex-wrap gap-3">
        <Button href="/">Home</Button>
        {owner && repo ? (
          <Button variant="secondary" href={`/${owner}/${repo}`}>
            Repository
          </Button>
        ) : null}
        {owner && repo && refEnc ? (
          <Button
            variant="secondary"
            href={`/${owner}/${repo}/tree?ref=${refEnc}${path ? `&path=${encodeURIComponent(path)}` : ""}`}
          >
            Browse
          </Button>
        ) : null}
      </div>
    </div>
  );
}
