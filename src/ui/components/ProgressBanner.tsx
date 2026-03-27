import { Clock, Folder } from "lucide-react";

export type Progress = {
  unpacking?: boolean;
  total?: number;
  processed?: number;
  percent?: number;
  queuedCount?: number;
} | null;

type ProgressBannerProps = {
  progress?: Progress;
};

export function ProgressBanner({ progress }: ProgressBannerProps) {
  if (!progress) {
    return null;
  }

  if (progress.unpacking && progress.total) {
    return (
      <div className="rounded-xl border border-amber-500/20 bg-amber-500/10 p-4 text-sm text-amber-700 dark:text-amber-300">
        <Folder
          className="mr-2 inline h-4 w-4 align-[-2px] text-amber-600 dark:text-amber-400"
          aria-hidden="true"
        />
        Unpacking objects: {progress.processed}/{progress.total} ({progress.percent}%)
      </div>
    );
  }

  if (progress.queuedCount && progress.queuedCount > 0) {
    return (
      <div className="rounded-xl border border-accent-500/20 bg-accent-500/10 p-4 text-sm text-accent-700 dark:text-accent-400">
        <Clock
          className="mr-2 inline h-4 w-4 align-[-2px] text-zinc-600 dark:text-zinc-300"
          aria-hidden="true"
        />
        A push is queued and will start shortly.
      </div>
    );
  }

  return null;
}
