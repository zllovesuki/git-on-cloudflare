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
      <div className="alert warn">
        <i
          className="bi bi-folder-fill mr-2 inline h-4 w-4 align-[-2px] text-amber-600 dark:text-amber-400"
          aria-hidden="true"
        ></i>
        Unpacking objects: {progress.processed}/{progress.total} ({progress.percent}%)
      </div>
    );
  }

  if (progress.queuedCount && progress.queuedCount > 0) {
    return (
      <div className="alert">
        <i
          className="bi bi-clock mr-2 inline h-4 w-4 align-[-2px] text-zinc-600 dark:text-zinc-300"
          aria-hidden="true"
        ></i>
        A push is queued and will start shortly.
      </div>
    );
  }

  return null;
}
