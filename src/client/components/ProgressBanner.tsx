import { Download, Layers3 } from "lucide-react";

export type Progress = {
  state: "receiving" | "compacting";
  startedAt?: number;
  expiresAt?: number;
} | null;

type ProgressBannerProps = {
  progress?: Progress;
};

export function ProgressBanner({ progress }: ProgressBannerProps) {
  if (!progress) {
    return null;
  }

  if (progress.state === "receiving") {
    return (
      <div className="mb-6 rounded-xl border border-amber-500/20 bg-amber-500/10 p-4 text-sm text-amber-700 dark:text-amber-300">
        <Download
          className="mr-2 inline h-4 w-4 align-[-2px] text-amber-600 dark:text-amber-400"
          aria-hidden="true"
        />
        Receiving push...
      </div>
    );
  }

  return (
    <div className="mb-6 rounded-xl border border-accent-500/20 bg-accent-500/10 p-4 text-sm text-accent-700 dark:text-accent-400">
      <Layers3
        className="mr-2 inline h-4 w-4 align-[-2px] text-zinc-600 dark:text-zinc-300"
        aria-hidden="true"
      />
      Compacting packs...
    </div>
  );
}
