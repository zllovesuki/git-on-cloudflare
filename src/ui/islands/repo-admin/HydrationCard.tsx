import { Check, Info, Play, Search, Trash2 } from "lucide-react";
import { JsonResult } from "./JsonResult";
import type { HydrationData } from "./types";

export type HydrationCardProps = {
  hydrationRunning: boolean;
  hydrationData?: HydrationData;
  packCount: number;
  hydrationStartedAt?: string | null;
  hydrationStatus: string;
  pending: Record<string, boolean>;
  startHydration: (dryRun: boolean) => Promise<void>;
  clearHydration: () => Promise<void>;
  hydrationResult: unknown;
};

export function HydrationCard({
  hydrationRunning,
  hydrationData,
  packCount,
  hydrationStartedAt,
  hydrationStatus,
  pending,
  startHydration,
  clearHydration,
  hydrationResult,
}: HydrationCardProps) {
  return (
    <div className="card p-6">
      <h2 className="mb-4 text-xl font-semibold">Pack Hydration</h2>
      <p className="mb-4 text-zinc-600 dark:text-zinc-400">
        Hydration builds thick packs used to serve fetches correctly and fast. It is required for
        correctness, not just performance.
      </p>
      {hydrationRunning ? (
        <div className="mb-4 rounded-xl bg-accent-50 p-3 dark:bg-accent-900/20">
          <div className="text-sm">
            <strong>
              <Info
                className="mr-1 inline h-4 w-4 align-[-2px] text-accent-600 dark:text-accent-400"
                aria-hidden="true"
              />
              Running:
            </strong>{" "}
            {hydrationData?.stage || "unknown"}
            {hydrationData?.progress?.packIndex
              ? ` • Pack ${hydrationData.progress.packIndex}/${packCount}`
              : ""}
            {hydrationData?.progress?.producedBytes
              ? ` • ${(hydrationData.progress.producedBytes / 1048576).toFixed(1)} MB produced`
              : ""}
            {hydrationData?.progress?.segmentSeq
              ? ` • Segment #${hydrationData.progress.segmentSeq}`
              : ""}
            {hydrationStartedAt ? ` • Started ${hydrationStartedAt}` : ""}
          </div>
        </div>
      ) : hydrationStatus.includes("Completed") ? (
        <div className="mb-4 rounded-xl bg-green-50 p-3 text-sm dark:bg-green-900/20">
          <strong>
            <Check
              className="mr-1 inline h-4 w-4 align-[-2px] text-green-600 dark:text-green-400"
              aria-hidden="true"
            />
            Hydration complete
          </strong>{" "}
          - hydrated packs are present
        </div>
      ) : null}
      <div className="space-y-4">
        <div className="flex flex-wrap gap-3">
          <button
            className="btn"
            type="button"
            onClick={() => void startHydration(true)}
            disabled={pending["hydration-dry-run"]}
          >
            <Search className="mr-2 inline h-4 w-4 align-[-2px]" aria-hidden="true" />
            <span className="label">
              {pending["hydration-dry-run"] ? "Processing..." : "Dry Run Analysis"}
            </span>
          </button>
          <button
            className="btn"
            type="button"
            onClick={() => void startHydration(false)}
            disabled={pending["hydration-start"]}
          >
            <Play className="mr-2 inline h-4 w-4 align-[-2px]" aria-hidden="true" />
            <span className="label">
              {pending["hydration-start"] ? "Processing..." : "Start Hydration"}
            </span>
          </button>
          <button
            className="btn secondary"
            type="button"
            onClick={() => void clearHydration()}
            disabled={pending["hydration-clear"]}
          >
            <Trash2 className="mr-2 inline h-4 w-4 align-[-2px]" aria-hidden="true" />
            <span className="label">
              {pending["hydration-clear"] ? "Clearing..." : "Clear Hydration State"}
            </span>
          </button>
        </div>
        {hydrationResult ? <JsonResult data={hydrationResult} /> : null}
      </div>
    </div>
  );
}
