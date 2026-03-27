import { useState } from "react";
import { Clipboard } from "lucide-react";
import { shortRefName } from "@/git/refDisplay.ts";
import { Card } from "@/ui/components/ui/card";
import { formatSampleBytes, shortValue } from "./format";
import type { AdminState, PackStat } from "./types";

export type RepoOverviewCardProps = {
  storageSize: string;
  packCount: number;
  hydrationPackCount: number;
  hydrationStatus: string;
  nextMaintenanceIn?: string;
  nextMaintenanceAt?: string;
  state: AdminState;
  head?: { target?: string; unborn?: boolean };
  branchCount: number;
  tagCount: number;
};

export function RepoOverviewCard({
  storageSize,
  packCount,
  hydrationPackCount,
  hydrationStatus,
  nextMaintenanceIn,
  nextMaintenanceAt,
  state,
  head,
  branchCount,
  tagCount,
}: RepoOverviewCardProps) {
  const [copiedDoId, setCopiedDoId] = useState(false);

  async function copyDoId(doId: string) {
    try {
      await navigator.clipboard.writeText(doId);
      setCopiedDoId(true);
      window.setTimeout(() => setCopiedDoId(false), 1200);
    } catch (error) {
      window.alert(`Failed to copy DO ID: ${String(error)}`);
    }
  }

  return (
    <Card>
      <h2 className="mb-4 text-xl font-semibold">Repository Overview</h2>
      <div className="grid grid-cols-2 gap-x-6 gap-y-3 md:grid-cols-3 lg:grid-cols-4">
        <div>
          <div className="mb-1 text-xs uppercase tracking-wide text-zinc-500 dark:text-zinc-400">
            R2 Packs Storage
          </div>
          <div className="font-mono text-sm">{storageSize || "0 bytes"}</div>
        </div>
        <div>
          <div className="mb-1 text-xs uppercase tracking-wide text-zinc-500 dark:text-zinc-400">
            Packs
          </div>
          <div className="font-mono text-sm">
            {packCount}
            {hydrationPackCount > 0 ? (
              <span className="ml-1 text-xs text-zinc-500">
                ({hydrationPackCount} hydration packs)
              </span>
            ) : null}
          </div>
        </div>
        <div>
          <div className="mb-1 text-xs uppercase tracking-wide text-zinc-500 dark:text-zinc-400">
            Next Maintenance
          </div>
          <div className="font-mono text-sm">
            {nextMaintenanceIn ? (
              <span title={nextMaintenanceAt || ""}>{nextMaintenanceIn}</span>
            ) : (
              <span className="text-zinc-500">n/a</span>
            )}
          </div>
        </div>
        {state?.meta?.doId ? (
          <div>
            <div className="mb-1 text-xs uppercase tracking-wide text-zinc-500 dark:text-zinc-400">
              Durable Object ID
            </div>
            <div className="relative font-mono text-sm">
              <span title={state.meta.doId}>{shortValue(String(state.meta.doId), 12)}</span>
              <button
                type="button"
                className="absolute -top-0.5 ml-2 rounded border border-zinc-300 p-1 transition-colors hover:bg-zinc-100 dark:border-zinc-700 dark:hover:bg-zinc-800"
                title="Copy DO ID"
                aria-label="Copy DO ID"
                style={{ marginLeft: "0.5rem", top: "-2px" }}
                onClick={() => void copyDoId(String(state.meta?.doId || ""))}
              >
                <Clipboard className="block h-4 w-4" aria-hidden="true" />
              </button>
              <span className="invisible">{copiedDoId ? "copied" : "___copy"}</span>
            </div>
          </div>
        ) : null}
        {state.looseR2SampleBytes ? (
          <div>
            <div className="mb-1 text-xs uppercase tracking-wide text-zinc-500 dark:text-zinc-400">
              R2 Loose (sample)
            </div>
            <div className="font-mono text-sm">
              {formatSampleBytes(state.looseR2SampleBytes)}
              <span className="ml-1 text-xs text-zinc-500">
                ({state.looseR2SampleCount || 0} objs{state.looseR2Truncated ? ", truncated" : ""})
              </span>
            </div>
          </div>
        ) : null}
        {state.dbSizeBytes ? (
          <div>
            <div className="mb-1 text-xs uppercase tracking-wide text-zinc-500 dark:text-zinc-400">
              Durable Object Storage
            </div>
            <div className="font-mono text-sm">{formatSampleBytes(state.dbSizeBytes)}</div>
          </div>
        ) : null}
        <div>
          <div className="mb-1 text-xs uppercase tracking-wide text-zinc-500 dark:text-zinc-400">
            Unpacking
          </div>
          <div className="font-mono text-sm text-amber-600 dark:text-amber-400">
            {state.unpackWork
              ? `${state.unpackWork.processedCount || 0}/${state.unpackWork.totalCount || 0}`
              : state.unpackNext
                ? "Scheduled"
                : "Idle"}
          </div>
        </div>
        <div>
          <div className="mb-1 text-xs uppercase tracking-wide text-zinc-500 dark:text-zinc-400">
            Hydration
          </div>
          <div className="font-mono text-sm text-accent-600 dark:text-accent-400">
            {hydrationStatus}
          </div>
        </div>
        <div>
          <div className="mb-1 text-xs uppercase tracking-wide text-zinc-500 dark:text-zinc-400">
            HEAD
          </div>
          <div className="font-mono text-sm">
            {head?.target ? shortRefName(String(head.target)) : head?.unborn ? "unborn" : "unknown"}
          </div>
        </div>
        <div>
          <div className="mb-1 text-xs uppercase tracking-wide text-zinc-500 dark:text-zinc-400">
            Branches/Tags
          </div>
          <div className="font-mono text-sm">
            {branchCount}/{tagCount}
          </div>
        </div>
      </div>
    </Card>
  );
}
