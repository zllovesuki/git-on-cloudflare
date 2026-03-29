import { Check, Clock, Download, Info, Play, Search, Trash2 } from "lucide-react";
import { Button } from "@/client/components/ui/button";
import { Card } from "@/client/components/ui/card";
import { JsonResult } from "./JsonResult";
import type { AdminState, CompactionData } from "./types";

export type HydrationCardProps = {
  compactionData?: CompactionData;
  compactionStartedAt?: string | null;
  compactionStatus: string;
  state: AdminState;
  pending: Record<string, boolean>;
  startCompaction: (dryRun: boolean) => Promise<void>;
  clearCompaction: () => Promise<void>;
  compactionResult: unknown;
};

export function HydrationCard({
  compactionData,
  compactionStartedAt,
  compactionStatus,
  state,
  pending,
  startCompaction,
  clearCompaction,
  compactionResult,
}: HydrationCardProps) {
  const receiveStartedAt = state.receiveLease
    ? new Date(state.receiveLease.createdAt).toLocaleString()
    : null;

  return (
    <Card>
      <h2 className="mb-4 text-xl font-semibold">Repository Activity</h2>
      <p className="mb-4 text-zinc-600 dark:text-zinc-400">
        Receive and compaction leases are the live repository signals on this page. The controls
        below preview or record compaction requests; background compaction is not wired here yet.
      </p>

      {state.receiveLease ? (
        <div className="mb-4 rounded-xl bg-amber-50 p-3 dark:bg-amber-900/20">
          <div className="text-sm">
            <strong>
              <Download
                className="mr-1 inline h-4 w-4 align-[-2px] text-amber-600 dark:text-amber-400"
                aria-hidden="true"
              />
              Receiving:
            </strong>{" "}
            receive lease is active
            {receiveStartedAt ? ` • Started ${receiveStartedAt}` : ""}
          </div>
        </div>
      ) : compactionData?.running ? (
        <div className="mb-4 rounded-xl bg-accent-50 p-3 dark:bg-accent-900/20">
          <div className="text-sm">
            <strong>
              <Info
                className="mr-1 inline h-4 w-4 align-[-2px] text-accent-600 dark:text-accent-400"
                aria-hidden="true"
              />
              Running:
            </strong>{" "}
            compaction lease is active
            {compactionStartedAt ? ` • Started ${compactionStartedAt}` : ""}
          </div>
        </div>
      ) : compactionData?.queued ? (
        <div className="mb-4 rounded-xl bg-accent-50 p-3 text-sm dark:bg-accent-900/20">
          <strong>
            <Clock
              className="mr-1 inline h-4 w-4 align-[-2px] text-accent-600 dark:text-accent-400"
              aria-hidden="true"
            />
            Queued:
          </strong>{" "}
          compaction request is recorded in repo metadata
        </div>
      ) : (
        <div className="mb-4 rounded-xl bg-green-50 p-3 text-sm dark:bg-green-900/20">
          <strong>
            <Check
              className="mr-1 inline h-4 w-4 align-[-2px] text-green-600 dark:text-green-400"
              aria-hidden="true"
            />
            Idle:
          </strong>{" "}
          no compaction lease is currently active
        </div>
      )}

      <div className="grid grid-cols-1 gap-3 text-sm md:grid-cols-2">
        <div className="rounded-xl border border-zinc-200 p-3 dark:border-zinc-800">
          <div className="mb-1 text-xs uppercase tracking-wide text-zinc-500 dark:text-zinc-400">
            Receive Lease
          </div>
          <div className="font-mono">
            {state.receiveLease
              ? `active until ${new Date(state.receiveLease.expiresAt).toLocaleString()}`
              : "idle"}
          </div>
        </div>
        <div className="rounded-xl border border-zinc-200 p-3 dark:border-zinc-800">
          <div className="mb-1 text-xs uppercase tracking-wide text-zinc-500 dark:text-zinc-400">
            Compaction
          </div>
          <div className="font-mono">{compactionStatus}</div>
          {compactionData?.wantedAt ? (
            <div className="mt-1 text-xs text-zinc-500 dark:text-zinc-400">
              requested {new Date(compactionData.wantedAt).toLocaleString()}
            </div>
          ) : null}
        </div>
      </div>

      <div className="mt-4 space-y-4">
        <div className="rounded-xl border border-zinc-200 bg-zinc-50 p-4 text-sm text-zinc-600 dark:border-zinc-800 dark:bg-zinc-900/40 dark:text-zinc-300">
          The compaction endpoints are compatibility controls during the streaming-push rollout.
          Pack-catalog state and active leases are the source of truth for repository status until
          background compaction work is wired in.
        </div>
        <div className="flex flex-wrap gap-3">
          <Button
            type="button"
            onClick={() => void startCompaction(true)}
            disabled={pending["compaction-dry-run"]}
          >
            <Search className="mr-2 inline h-4 w-4 align-[-2px]" aria-hidden="true" />
            <span className="label">
              {pending["compaction-dry-run"] ? "Processing..." : "Preview Compaction"}
            </span>
          </Button>
          <Button
            type="button"
            onClick={() => void startCompaction(false)}
            disabled={pending["compaction-start"]}
          >
            <Play className="mr-2 inline h-4 w-4 align-[-2px]" aria-hidden="true" />
            <span className="label">
              {pending["compaction-start"] ? "Processing..." : "Request Compaction"}
            </span>
          </Button>
          <Button
            variant="secondary"
            type="button"
            onClick={() => void clearCompaction()}
            disabled={pending["compaction-clear"]}
          >
            <Trash2 className="mr-2 inline h-4 w-4 align-[-2px]" aria-hidden="true" />
            <span className="label">
              {pending["compaction-clear"] ? "Clearing..." : "Clear Compaction Request"}
            </span>
          </Button>
        </div>
        {compactionResult ? <JsonResult data={compactionResult} /> : null}
      </div>
    </Card>
  );
}
