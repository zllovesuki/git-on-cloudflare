import { AlertTriangle, Check, History, PackageCheck, Rocket } from "lucide-react";
import { Badge } from "@/client/components/ui/badge";
import { Button } from "@/client/components/ui/button";
import { Card } from "@/client/components/ui/card";
import { JsonResult } from "./JsonResult";
import type {
  RepoStorageMode,
  RepoStorageModeControl,
  RepoStorageModeMutationResult,
} from "./types";
import type { RollbackCompatStatus } from "@/contracts/repoStorageMode.ts";

export type StorageModeCardProps = {
  control?: RepoStorageModeControl;
  result: RepoStorageModeMutationResult | null;
  backfillResult: unknown;
  pending: Record<string, boolean>;
  setStorageMode: (mode: RepoStorageMode) => Promise<void>;
  requestLegacyCompatBackfill: () => Promise<void>;
};

// -- Contextual guidance per mode ---------------------------------------------

function buildGuidance(control: RepoStorageModeControl): string {
  if (control.currentMode === "legacy") {
    if (control.activePackCount === 0) {
      return "No active packs yet \u2014 push data before promoting to streaming.";
    }
    return "Legacy receive/unpack/hydration paths are active. Promote to streaming when ready.";
  }

  // streaming
  const s = control.rollbackCompat.status;
  if (s === "ready") {
    return "Streaming is live. Rollback data is prepared \u2014 you can safely revert if needed.";
  }
  if (s === "queued" || s === "running") {
    return "Streaming is live. Rollback data is being prepared\u2026";
  }
  if (s === "stale") {
    return "Streaming is live. Rollback data is stale \u2014 prepare fresh data before reverting.";
  }
  if (s === "failed") {
    return "Streaming is live. Rollback data preparation failed \u2014 retry before reverting.";
  }
  return "Streaming is live. Prepare rollback compatibility data before reverting.";
}

// -- Rollback compat badge config ---------------------------------------------

const COMPAT_BADGE: Record<
  RollbackCompatStatus,
  {
    variant: "default" | "accent" | "success" | "error" | "warning";
    label: string;
    pulse?: boolean;
  }
> = {
  not_requested: { variant: "default", label: "Not requested" },
  queued: { variant: "accent", label: "Queued" },
  running: { variant: "accent", label: "Running", pulse: true },
  ready: { variant: "success", label: "Ready" },
  stale: { variant: "warning", label: "Stale" },
  failed: { variant: "error", label: "Failed" },
};

// -- Sub-components -----------------------------------------------------------

/** Two-state toggle showing the current storage mode. */
function ModeIndicator({ currentMode }: { currentMode: RepoStorageMode }) {
  const modes = [
    { mode: "legacy" as const, label: "Legacy", icon: History },
    { mode: "streaming" as const, label: "Streaming", icon: Rocket },
  ];

  return (
    <div className="flex items-center">
      {modes.map((step, i) => {
        const StepIcon = step.icon;
        const isActive = step.mode === currentMode;
        const isPast = currentMode === "streaming" && step.mode === "legacy";

        const circleBase = "flex h-8 w-8 shrink-0 items-center justify-center rounded-full";
        const circleClasses = isActive
          ? `${circleBase} bg-accent-500 text-white ring-2 ring-accent-500/30 ring-offset-2 ring-offset-white dark:ring-offset-zinc-900`
          : isPast
            ? `${circleBase} bg-accent-500/20 text-accent-600 dark:text-accent-400`
            : `${circleBase} border-2 border-zinc-300 text-zinc-400 dark:border-zinc-700 dark:text-zinc-500`;

        const labelClasses = isActive
          ? "mt-1.5 text-xs font-semibold text-accent-600 dark:text-accent-400"
          : "mt-1.5 text-xs text-zinc-500 dark:text-zinc-400";

        return (
          <div key={step.mode} className="contents">
            {i > 0 && (
              <div
                className={`h-0.5 flex-1 ${
                  isPast || isActive ? "bg-accent-500/40" : "bg-zinc-200 dark:bg-zinc-700"
                }`}
              />
            )}
            <div className="flex w-24 flex-col items-center">
              <div className={circleClasses}>
                {isPast ? (
                  <Check className="h-4 w-4" aria-hidden="true" />
                ) : (
                  <StepIcon className="h-4 w-4" aria-hidden="true" />
                )}
              </div>
              <span className={labelClasses}>{step.label}</span>
            </div>
          </div>
        );
      })}
    </div>
  );
}

function RollbackCompatBadge({ status }: { status: RollbackCompatStatus }) {
  const config = COMPAT_BADGE[status];
  return (
    <Badge variant={config.variant} className={config.pulse ? "animate-pulse" : ""}>
      {config.label}
    </Badge>
  );
}

function renderBlockers(blockers: string[]) {
  if (blockers.length === 0) return null;
  return (
    <div className="rounded-xl border border-amber-500/20 bg-amber-500/10 p-4 text-sm text-amber-700 dark:text-amber-300">
      <div className="font-semibold">
        <AlertTriangle className="mr-2 inline h-4 w-4 align-[-2px]" aria-hidden="true" />
        Mode changes are currently blocked
      </div>
      <ul className="mt-2 list-disc space-y-1 pl-5">
        {blockers.map((blocker) => (
          <li key={blocker}>{blocker}</li>
        ))}
      </ul>
    </div>
  );
}

// -- Main component -----------------------------------------------------------

export function StorageModeCard({
  control,
  result,
  backfillResult,
  pending,
  setStorageMode,
  requestLegacyCompatBackfill,
}: StorageModeCardProps) {
  if (!control) {
    return (
      <Card>
        <h2 className="mb-4 text-xl font-semibold">Storage Mode</h2>
        <p className="text-sm text-zinc-600 dark:text-zinc-400">
          Storage mode controls are unavailable because the current repository state could not be
          loaded.
        </p>
      </Card>
    );
  }

  const repoBusy = control.receiveActive || control.compactionActive;
  const hasBlockers = control.blockers.length > 0;
  const guidance = buildGuidance(control);

  // The toggle target: switch to the other mode
  const targetMode: RepoStorageMode = control.currentMode === "legacy" ? "streaming" : "legacy";
  const pendingKey = `storage-mode:${targetMode}`;
  const isPending = !!pending[pendingKey];

  // Backfill is only actionable in streaming mode when not already in progress
  const canRequestBackfill =
    control.currentMode === "streaming" &&
    control.rollbackCompat.status !== "ready" &&
    control.rollbackCompat.status !== "queued" &&
    control.rollbackCompat.status !== "running";

  return (
    <Card>
      <h2 className="mb-4 text-xl font-semibold">Storage Mode</h2>

      {/* Two-state indicator */}
      <ModeIndicator currentMode={control.currentMode} />

      {/* Contextual panel */}
      <div className="mt-4 rounded-xl border border-zinc-200 bg-zinc-50 p-4 dark:border-zinc-800 dark:bg-zinc-900/40">
        {/* Status line */}
        <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-sm text-zinc-500 dark:text-zinc-400">
          <span>
            <span className="font-mono">{control.activePackCount}</span> active packs
          </span>
          <span>&middot;</span>
          <span className="font-mono">{repoBusy ? "Busy" : "Idle"}</span>
          {/* Rollback compat badge — inline when in streaming mode */}
          {control.currentMode === "streaming" && (
            <>
              <span>&middot;</span>
              <span className="inline-flex items-center gap-1.5">
                Rollback <RollbackCompatBadge status={control.rollbackCompat.status} />
              </span>
            </>
          )}
        </div>

        {/* Contextual guidance */}
        <p className="mt-3 text-sm text-zinc-700 dark:text-zinc-300">{guidance}</p>

        {/* Rollback compat details — only in streaming mode */}
        {control.currentMode === "streaming" && (
          <div className="mt-2 font-mono text-xs text-zinc-500 dark:text-zinc-400">
            catalog v{control.rollbackCompat.currentPacksetVersion}
            {control.rollbackCompat.targetPacksetVersion !== undefined && (
              <>
                <span className="mx-1">&rarr;</span>
                prepared v{control.rollbackCompat.targetPacksetVersion}
              </>
            )}
          </div>
        )}

        {/* Blockers */}
        {hasBlockers && <div className="mt-3">{renderBlockers(control.blockers)}</div>}

        {/* Actions */}
        <div className="mt-4 flex flex-wrap items-center gap-3">
          {/* Forward: promote to streaming */}
          {control.currentMode === "legacy" && (
            <Button
              variant="primary"
              type="button"
              onClick={() => void setStorageMode("streaming")}
              disabled={hasBlockers || repoBusy || isPending}
            >
              <Rocket className="mr-2 inline h-4 w-4 align-[-2px]" aria-hidden="true" />
              <span className="label">{isPending ? "Saving..." : "Enable streaming receive"}</span>
            </Button>
          )}

          {/* Backfill action — only in streaming when rollback data is needed */}
          {canRequestBackfill && (
            <Button
              variant="secondary"
              size="sm"
              type="button"
              onClick={() => void requestLegacyCompatBackfill()}
              disabled={pending["storage-mode:backfill"]}
            >
              <PackageCheck className="mr-2 inline h-4 w-4 align-[-2px]" aria-hidden="true" />
              <span className="label">
                {pending["storage-mode:backfill"] ? "Queueing..." : "Prepare rollback data"}
              </span>
            </Button>
          )}

          {/* Rollback: revert to legacy */}
          {control.currentMode === "streaming" && (
            <Button
              variant="ghost"
              size="sm"
              type="button"
              onClick={() => void setStorageMode("legacy")}
              disabled={hasBlockers || repoBusy || isPending}
            >
              <History className="mr-1.5 inline h-4 w-4 align-[-2px]" aria-hidden="true" />
              <span className="label">{isPending ? "Saving..." : "Return to legacy"}</span>
            </Button>
          )}
        </div>
      </div>

      {result ? <JsonResult data={result} /> : null}
      {backfillResult ? <JsonResult data={backfillResult} /> : null}
    </Card>
  );
}
