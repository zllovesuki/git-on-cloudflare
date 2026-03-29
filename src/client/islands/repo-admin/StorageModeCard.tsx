import { AlertTriangle, Eye, History, ShieldCheck } from "lucide-react";
import { Button } from "@/client/components/ui/button";
import { Card } from "@/client/components/ui/card";
import { JsonResult } from "./JsonResult";
import type { RepoStorageModeControl, RepoStorageModeMutationResult } from "./types";

export type StorageModeCardProps = {
  control?: RepoStorageModeControl;
  result: RepoStorageModeMutationResult | null;
  pending: Record<string, boolean>;
  setStorageMode: (mode: "legacy" | "shadow-read") => Promise<void>;
};

function renderBlockers(blockers: string[]) {
  if (blockers.length === 0) return null;
  return (
    <div className="mt-4 rounded-xl border border-amber-500/20 bg-amber-500/10 p-4 text-sm text-amber-700 dark:text-amber-300">
      <div className="font-semibold">
        <AlertTriangle className="mr-2 inline h-4 w-4 align-[-2px]" aria-hidden="true" />
        Validation changes are currently blocked
      </div>
      <ul className="mt-2 list-disc space-y-1 pl-5">
        {blockers.map((blocker) => (
          <li key={blocker}>{blocker}</li>
        ))}
      </ul>
    </div>
  );
}

export function StorageModeCard({
  control,
  result,
  pending,
  setStorageMode,
}: StorageModeCardProps) {
  if (!control) {
    return (
      <Card>
        <h2 className="mb-4 text-xl font-semibold">Packed Read Validation</h2>
        <p className="text-sm text-zinc-600 dark:text-zinc-400">
          Validation controls are unavailable because the current repository state could not be
          loaded.
        </p>
      </Card>
    );
  }

  const modeLabel =
    control.currentMode === "shadow-read"
      ? "shadow-read (validation on)"
      : "legacy (validation off)";
  const canEnableShadowRead =
    control.status === "ok" &&
    control.currentMode === "legacy" &&
    !control.receiveActive &&
    !control.compactionActive &&
    control.activePackCount > 0;
  const canReturnToLegacy =
    control.status === "ok" &&
    control.currentMode === "shadow-read" &&
    !control.receiveActive &&
    !control.compactionActive;

  return (
    <Card>
      <h2 className="mb-4 text-xl font-semibold">Packed Read Validation</h2>
      <p className="mb-4 text-sm text-zinc-600 dark:text-zinc-400">
        Fetch and UI reads stay on the same pack-first Worker path in both modes. This control only
        enables or disables packed-vs-compatibility validation during rollout.
      </p>

      <div className="grid grid-cols-1 gap-3 text-sm md:grid-cols-3">
        <div className="rounded-xl border border-zinc-200 p-3 dark:border-zinc-800">
          <div className="mb-1 text-xs uppercase tracking-wide text-zinc-500 dark:text-zinc-400">
            Current Mode
          </div>
          <div className="font-mono">{modeLabel}</div>
        </div>
        <div className="rounded-xl border border-zinc-200 p-3 dark:border-zinc-800">
          <div className="mb-1 text-xs uppercase tracking-wide text-zinc-500 dark:text-zinc-400">
            Active Packs
          </div>
          <div className="font-mono">{control.activePackCount}</div>
        </div>
        <div className="rounded-xl border border-zinc-200 p-3 dark:border-zinc-800">
          <div className="mb-1 text-xs uppercase tracking-wide text-zinc-500 dark:text-zinc-400">
            Validation Toggle
          </div>
          <div className="font-mono">{control.canChange ? "Available" : "Blocked"}</div>
        </div>
      </div>

      {control.message ? (
        <div className="mt-4 rounded-xl border border-zinc-200 bg-zinc-50 p-4 text-sm text-zinc-700 dark:border-zinc-800 dark:bg-zinc-900/40 dark:text-zinc-300">
          {control.message}
        </div>
      ) : null}

      {control.status === "unsupported_current_mode" ? (
        <div className="mt-4 rounded-xl border border-red-500/20 bg-red-500/10 p-4 text-sm text-red-700 dark:text-red-300">
          <AlertTriangle className="mr-2 inline h-4 w-4 align-[-2px]" aria-hidden="true" />
          This repository is already using a mode that is outside this validation-only admin
          control.
        </div>
      ) : (
        <div className="mt-4 flex flex-wrap gap-3">
          <Button
            type="button"
            onClick={() => void setStorageMode("shadow-read")}
            disabled={!canEnableShadowRead || pending["storage-mode:shadow-read"]}
          >
            <ShieldCheck className="mr-2 inline h-4 w-4 align-[-2px]" aria-hidden="true" />
            <span className="label">
              {pending["storage-mode:shadow-read"] ? "Saving..." : "Enable Packed-Read Validation"}
            </span>
          </Button>
          <Button
            variant="secondary"
            type="button"
            onClick={() => void setStorageMode("legacy")}
            disabled={!canReturnToLegacy || pending["storage-mode:legacy"]}
          >
            <History className="mr-2 inline h-4 w-4 align-[-2px]" aria-hidden="true" />
            <span className="label">
              {pending["storage-mode:legacy"] ? "Saving..." : "Disable Packed-Read Validation"}
            </span>
          </Button>
        </div>
      )}

      <div className="mt-4 grid grid-cols-1 gap-3 text-sm md:grid-cols-2">
        <div className="rounded-xl border border-zinc-200 p-3 dark:border-zinc-800">
          <div className="mb-1 font-medium">
            <History className="mr-2 inline h-4 w-4 align-[-2px]" aria-hidden="true" />
            Validation off
          </div>
          <p className="text-zinc-600 dark:text-zinc-400">
            The pack-first read path stays active, and compatibility validation is skipped.
          </p>
        </div>
        <div className="rounded-xl border border-zinc-200 p-3 dark:border-zinc-800">
          <div className="mb-1 font-medium">
            <Eye className="mr-2 inline h-4 w-4 align-[-2px]" aria-hidden="true" />
            Validation on
          </div>
          <p className="text-zinc-600 dark:text-zinc-400">
            The same pack-first reads run, then compare their results against compatibility reads.
          </p>
        </div>
      </div>

      {renderBlockers(control.blockers)}
      {result ? <JsonResult data={result} /> : null}
    </Card>
  );
}
