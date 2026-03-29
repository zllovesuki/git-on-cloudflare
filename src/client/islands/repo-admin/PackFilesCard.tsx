import { Check, Trash2, X } from "lucide-react";
import { Card } from "@/client/components/ui/card";
import { formatSampleBytes, shortValue } from "./format";
import type { PackStat } from "./types";

export type PackFilesCardProps = {
  packCount: number;
  packStats: PackStat[];
  pending: Record<string, boolean>;
  removePack: (packName: string) => Promise<void>;
};

function packKindLabel(kind: PackStat["kind"]): string {
  switch (kind) {
    case "compact":
      return "compact";
    case "receive":
      return "receive";
    default:
      return "legacy";
  }
}

export function PackFilesCard({ packCount, packStats, pending, removePack }: PackFilesCardProps) {
  if (packStats.length === 0 && packCount === 0) {
    return null;
  }

  if (packStats.length === 0 && packCount > 0) {
    return (
      <Card>
        <h2 className="mb-4 text-xl font-semibold">Pack Catalog</h2>
        <p className="text-zinc-600 dark:text-zinc-400">
          {packCount} pack rows exist but detailed catalog data was not loaded.
        </p>
      </Card>
    );
  }

  return (
    <Card>
      <h2 className="mb-4 text-xl font-semibold">
        Pack Catalog ({packStats.length} row{packStats.length === 1 ? "" : "s"})
      </h2>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-zinc-200 dark:border-zinc-800">
              <th className="py-2 text-left">Pack Name</th>
              <th className="py-2 text-left">State</th>
              <th className="py-2 text-left">Kind</th>
              <th className="py-2 text-right">Tier</th>
              <th className="py-2 text-right">Objects</th>
              <th className="py-2 text-right">Pack Size</th>
              <th className="py-2 text-right">Index Size</th>
              <th className="py-2 text-center">Index</th>
              <th className="py-2 text-center">Actions</th>
            </tr>
          </thead>
          <tbody>
            {packStats.map((packStat) => {
              const packName = String(packStat.key).split("/").pop() || packStat.key;
              const canDelete = packStat.state === "superseded";

              return (
                <tr key={packStat.key} className="border-b border-zinc-100 dark:border-zinc-800">
                  <td className="py-2 font-mono text-xs">
                    <span title={packStat.key}>{shortValue(packName, 35)}</span>
                  </td>
                  <td className="py-2">
                    <span
                      className={`rounded px-2 py-1 text-xs ${
                        packStat.state === "active"
                          ? "bg-green-100 dark:bg-green-900"
                          : "bg-zinc-200 dark:bg-zinc-800"
                      }`}
                    >
                      {packStat.state}
                    </span>
                  </td>
                  <td className="py-2">{packKindLabel(packStat.kind)}</td>
                  <td className="py-2 text-right font-mono text-xs">{packStat.tier}</td>
                  <td className="py-2 text-right font-mono text-xs">{packStat.objectCount}</td>
                  <td className="py-2 text-right font-mono text-xs">
                    {formatSampleBytes(packStat.packSize)}
                  </td>
                  <td className="py-2 text-right font-mono text-xs">
                    {formatSampleBytes(packStat.indexSize)}
                  </td>
                  <td className="py-2 text-center">
                    {packStat.hasIndex ? (
                      <Check
                        className="inline h-4 w-4 text-green-600 dark:text-green-400"
                        aria-hidden="true"
                      />
                    ) : (
                      <X
                        className="inline h-4 w-4 text-red-600 dark:text-red-400"
                        aria-hidden="true"
                      />
                    )}
                  </td>
                  <td className="py-2 text-center">
                    <button
                      type="button"
                      className="rounded p-1 text-red-600 transition-colors hover:bg-red-100 hover:text-red-800 disabled:cursor-not-allowed disabled:opacity-40 dark:text-red-400 dark:hover:bg-red-900/20 dark:hover:text-red-300"
                      title={
                        canDelete
                          ? "Delete this superseded pack"
                          : "Active packs cannot be deleted during normal operation"
                      }
                      aria-label="Delete pack"
                      onClick={() => void removePack(packName)}
                      disabled={!canDelete || pending[`remove-pack:${packName}`]}
                    >
                      <Trash2 className="h-4 w-4" aria-hidden="true" />
                    </button>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </Card>
  );
}
