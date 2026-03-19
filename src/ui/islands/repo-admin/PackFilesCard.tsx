import { Check, Trash2, X } from "lucide-react";
import { formatSampleBytes, shortValue } from "./format";
import type { PackStat } from "./types";

export type PackFilesCardProps = {
  packCount: number;
  packStats: PackStat[];
  pending: Record<string, boolean>;
  removePack: (packName: string) => Promise<void>;
};

export function PackFilesCard({ packCount, packStats, pending, removePack }: PackFilesCardProps) {
  if (packStats.length === 0 && packCount === 0) {
    return null;
  }

  if (packStats.length === 0 && packCount > 0) {
    return (
      <div className="card p-6">
        <h2 className="mb-4 text-xl font-semibold">Pack Files</h2>
        <p className="text-zinc-600 dark:text-zinc-400">
          {packCount} pack files exist but size information not loaded
        </p>
      </div>
    );
  }

  return (
    <div className="card p-6">
      <h2 className="mb-4 text-xl font-semibold">Pack Files ({packCount} total)</h2>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-zinc-200 dark:border-zinc-800">
              <th className="py-2 text-left">Pack Name</th>
              <th className="py-2 text-left">Type</th>
              <th className="py-2 text-right">Pack Size</th>
              <th className="py-2 text-right">Index Size</th>
              <th className="py-2 text-center">Index</th>
              <th className="py-2 text-center">Actions</th>
            </tr>
          </thead>
          <tbody>
            {packStats.map((packStat) => {
              const packName =
                String(packStat.key || "")
                  .split("/")
                  .pop() || String(packStat.key || "");
              const kind = packName.includes("pack-hydr-")
                ? "hydration"
                : packName.includes("pack-test-")
                  ? "test"
                  : "upload";

              return (
                <tr key={packStat.key} className="border-b border-zinc-100 dark:border-zinc-800">
                  <td className="py-2 font-mono text-xs">
                    <span title={packName}>{shortValue(packName, 35)}</span>
                  </td>
                  <td className="py-2">
                    <span
                      className={`rounded px-2 py-1 text-xs ${
                        kind === "hydration"
                          ? "bg-accent-100 dark:bg-accent-900"
                          : kind === "test"
                            ? "bg-yellow-100 dark:bg-yellow-900"
                            : "bg-green-100 dark:bg-green-900"
                      }`}
                    >
                      {kind}
                    </span>
                  </td>
                  <td className="py-2 text-right font-mono text-xs">
                    {packStat.packSize ? formatSampleBytes(packStat.packSize) : "-"}
                  </td>
                  <td className="py-2 text-right font-mono text-xs">
                    {packStat.indexSize ? formatSampleBytes(packStat.indexSize) : "-"}
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
                      className="rounded p-1 text-red-600 transition-colors hover:bg-red-100 hover:text-red-800 dark:text-red-400 dark:hover:bg-red-900/20 dark:hover:text-red-300"
                      title="Delete this pack"
                      aria-label="Delete pack"
                      onClick={() => void removePack(packName)}
                      disabled={pending[`remove-pack:${packName}`]}
                    >
                      <Trash2 className="h-4 w-4" aria-hidden="true" />
                    </button>
                  </td>
                </tr>
              );
            })}
            {packCount > packStats.length ? (
              <tr>
                <td colSpan={6} className="py-2 text-center italic text-zinc-500">
                  Showing first {packStats.length} of {packCount} packs
                </td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
    </div>
  );
}
