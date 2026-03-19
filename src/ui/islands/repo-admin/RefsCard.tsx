import { RefreshCw } from "lucide-react";
import { classifyRef, shortRefName } from "@/git/refDisplay.ts";
import { shortValue } from "./format";

export type RefsCardProps = {
  refs: Array<{ name: string; oid: string }>;
};

export function RefsCard({ refs }: RefsCardProps) {
  return (
    <div className="card p-6">
      <div className="mb-4 flex items-center justify-between">
        <h2 className="text-xl font-semibold">References</h2>
        <button className="btn sm" type="button" onClick={() => window.location.reload()}>
          <RefreshCw className="mr-2 inline h-4 w-4 align-[-2px]" aria-hidden="true" />
          <span className="label">Refresh All</span>
        </button>
      </div>
      {refs.length ? (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-zinc-200 dark:border-zinc-800">
                <th className="py-2 text-left">Reference Name</th>
                <th className="py-2 text-left">Target OID</th>
                <th className="py-2 text-left">Type</th>
              </tr>
            </thead>
            <tbody>
              {refs.map((ref) => {
                const kind = classifyRef(ref.name);

                return (
                  <tr key={ref.name} className="border-b border-zinc-100 dark:border-zinc-800">
                    <td className="py-2 font-mono text-xs">{shortRefName(ref.name)}</td>
                    <td className="py-2 font-mono text-xs text-zinc-600 dark:text-zinc-400">
                      <span title={ref.oid}>{shortValue(ref.oid, 12)}</span>
                    </td>
                    <td className="py-2">
                      <span
                        className={`rounded px-2 py-1 text-xs ${
                          kind === "branch"
                            ? "bg-accent-100 dark:bg-accent-900"
                            : kind === "tag"
                              ? "bg-green-100 dark:bg-green-900"
                              : "bg-zinc-100 dark:bg-zinc-800"
                        }`}
                      >
                        {kind === "branch"
                          ? "branch"
                          : kind === "tag"
                            ? "tag"
                            : ref.name.split("/")[0]}
                      </span>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      ) : (
        <p className="italic text-zinc-500">No references found</p>
      )}
    </div>
  );
}
