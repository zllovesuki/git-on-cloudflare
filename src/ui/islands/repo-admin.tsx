/// <reference lib="dom" />

import { useState } from "react";

import { hydrateIsland } from "@/ui/client/hydrate";

function formatSampleBytes(bytes?: number): string {
  if (bytes === undefined || bytes === null) return "0 KB";
  const mb = bytes / 1048576;
  if (mb >= 1) return `${mb.toFixed(2)} MB`;
  return `${(bytes / 1024).toFixed(1)} KB`;
}

function shortValue(value: string, length: number): string {
  return value.length > length ? `${value.slice(0, length)}...` : value;
}

type PackStat = {
  key?: string;
  packSize?: number;
  indexSize?: number;
  hasIndex?: boolean;
};

type HydrationData = {
  running?: boolean;
  stage?: string;
  startedAt?: number;
  queued?: number;
  error?: string;
  progress?: {
    packIndex?: number;
    producedBytes?: number;
    segmentSeq?: number;
  };
};

type AdminState = {
  packStats?: PackStat[];
  meta?: { doId?: string };
  looseR2SampleBytes?: number;
  looseR2SampleCount?: number;
  looseR2Truncated?: boolean;
  dbSizeBytes?: number;
  unpackWork?: { processedCount?: number; totalCount?: number };
  unpackNext?: unknown;
};

export type RepoAdminProps = {
  owner: string;
  repo: string;
  refEnc: string;
  head?: { target?: string; unborn?: boolean };
  refs: Array<{ name: string; oid: string }>;
  storageSize: string;
  packCount: number;
  packList: string[];
  state: AdminState;
  defaultBranch: string;
  hydrationStatus: string;
  hydrationStartedAt?: string | null;
  hydrationData?: HydrationData;
  hydrationPackCount: number;
  nextMaintenanceIn?: string;
  nextMaintenanceAt?: string;
};

async function readJson(response: Response) {
  return response.json().catch(() => ({}));
}

function JsonResult({ data }: { data: unknown }) {
  return (
    <div className="mt-2">
      <pre className="overflow-x-auto rounded-xl bg-zinc-100 p-4 text-sm dark:bg-zinc-800">
        {JSON.stringify(data, null, 2)}
      </pre>
    </div>
  );
}

export function RepoAdminIsland(props: RepoAdminProps) {
  const {
    owner,
    repo,
    head,
    refs,
    storageSize,
    packCount,
    packList,
    state,
    defaultBranch,
    hydrationStatus,
    hydrationStartedAt,
    hydrationData,
    hydrationPackCount,
    nextMaintenanceIn,
    nextMaintenanceAt,
  } = props;

  const [hydrationResult, setHydrationResult] = useState<unknown>(null);
  const [oidResult, setOidResult] = useState<unknown>(null);
  const [stateDump, setStateDump] = useState<unknown>(null);
  const [debugOid, setDebugOid] = useState("");
  const [pending, setPending] = useState<Record<string, boolean>>({});
  const [copiedDoId, setCopiedDoId] = useState(false);

  const branchCount = refs.filter((ref) => ref.name.includes("refs/heads/")).length;
  const tagCount = refs.filter((ref) => ref.name.includes("refs/tags/")).length;
  const packStats: PackStat[] = Array.isArray(state.packStats) ? state.packStats : [];
  const hydrationRunning = Boolean(hydrationData?.running);

  async function runAction<T>(key: string, action: () => Promise<T>) {
    setPending((current) => ({ ...current, [key]: true }));
    try {
      return await action();
    } finally {
      setPending((current) => ({ ...current, [key]: false }));
    }
  }

  async function copyDoId(doId: string) {
    try {
      await navigator.clipboard.writeText(doId);
      setCopiedDoId(true);
      window.setTimeout(() => setCopiedDoId(false), 1200);
    } catch (error) {
      window.alert(`Failed to copy DO ID: ${String(error)}`);
    }
  }

  async function startHydration(dryRun: boolean) {
    await runAction(dryRun ? "hydration-dry-run" : "hydration-start", async () => {
      const response = await fetch(`/${owner}/${repo}/admin/hydrate`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ dryRun }),
      });
      const data = await readJson(response);
      setHydrationResult(data);
      if (response.ok && !dryRun) {
        window.setTimeout(() => window.location.reload(), 2000);
      }
    });
  }

  async function clearHydration() {
    if (!window.confirm("Clear all hydration state and hydration-generated packs?")) {
      return;
    }

    await runAction("hydration-clear", async () => {
      const response = await fetch(`/${owner}/${repo}/admin/hydrate`, { method: "DELETE" });
      const data = await readJson(response);
      if ((data as { ok?: boolean }).ok) {
        window.alert("Hydration state cleared successfully");
        window.location.reload();
        return;
      }

      window.alert(`Error: ${(data as { error?: string }).error || "Unknown error"}`);
    });
  }

  async function removePack(packName: string) {
    let warning = `Are you sure you want to remove pack: ${packName}?\n\nThis will delete the pack file, its index, and all associated metadata.`;
    if (packName.includes("pack-hydr-")) {
      warning = `WARNING: This is a hydration pack!\n\n${warning}\n\nRemoving hydration packs can impact fetch correctness. Only do this for troubleshooting and re-run hydration afterward.`;
    }
    if (!window.confirm(warning)) {
      return;
    }

    await runAction(`remove-pack:${packName}`, async () => {
      const response = await fetch(`/${owner}/${repo}/admin/pack/${encodeURIComponent(packName)}`, {
        method: "DELETE",
      });
      const data = await readJson(response);
      if ((data as { ok?: boolean }).ok) {
        window.alert(
          `Pack removed successfully:\n- Pack file: ${(data as { deletedPack?: boolean }).deletedPack ? "deleted" : "not found"}\n- Index file: ${(data as { deletedIndex?: boolean }).deletedIndex ? "deleted" : "not found"}\n- Metadata: ${(data as { deletedMetadata?: boolean }).deletedMetadata ? "cleaned" : "unchanged"}`
        );
        window.location.reload();
        return;
      }

      window.alert(`Error removing pack: ${(data as { error?: string }).error || "Unknown error"}`);
    });
  }

  async function checkOid() {
    if (!/^[a-f0-9]{40}$/i.test(debugOid.trim())) {
      window.alert("Please enter a valid 40-character SHA-1 hash");
      return;
    }

    await runAction("check-oid", async () => {
      const response = await fetch(`/${owner}/${repo}/admin/debug-oid/${debugOid.trim()}`);
      setOidResult(await readJson(response));
    });
  }

  async function dumpState() {
    await runAction("dump-state", async () => {
      const response = await fetch(`/${owner}/${repo}/admin/debug-state`);
      setStateDump(await readJson(response));
    });
  }

  async function purgeRepo() {
    const confirmation = window.prompt(
      `This action will PERMANENTLY DELETE all repository data.\n\nTo confirm, type exactly: purge-${owner}/${repo}`
    );
    if (confirmation !== `purge-${owner}/${repo}`) {
      if (confirmation !== null) {
        window.alert("Confirmation text did not match. Action cancelled.");
      }
      return;
    }
    if (!window.confirm("Final confirmation: Delete this repository forever?")) {
      return;
    }

    await runAction("purge-repo", async () => {
      const response = await fetch(`/${owner}/${repo}/admin/purge`, {
        method: "DELETE",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ confirm: confirmation }),
      });
      const data = await readJson(response);
      if ((data as { ok?: boolean }).ok) {
        window.alert("Repository has been permanently deleted");
        window.location.href = `/${owner}`;
        return;
      }

      window.alert(`Error: ${(data as { error?: string }).error || "Unknown error"}`);
    });
  }

  return (
    <div className="space-y-6">
      <div className="alert warn">
        <strong>
          <i
            className="bi bi-exclamation-triangle-fill mr-2 inline h-4 w-4 align-[-2px] text-amber-600 dark:text-amber-400"
            aria-hidden="true"
          ></i>
          Admin Area
        </strong>{" "}
        - Actions here can permanently modify repository data
      </div>

      <div className="card p-6">
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
                  <i className="bi bi-clipboard block h-4 w-4" aria-hidden="true"></i>
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
                  ({state.looseR2SampleCount || 0} objs{state.looseR2Truncated ? ", truncated" : ""}
                  )
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
            <div className="font-mono text-sm text-indigo-600 dark:text-indigo-400">
              {hydrationStatus || "Not Started"}
            </div>
          </div>
          <div>
            <div className="mb-1 text-xs uppercase tracking-wide text-zinc-500 dark:text-zinc-400">
              HEAD
            </div>
            <div className="font-mono text-sm">
              {head?.target
                ? String(head.target).replace(/^refs\/(heads|tags)\//, "")
                : head?.unborn
                  ? "unborn"
                  : "unknown"}
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
      </div>

      <div className="card p-6">
        <h2 className="mb-4 text-xl font-semibold">Pack Hydration</h2>
        <p className="mb-4 text-zinc-600 dark:text-zinc-400">
          Hydration builds thick packs used to serve fetches correctly and fast. It is required for
          correctness, not just performance.
        </p>
        {hydrationRunning ? (
          <div className="mb-4 rounded-xl bg-indigo-50 p-3 dark:bg-indigo-900/20">
            <div className="text-sm">
              <strong>
                <i
                  className="bi bi-info-circle-fill mr-1 inline h-4 w-4 align-[-2px] text-indigo-600 dark:text-indigo-400"
                  aria-hidden="true"
                ></i>
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
              <i
                className="bi bi-check-circle-fill mr-1 inline h-4 w-4 align-[-2px] text-green-600 dark:text-green-400"
                aria-hidden="true"
              ></i>
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
              <i className="bi bi-search mr-2 inline h-4 w-4 align-[-2px]" aria-hidden="true"></i>
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
              <i
                className="bi bi-play-fill mr-2 inline h-4 w-4 align-[-2px]"
                aria-hidden="true"
              ></i>
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
              <i className="bi bi-trash3 mr-2 inline h-4 w-4 align-[-2px]" aria-hidden="true"></i>
              <span className="label">
                {pending["hydration-clear"] ? "Clearing..." : "Clear Hydration State"}
              </span>
            </button>
          </div>
          {hydrationResult ? <JsonResult data={hydrationResult} /> : null}
        </div>
      </div>

      {packStats.length ? (
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
                    <tr
                      key={packStat.key}
                      className="border-b border-zinc-100 dark:border-zinc-800"
                    >
                      <td className="py-2 font-mono text-xs">
                        <span title={packName}>{shortValue(packName, 35)}</span>
                      </td>
                      <td className="py-2">
                        <span
                          className={`rounded px-2 py-1 text-xs ${
                            kind === "hydration"
                              ? "bg-indigo-100 dark:bg-indigo-900"
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
                          <i
                            className="bi bi-check-circle-fill inline h-4 w-4 text-green-600 dark:text-green-400"
                            aria-hidden="true"
                          ></i>
                        ) : (
                          <i
                            className="bi bi-x-circle inline h-4 w-4 text-red-600 dark:text-red-400"
                            aria-hidden="true"
                          ></i>
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
                          <i className="bi bi-trash3" style={{ width: "16px", height: "16px" }}></i>
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
      ) : packCount > 0 ? (
        <div className="card p-6">
          <h2 className="mb-4 text-xl font-semibold">Pack Files</h2>
          <p className="text-zinc-600 dark:text-zinc-400">
            {packCount} pack files exist but size information not loaded
          </p>
        </div>
      ) : null}

      <div className="card p-6">
        <div className="mb-4 flex items-center justify-between">
          <h2 className="text-xl font-semibold">References</h2>
          <button className="btn sm" type="button" onClick={() => window.location.reload()}>
            <i
              className="bi bi-arrow-clockwise mr-2 inline h-4 w-4 align-[-2px]"
              aria-hidden="true"
            ></i>
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
                {refs.map((ref) => (
                  <tr key={ref.name} className="border-b border-zinc-100 dark:border-zinc-800">
                    <td className="py-2 font-mono text-xs">
                      {ref.name.replace(/^refs\/(heads|tags)\//, "")}
                    </td>
                    <td className="py-2 font-mono text-xs text-zinc-600 dark:text-zinc-400">
                      <span title={ref.oid}>{shortValue(ref.oid, 12)}</span>
                    </td>
                    <td className="py-2">
                      <span
                        className={`rounded px-2 py-1 text-xs ${
                          ref.name.includes("refs/heads/")
                            ? "bg-indigo-100 dark:bg-indigo-900"
                            : ref.name.includes("refs/tags/")
                              ? "bg-green-100 dark:bg-green-900"
                              : "bg-zinc-100 dark:bg-zinc-800"
                        }`}
                      >
                        {ref.name.includes("refs/heads/")
                          ? "branch"
                          : ref.name.includes("refs/tags/")
                            ? "tag"
                            : ref.name.split("/")[0]}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <p className="italic text-zinc-500">No references found</p>
        )}
      </div>

      <div className="card p-6">
        <h2 className="mb-4 text-xl font-semibold">Debug Tools</h2>
        <div className="space-y-4">
          <div>
            <label htmlFor="debug-oid" className="mb-2 block text-sm font-medium">
              Check Object ID Existence
            </label>
            <div className="flex gap-2">
              <input
                type="text"
                id="debug-oid"
                placeholder="Enter 40-character SHA-1 hash"
                className="flex-1 rounded-xl border border-zinc-300 bg-white px-3 py-2 font-mono text-sm focus:ring-2 focus:ring-indigo-500 dark:border-zinc-700 dark:bg-zinc-800"
                pattern="[a-f0-9]{40}"
                value={debugOid}
                onChange={(event) => setDebugOid(event.target.value)}
              />
              <button
                className="btn"
                type="button"
                onClick={() => void checkOid()}
                disabled={pending["check-oid"]}
              >
                {pending["check-oid"] ? "Checking..." : "Check OID"}
              </button>
            </div>
            {oidResult ? <JsonResult data={oidResult} /> : null}
          </div>

          <div>
            <label className="mb-2 block text-sm font-medium">Repository State Dump</label>
            <button
              className="btn"
              type="button"
              onClick={() => void dumpState()}
              disabled={pending["dump-state"]}
            >
              <i
                className="bi bi-bar-chart-fill mr-2 inline h-4 w-4 align-[-2px]"
                aria-hidden="true"
              ></i>
              <span className="label">
                {pending["dump-state"] ? "Processing..." : "View DO State"}
              </span>
            </button>
            {stateDump ? <JsonResult data={stateDump} /> : null}
          </div>
        </div>
      </div>

      <details className="card border-2 border-red-500 p-6 dark:border-red-600">
        <summary className="cursor-pointer font-bold text-red-600 dark:text-red-500">
          <i
            className="bi bi-exclamation-triangle-fill mr-2 inline h-4 w-4 align-[-2px]"
            aria-hidden="true"
          ></i>
          Danger Zone - Irreversible Actions
        </summary>
        <div className="mt-6 space-y-4">
          <div className="alert error">
            <strong>Warning:</strong> These actions cannot be undone. All repository data will be
            permanently deleted.
          </div>
          <p className="text-sm text-zinc-600 dark:text-zinc-400">
            This will delete all objects, packs, references, and metadata associated with this
            repository. The repository will be removed from the owner registry.
          </p>
          <button
            className="btn bg-red-600 text-white hover:bg-red-700"
            type="button"
            onClick={() => void purgeRepo()}
            disabled={pending["purge-repo"]}
          >
            <i
              className="bi bi-trash3-fill mr-2 inline h-4 w-4 align-[-2px]"
              aria-hidden="true"
            ></i>
            <span className="label">
              {pending["purge-repo"] ? "Deleting..." : "Permanently Delete Repository"}
            </span>
          </button>
          <p className="muted text-xs">
            Default branch: <code>{defaultBranch}</code>
          </p>
          {packList.length ? (
            <p className="muted text-xs">Visible pack keys: {packList.length}</p>
          ) : null}
        </div>
      </details>
    </div>
  );
}

export function initRepoAdmin() {
  hydrateIsland<RepoAdminProps>("repo-admin", RepoAdminIsland);
}
