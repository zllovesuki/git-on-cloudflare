/// <reference lib="dom" />

import { TriangleAlert } from "lucide-react";
import { hydrateIsland } from "@/client/hydrate";
import { countRefsByKind } from "@/git/refDisplay.ts";

export type { RepoAdminProps } from "./types";
import type { RepoAdminProps } from "./types";
import { useRepoAdminActions } from "./useRepoAdminActions";
import { RepoOverviewCard } from "./RepoOverviewCard";
import { CompactionCard } from "./CompactionCard";
import { PackFilesCard } from "./PackFilesCard";
import { RefsCard } from "./RefsCard";
import { DebugToolsCard } from "./DebugToolsCard";
import { DangerZoneCard } from "./DangerZoneCard";

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
    compactionStatus,
    compactionStartedAt,
    compactionData,
    supersededPackCount,
  } = props;

  const {
    compactionResult,
    oidResult,
    stateDump,
    pending,
    startCompaction,
    clearCompaction,
    removePack,
    checkOid,
    dumpState,
    purgeRepo,
  } = useRepoAdminActions(owner, repo);

  const { branchCount, tagCount } = countRefsByKind(refs);
  const packStats = Array.isArray(state.packStats) ? state.packStats : [];

  return (
    <div className="space-y-6">
      <div className="rounded-xl border border-amber-500/20 bg-amber-500/10 p-4 text-sm text-amber-700 dark:text-amber-300">
        <strong>
          <TriangleAlert
            className="mr-2 inline h-4 w-4 align-[-2px] text-amber-600 dark:text-amber-400"
            aria-hidden="true"
          />
          Admin Area
        </strong>{" "}
        - Actions here can permanently modify repository data
      </div>

      <RepoOverviewCard
        storageSize={storageSize}
        packCount={packCount}
        supersededPackCount={supersededPackCount}
        compactionStatus={compactionStatus}
        state={state}
        head={head}
        branchCount={branchCount}
        tagCount={tagCount}
      />

      <CompactionCard
        compactionData={compactionData}
        compactionStartedAt={compactionStartedAt}
        compactionStatus={compactionStatus}
        state={state}
        pending={pending}
        startCompaction={startCompaction}
        clearCompaction={clearCompaction}
        compactionResult={compactionResult}
      />

      <PackFilesCard
        packCount={packCount}
        packStats={packStats}
        pending={pending}
        removePack={removePack}
      />

      <RefsCard refs={refs} />

      <DebugToolsCard
        oidResult={oidResult}
        stateDump={stateDump}
        pending={pending}
        checkOid={checkOid}
        dumpState={dumpState}
      />

      <DangerZoneCard
        defaultBranch={defaultBranch}
        packList={packList}
        pending={pending}
        purgeRepo={purgeRepo}
      />
    </div>
  );
}

export function initRepoAdmin() {
  hydrateIsland<RepoAdminProps>("repo-admin", RepoAdminIsland);
}
