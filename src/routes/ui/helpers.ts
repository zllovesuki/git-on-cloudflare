import type { HeadInfo, Ref } from "@/git";
import { getHeadAndRefs } from "@/git";
import { shortRefName } from "@/git/refDisplay.ts";
import { formatSize, HttpError } from "@/web";
import { handleError } from "@/client/server/error";
import { buildCacheKeyFrom, cacheOrLoadJSON } from "@/cache";
import type { DebugStateSnapshot } from "@/do/repo/debug";

export type DebugState = DebugStateSnapshot;
export type CompactionData = DebugState["compaction"];
export type RouteRequest = Request & {
  params: { owner: string; repo: string; [key: string]: string };
};

export async function badRequest(
  env: Env,
  title: string,
  message: string,
  extra?: { owner?: string; repo?: string; refEnc?: string; path?: string }
): Promise<Response> {
  return handleError(env, new HttpError(400, message, { expose: true }), title, extra);
}

export function formatFromNowShort(deltaMs: number): string {
  const s = Math.round(deltaMs / 1000);
  if (s <= 0) return "soon";
  const m = Math.floor(s / 60);
  const h = Math.floor(m / 60);
  const d = Math.floor(h / 24);
  if (d > 0) return `in ${d}d ${h % 24}h`;
  if (h > 0) return `in ${h}h ${m % 60}m`;
  if (m > 0) return `in ${m}m`;
  return `in ${s}s`;
}

export async function loadHeadAndRefsCached(
  env: Env,
  request: Request,
  ctx: ExecutionContext,
  repoId: string
): Promise<{ head: HeadInfo | undefined; refs: Ref[] } | null> {
  const cacheKeyRefs = buildCacheKeyFrom(request, "/_cache/refs", { repo: repoId });
  return cacheOrLoadJSON<{ head: HeadInfo | undefined; refs: Ref[] }>(
    cacheKeyRefs,
    async () => {
      try {
        const res = await getHeadAndRefs(env, repoId);
        return { head: res.head, refs: res.refs };
      } catch {
        return null;
      }
    },
    60,
    ctx
  );
}

export function getDefaultBranchFromHead(head: HeadInfo | undefined): string {
  return head?.target ? shortRefName(head.target) : "main";
}

export function computeStorageMetrics(state: Partial<DebugState> | undefined): {
  storageSize: string;
  packCount: number;
  packList: string[];
  supersededPackCount: number;
} {
  let totalStorageBytes = 0;
  const packStats = state?.packStats ?? [];
  for (const pack of packStats) {
    if (typeof pack.packSize === "number") totalStorageBytes += pack.packSize;
    if (typeof pack.indexSize === "number") totalStorageBytes += pack.indexSize;
  }
  const storageSize = formatSize(totalStorageBytes);
  const activePacks = state?.activePacks ?? [];
  const packList = activePacks.map((pack) => pack.key);
  const packCount = packList.length;
  const supersededPackCount = state?.supersededPacks?.length ?? 0;
  return { storageSize, packCount, packList, supersededPackCount };
}

export function computeCompactionStatus(compactionData: CompactionData | undefined): {
  compactionStatus: string;
  compactionStartedAt: string | null;
} {
  let compactionStatus = "Idle";
  let compactionStartedAt: string | null = null;
  if (compactionData?.running) {
    compactionStatus = "Running";
    if (compactionData.startedAt) {
      try {
        compactionStartedAt = new Date(compactionData.startedAt).toLocaleString();
      } catch {}
    }
  } else if (compactionData?.queued) {
    compactionStatus = "Queued";
  }
  return { compactionStatus, compactionStartedAt };
}
