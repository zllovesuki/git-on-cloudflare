import type { CacheContext } from "@/cache";
import type { DebugPackState, DebugStateSnapshot } from "@/do/repo/debug";
import type { HeadInfo, Ref } from "@/git";
import type { PackRefIndexStatus } from "@/git/pack/refIndex.ts";
import { getHeadAndRefs } from "@/git";
import { shortRefName } from "@/git/refDisplay.ts";
import { formatSize, HttpError } from "@/web";
import { handleError } from "@/client/server/error";
import { buildCacheKeyFrom, cacheOrLoadJSON } from "@/cache";
import { createLogger } from "@/common/logger.ts";
import { countSubrequest, getLimiter } from "@/git/operations/limits.ts";
import { packRefsKey } from "@/keys.ts";

export type AdminPackState = DebugPackState & {
  refIndexStatus?: PackRefIndexStatus;
  refIndexSize?: number;
};
export type DebugState = Omit<
  DebugStateSnapshot,
  "packStats" | "activePacks" | "supersededPacks"
> & {
  packStats?: AdminPackState[];
  activePacks: AdminPackState[];
  supersededPacks: AdminPackState[];
};
export type CompactionData = DebugState["compaction"];
export type RouteRequest = Request & {
  params: { owner: string; repo: string; [key: string]: string };
};

type PackRefIndexMetadata = {
  status: PackRefIndexStatus;
  size?: number;
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

function collectPackKeys(state: Partial<DebugStateSnapshot>): string[] {
  const keys = new Set<string>();
  for (const pack of state.packStats ?? []) keys.add(pack.key);
  for (const pack of state.activePacks ?? []) keys.add(pack.key);
  for (const pack of state.supersededPacks ?? []) keys.add(pack.key);
  return [...keys];
}

function applyPackRefMetadata(
  packs: DebugPackState[] | undefined,
  metadataByPackKey: Map<string, PackRefIndexMetadata>
): AdminPackState[] | undefined {
  if (!packs) return undefined;

  return packs.map((pack) => {
    const metadata = metadataByPackKey.get(pack.key);
    if (!metadata) {
      return { ...pack, refIndexStatus: "unknown" };
    }

    return {
      ...pack,
      refIndexStatus: metadata.status,
      refIndexSize: metadata.size,
    };
  });
}

async function loadPackRefIndexMetadata(args: {
  env: Env;
  repoId: string;
  state: Partial<DebugStateSnapshot>;
  cacheCtx: CacheContext;
}): Promise<Map<string, PackRefIndexMetadata>> {
  const packKeys = collectPackKeys(args.state);
  const metadataByPackKey = new Map<string, PackRefIndexMetadata>();
  if (packKeys.length === 0) return metadataByPackKey;

  const limiter = getLimiter(args.cacheCtx);
  const log = createLogger(args.env.LOG_LEVEL, { service: "AdminPage", repoId: args.repoId });
  const entries = await Promise.all(
    packKeys.map(async (packKey): Promise<[string, PackRefIndexMetadata]> => {
      const refsKey = packRefsKey(packKey);
      try {
        const refsObject = await limiter.run("r2:head-pack-refs", async () => {
          if (!countSubrequest(args.cacheCtx)) {
            log.warn("admin:pack-ref-head-budget-exhausted", { packKey, refsKey });
          }
          return await args.env.REPO_BUCKET.head(refsKey);
        });

        if (!refsObject) {
          log.debug("admin:pack-ref-head-missing", { packKey, refsKey });
          return [packKey, { status: "missing" }];
        }

        log.debug("admin:pack-ref-head-present", {
          packKey,
          refsKey,
          bytes: refsObject.size,
        });
        return [packKey, { status: "present", size: refsObject.size }];
      } catch (error) {
        log.warn("admin:pack-ref-head-failed", {
          packKey,
          refsKey,
          error: String(error),
        });
        return [packKey, { status: "unknown" }];
      }
    })
  );

  let present = 0;
  let missing = 0;
  let unknown = 0;
  for (const [packKey, metadata] of entries) {
    metadataByPackKey.set(packKey, metadata);
    if (metadata.status === "present") present++;
    else if (metadata.status === "missing") missing++;
    else unknown++;
  }
  log.debug("admin:pack-ref-head-summary", {
    packs: packKeys.length,
    present,
    missing,
    unknown,
  });
  return metadataByPackKey;
}

export async function loadAdminPackRefIndexState(args: {
  env: Env;
  repoId: string;
  state: Partial<DebugStateSnapshot>;
  cacheCtx: CacheContext;
}): Promise<Partial<DebugState>> {
  const metadataByPackKey = await loadPackRefIndexMetadata(args);
  return {
    ...args.state,
    packStats: applyPackRefMetadata(args.state.packStats, metadataByPackKey),
    activePacks: applyPackRefMetadata(args.state.activePacks, metadataByPackKey),
    supersededPacks: applyPackRefMetadata(args.state.supersededPacks, metadataByPackKey),
  };
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
    if (typeof pack.refIndexSize === "number") totalStorageBytes += pack.refIndexSize;
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
