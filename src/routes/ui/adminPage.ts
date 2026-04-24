import type { HeadInfo, Ref } from "@/git";
import { isValidOwnerRepo } from "@/web";
import { renderUiView } from "@/client/server/render";
import { getRepoActivity, getRepoStub, unauthorizedAdminBasic } from "@/common";
import { verifyAuth } from "@/auth";
import { repoKey } from "@/keys";
import {
  badRequest,
  computeStorageMetrics,
  computeCompactionStatus,
  getDefaultBranchFromHead,
  loadAdminPackRefIndexState,
  loadHeadAndRefsCached,
  type DebugState,
  type RouteRequest,
} from "./helpers";

export async function handleAdminPage(request: RouteRequest, env: Env, ctx: ExecutionContext) {
  const { owner, repo } = request.params;

  // Validate parameters
  if (!isValidOwnerRepo(owner) || !isValidOwnerRepo(repo)) {
    return badRequest(env, "Invalid owner/repo", "Owner or repo invalid", { owner, repo });
  }

  // Check authentication - admin access required
  if (!(await verifyAuth(env, owner, request, true))) {
    return unauthorizedAdminBasic();
  }

  const repoId = repoKey(owner, repo);
  const stub = getRepoStub(env, repoId);
  const cacheCtx = { req: request, ctx };

  // Gather admin data in parallel for performance
  const [rawState, refsData, progress] = await Promise.all([
    stub.debugState().catch(() => ({}) as Partial<DebugState>),
    loadHeadAndRefsCached(env, request, ctx, repoId),
    getRepoActivity(env, repoId),
  ]);
  const state = await loadAdminPackRefIndexState({
    env,
    repoId,
    state: rawState,
    cacheCtx,
  });
  const head: HeadInfo | undefined = refsData?.head || undefined;
  const refs: Ref[] = refsData?.refs || [];

  const { storageSize, packCount, packList, supersededPackCount } = computeStorageMetrics(state);
  const { compactionStatus, compactionStartedAt } = computeCompactionStatus(state.compaction);

  const defaultBranch = getDefaultBranchFromHead(head);
  const refEnc = encodeURIComponent(defaultBranch);

  const html = await renderUiView(env, "admin", {
    title: `Admin · ${owner}/${repo}`,
    owner,
    repo,
    refEnc,
    head,
    refs,
    storageSize,
    packCount,
    packList,
    state,
    defaultBranch,
    compactionStatus,
    compactionStartedAt,
    compactionData: state.compaction,
    supersededPackCount,
    progress,
  });
  if (!html) {
    return new Response("Failed to render view", { status: 500 });
  }
  return new Response(html, {
    headers: {
      "Content-Type": "text/html; charset=utf-8",
      "Cache-Control": "no-store, no-cache, must-revalidate",
      "X-Page-Renderer": "react-ssr",
    },
  });
}
