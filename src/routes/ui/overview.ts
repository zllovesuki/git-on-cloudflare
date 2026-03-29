import type { HeadInfo, Ref } from "@/git";
import type { CacheContext } from "@/cache";
import { readPath } from "@/git";
import { classifyRef, formatRefOption, shortRefName } from "@/git/refDisplay.ts";
import { isValidOwnerRepo, bytesToText } from "@/web";
import { renderUiView } from "@/client/server/render";
import { listReposForOwner } from "@/registry";
import { buildCacheKeyFrom, cacheOrLoadJSON } from "@/cache";
import { getRepoActivity } from "@/common";
import { repoKey } from "@/keys";
import { badRequest, loadHeadAndRefsCached } from "./helpers";
import type { RouteRequest } from "./helpers";

export async function handleOwnerOverview(request: RouteRequest, env: Env) {
  const { owner } = request.params;
  if (!isValidOwnerRepo(owner)) {
    return badRequest(env, "Invalid owner", "Owner contains invalid characters or length");
  }
  const repos = await listReposForOwner(env, owner);
  const html = await renderUiView(env, "owner", {
    title: `${owner} · Repositories`,
    owner,
    repos,
  });
  if (!html) {
    return new Response("Failed to render view", { status: 500 });
  }
  return new Response(html, {
    headers: {
      "Content-Type": "text/html; charset=utf-8",
      "Cache-Control": "public, max-age=60",
      "X-Page-Renderer": "react-ssr",
    },
  });
}

export async function handleRepoOverview(request: RouteRequest, env: Env, ctx: ExecutionContext) {
  const { owner, repo } = request.params;
  if (!isValidOwnerRepo(owner) || !isValidOwnerRepo(repo)) {
    return badRequest(env, "Invalid owner/repo", "Owner or repo invalid", { owner, repo });
  }
  const repoId = repoKey(owner, repo);

  const refsData = await loadHeadAndRefsCached(env, request, ctx, repoId);
  const head: HeadInfo | undefined = refsData?.head;
  const refs: Ref[] = refsData?.refs || [];

  const defaultRef = head?.target || (refs[0]?.name ?? "refs/heads/main");
  const refShort = shortRefName(defaultRef);
  const refEnc = encodeURIComponent(refShort);
  const branchesData = refs
    .filter((ref) => classifyRef(ref.name) === "branch")
    .map(formatRefOption);
  const tagsData = refs.filter((ref) => classifyRef(ref.name) === "tag").map(formatRefOption);

  // Try to load README at repo root on default branch with caching (5 minutes)
  const cacheKeyReadme = buildCacheKeyFrom(request, "/_cache/readme", {
    repo: repoId,
    ref: refShort,
  });
  const readmeData = await cacheOrLoadJSON<{ md: string }>(
    cacheKeyReadme,
    async () => {
      try {
        // Load all candidates in parallel for better performance
        const candidates = ["README.md", "README.MD", "Readme.md", "README", "readme.md"];
        const cacheCtx: CacheContext = { req: request, ctx };
        const results = await Promise.all(
          candidates.map(async (name) => {
            try {
              const res = await readPath(env, repoId, refShort, name, cacheCtx);
              if (res.type === "blob") {
                return { name, content: res.content };
              }
            } catch {}
            return null;
          })
        );
        const found = results.find((r) => r !== null) as {
          name: string;
          content: Uint8Array;
        } | null;
        if (!found) return null;
        const text = bytesToText(found.content);
        return { md: text };
      } catch {
        return null;
      }
    },
    300,
    ctx
  );
  const readmeMd = readmeData?.md || "";
  const progress = await getRepoActivity(env, repoId);

  const html = await renderUiView(env, "overview", {
    title: `${owner}/${repo}`,
    owner,
    repo,
    refShort,
    refEnc,
    branches: branchesData,
    tags: tagsData,
    readmeMd,
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
