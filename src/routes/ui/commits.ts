import type { CacheContext } from "@/cache";
import type { CommitDiffResult, CommitFilePatchResult } from "@/git";
import {
  listCommitsFirstParentRange,
  listMergeSideFirstParent,
  readCommitInfo,
  listCommitChangedFiles,
  readCommitFilePatch,
} from "@/git";
import { isValidOwnerRepo, isValidRef, isValidPath, formatWhen, OID_RE } from "@/web";
import { renderUiView } from "@/client/server/render";
import { handleError } from "@/client/server/error";
import { repoKey } from "@/keys";
import { buildCacheKeyFrom, cacheOrLoadJSONWithTTL } from "@/cache";
import { getRepoActivity } from "@/common";
import { badRequest } from "./helpers";
import type { RouteRequest } from "./helpers";

export async function handleCommits(request: RouteRequest, env: Env, ctx: ExecutionContext) {
  const { owner, repo } = request.params;
  if (!isValidOwnerRepo(owner) || !isValidOwnerRepo(repo)) {
    return badRequest(env, "Invalid owner/repo", "Owner or repo invalid", { owner, repo });
  }
  const repoId = repoKey(owner, repo);
  const u = new URL(request.url);
  const ref = u.searchParams.get("ref") || "main";
  if (!isValidRef(ref)) {
    return badRequest(env, "Invalid ref", "Ref format not allowed", {
      owner,
      repo,
      refEnc: encodeURIComponent(ref),
    });
  }
  const pageRaw = u.searchParams.get("page") || "";
  const perRaw = Number(u.searchParams.get("per_page") || "25");
  const perPage = Number.isFinite(perRaw) ? Math.max(5, Math.min(100, Math.floor(perRaw))) : 25;
  try {
    const cacheCtx: CacheContext = { req: request, ctx };
    let page = Number(pageRaw);
    if (!Number.isFinite(page) || page < 0) page = 0;
    let offset = page * perPage;

    const cacheKey = buildCacheKeyFrom(request, "/_cache/commits", {
      repo: repoId,
      ref,
      per_page: String(perPage),
      page: String(page),
      offset: String(offset),
    });

    const commitsView = await cacheOrLoadJSONWithTTL<
      Array<{
        oid: string;
        shortOid: string;
        firstLine: string;
        authorName: string;
        when: string;
      }>
    >(
      cacheKey,
      async () => {
        const commits = await listCommitsFirstParentRange(
          env,
          repoId,
          ref,
          offset,
          perPage,
          cacheCtx
        );
        return commits.map((c) => ({
          oid: c.oid,
          shortOid: c.oid.slice(0, 7),
          firstLine: (c.message || "").split(/\r?\n/, 1)[0],
          authorName: c.author?.name || "",
          when: c.author ? formatWhen(c.author.when, c.author.tz) : "",
          isMerge: Array.isArray(c.parents) && c.parents.length > 1,
        }));
      },
      () => {
        const isOid = OID_RE.test(ref);
        const isTag = ref.startsWith("refs/tags/");
        // Branch commits: 300s; Tags/OIDs (immutable): 3600s
        return isOid || isTag ? 3600 : 300;
      },
      ctx
    );
    const list = commitsView || [];
    const last = list[list.length - 1]?.oid || "";
    const refEnc = encodeURIComponent(ref);
    const pager = {
      perPageLinks: [10, 25, 50].map((n) => ({
        text: String(n),
        href: `/${owner}/${repo}/commits?ref=${refEnc}&page=${page}&per_page=${n}`,
      })),
      newerHref:
        page > 0
          ? `/${owner}/${repo}/commits?ref=${refEnc}&page=${page - 1}&per_page=${perPage}`
          : undefined,
      olderHref:
        last && list.length === perPage
          ? `/${owner}/${repo}/commits?ref=${refEnc}&page=${page + 1}&per_page=${perPage}`
          : undefined,
    };
    const progress = await getRepoActivity(env, repoId);
    const html = await renderUiView(env, "commits", {
      title: `Commits on ${ref} · ${owner}/${repo}`,
      owner,
      repo,
      ref,
      refEnc,
      commits: list,
      pager,
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
  } catch (e: any) {
    return handleError(env, e, `Error · ${owner}/${repo}`, {
      owner,
      repo,
      refEnc: encodeURIComponent(ref),
    });
  }
}

export async function handleCommitFragments(
  request: RouteRequest,
  env: Env,
  ctx: ExecutionContext
) {
  const { owner, repo, oid } = request.params;
  if (!isValidOwnerRepo(owner) || !isValidOwnerRepo(repo)) {
    return badRequest(env, "Invalid owner/repo", "Owner or repo invalid", { owner, repo });
  }
  if (!OID_RE.test(oid)) {
    return badRequest(env, "Invalid OID", "OID must be 40 hex", {
      owner,
      repo,
      refEnc: encodeURIComponent(oid),
    });
  }
  const u = new URL(request.url);
  const limitRaw = Number(u.searchParams.get("limit") || "20");
  const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(100, Math.floor(limitRaw))) : 20;
  const repoId = repoKey(owner, repo);
  try {
    const cacheCtx: CacheContext = { req: request, ctx };
    const side = await listMergeSideFirstParent(
      env,
      repoId,
      oid,
      limit,
      {
        scanLimit: Math.min(400, limit * 5),
        timeBudgetMs: 5000, // Increased for production R2 latency
        mainlineProbe: 50, // Reduced to speed up initial probe
      },
      cacheCtx
    );
    const commits = (side || []).map((c) => ({
      oid: c.oid,
      shortOid: c.oid.slice(0, 7),
      firstLine: (c.message || "").split(/\r?\n/, 1)[0],
      authorName: c.author?.name || "",
      when: c.author ? formatWhen(c.author.when, c.author.tz) : "",
    }));
    return new Response(JSON.stringify({ owner, repo, commits, compact: true, mergeOf: oid }), {
      headers: {
        "Content-Type": "application/json; charset=utf-8",
        "Cache-Control": "no-store, no-cache, must-revalidate",
        "X-Page-Renderer": "react-fragment-json",
      },
    });
  } catch (e: any) {
    return handleError(env, e, `Error · ${owner}/${repo}`, {
      owner,
      repo,
      refEnc: encodeURIComponent(oid),
    });
  }
}

export async function handleCommitDiff(request: RouteRequest, env: Env, ctx: ExecutionContext) {
  const { owner, repo, oid } = request.params;
  if (!isValidOwnerRepo(owner) || !isValidOwnerRepo(repo)) {
    return badRequest(env, "Invalid owner/repo", "Owner or repo invalid", { owner, repo });
  }
  if (!OID_RE.test(oid)) {
    return badRequest(env, "Invalid commit OID", "Commit id must be 40-hex", {
      owner,
      repo,
      refEnc: encodeURIComponent(oid),
    });
  }
  const url = new URL(request.url);
  const path = url.searchParams.get("path") || "";
  if (!path || !isValidPath(path)) {
    return badRequest(env, "Invalid path", "Path contains invalid characters or is too long", {
      owner,
      repo,
      refEnc: encodeURIComponent(oid),
      path,
    });
  }
  const repoId = repoKey(owner, repo);
  try {
    const cacheCtx: CacheContext = { req: request, ctx };
    const patchCacheKey = buildCacheKeyFrom(request, "/_cache/commit-patch", {
      repo: repoId,
      oid,
      path,
      v: "1",
    });
    const patch = await cacheOrLoadJSONWithTTL<CommitFilePatchResult>(
      patchCacheKey,
      async () => await readCommitFilePatch(env, repoId, oid, path, cacheCtx),
      () => 86400,
      ctx
    );
    return new Response(JSON.stringify(patch), {
      headers: {
        "Content-Type": "application/json; charset=utf-8",
        "Cache-Control": "no-store, no-cache, must-revalidate",
        "X-Page-Renderer": "react-fragment-json",
      },
    });
  } catch (e: any) {
    return handleError(env, e, `Error · ${owner}/${repo}`, {
      owner,
      repo,
      refEnc: encodeURIComponent(oid),
      path,
    });
  }
}

export async function handleCommit(request: RouteRequest, env: Env, ctx: ExecutionContext) {
  const { owner, repo, oid } = request.params;
  if (!isValidOwnerRepo(owner) || !isValidOwnerRepo(repo)) {
    return badRequest(env, "Invalid owner/repo", "Owner or repo invalid", { owner, repo });
  }
  if (!OID_RE.test(oid)) {
    return badRequest(env, "Invalid commit OID", "Commit id must be 40-hex", {
      owner,
      repo,
      refEnc: encodeURIComponent(oid),
    });
  }
  const repoId = repoKey(owner, repo);
  try {
    const cacheCtx: CacheContext = { req: request, ctx };
    const c = await readCommitInfo(env, repoId, oid, cacheCtx);
    const diffCacheKey = buildCacheKeyFrom(request, "/_cache/commit-diff", {
      repo: repoId,
      oid,
      v: "1",
    });
    const diff = await cacheOrLoadJSONWithTTL<CommitDiffResult>(
      diffCacheKey,
      async () =>
        await listCommitChangedFiles(env, repoId, oid, cacheCtx, {
          timeBudgetMs: 5000,
        }),
      () => 86400,
      ctx
    );
    const when = c.author ? formatWhen(c.author.when, c.author.tz) : "";
    const parents = (c.parents || []).map((p) => ({ oid: p, short: p.slice(0, 7) }));
    const progress = await getRepoActivity(env, repoId);
    const html = await renderUiView(env, "commit", {
      title: `${c.oid.slice(0, 7)} · ${owner}/${repo}`,
      owner,
      repo,
      commitOid: c.oid,
      refEnc: encodeURIComponent(c.oid),
      progress,
      commitShort: c.oid.slice(0, 7),
      authorName: c.author?.name || "",
      authorEmail: c.author?.email || "",
      when,
      parents,
      treeShort: (c.tree || "").slice(0, 7),
      message: c.message || "",
      diffBaseRefEnc: diff?.baseCommitOid ? encodeURIComponent(diff.baseCommitOid) : "",
      diffCompareMode: diff?.compareMode || "root",
      diffEntries: diff?.entries || [],
      diffSummary: {
        added: diff?.added || 0,
        modified: diff?.modified || 0,
        deleted: diff?.deleted || 0,
        total: diff?.total || 0,
      },
      diffTruncated: diff?.truncated || false,
      diffTruncateReason: diff?.truncateReason || "",
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
  } catch (e: any) {
    return handleError(env, e, `Error · ${owner}/${repo}`, {
      owner,
      repo,
      refEnc: encodeURIComponent(oid),
      path: "",
    });
  }
}
