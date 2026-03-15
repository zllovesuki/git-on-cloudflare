import type { TreeEntry, HeadInfo, Ref } from "@/git";
import type { CacheContext } from "@/cache";
import type { debugState } from "@/do/repo/debug";
import { getConfig } from "@/do/repo/repoConfig.ts";

import { AutoRouter } from "itty-router";
import {
  getHeadAndRefs,
  readPath,
  listCommitsFirstParentRange,
  listMergeSideFirstParent,
  readCommitInfo,
  readBlobStream,
} from "@/git";
import { repoKey } from "@/keys";
import {
  detectBinary,
  formatSize,
  bytesToText,
  formatWhen,
  getFileIconClass,
  getHighlightLangsForBlobSmart,
  isValidOwnerRepo,
  isValidRef,
  isValidPath,
  OID_RE,
  getContentTypeFromName,
  HttpError,
} from "@/web";
import { renderUiView } from "@/ui/server/render";
import { handleError } from "@/ui/server/error";
import { listReposForOwner } from "@/registry";
import { buildCacheKeyFrom, cacheOrLoadJSON, cacheOrLoadJSONWithTTL } from "@/cache";
import { getUnpackProgress, getRepoStub } from "@/common";
import { verifyAuth } from "@/auth";

// Shorthand for 400 Bad Request using the shared error handler
async function badRequest(
  env: Env,
  title: string,
  message: string,
  extra?: { owner?: string; repo?: string; refEnc?: string; path?: string }
): Promise<Response> {
  return handleError(env, new HttpError(400, message, { expose: true }), title, extra);
}

// Short "from now" formatter like "in 3m", "in 1h 20m", "in 2d 5h"
function formatFromNowShort(deltaMs: number): string {
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

// Shared type for admin debug state helpers
type DebugState = Awaited<ReturnType<typeof debugState>>;
type HydrationData = DebugState["hydration"];
type ReadPathResult = Awaited<ReturnType<typeof readPath>>;
type RouteRequest = Request & { params: { owner: string; repo: string } };

// Cache HEAD and refs for 60s
async function loadHeadAndRefsCached(
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

function getDefaultBranchFromHead(head: HeadInfo | undefined): string {
  return head?.target?.replace(/^refs\/(heads|tags)\//, "") || "main";
}

function computeStorageMetrics(state: Partial<DebugState> | undefined): {
  storageSize: string;
  packCount: number;
  packList: string[];
  hydrationPackCount: number;
} {
  let totalStorageBytes = 0;
  const packStats = (state?.packStats as Array<{ packSize?: number; indexSize?: number }>) || [];
  for (const pack of packStats) {
    if (typeof pack.packSize === "number") totalStorageBytes += pack.packSize;
    if (typeof pack.indexSize === "number") totalStorageBytes += pack.indexSize;
  }
  const storageSize = formatSize(totalStorageBytes);
  const packList: string[] = Array.isArray(state?.packList) ? (state!.packList as string[]) : [];
  const packCount =
    typeof state?.packListCount === "number" ? (state!.packListCount as number) : packList.length;
  const hydrationPackCount =
    typeof state?.hydrationPackCount === "number"
      ? (state!.hydrationPackCount as number)
      : packList.filter((p) => typeof p === "string" && p.includes("pack-hydr-")).length;
  return { storageSize, packCount, packList, hydrationPackCount };
}

function computeHydrationStatus(
  hydrationData: HydrationData | undefined,
  packCount: number,
  hydrationPackCount: number
): { hydrationStatus: string; hydrationStartedAt: string | null } {
  let hydrationStatus = "Not Started";
  let hydrationStartedAt: string | null = null;
  if (hydrationData?.running) {
    hydrationStatus = `Running: ${hydrationData.stage || "unknown"}`;
    if (hydrationData.startedAt) {
      try {
        hydrationStartedAt = new Date(hydrationData.startedAt).toLocaleString();
      } catch {}
    }
  } else if (hydrationData?.stage === "done") {
    hydrationStatus = "Completed";
  } else if (hydrationData && hydrationData.queued > 0) {
    hydrationStatus = `Queued (${hydrationData.queued} pending)`;
  } else if (packCount > 0 && hydrationPackCount > 0) {
    hydrationStatus = "Completed (hydration packs present)";
  }
  return { hydrationStatus, hydrationStartedAt };
}

function computeNextMaintenance(
  env: Env,
  lastMaintenanceMs?: number
): { nextMaintenanceIn?: string; nextMaintenanceAt?: string } {
  try {
    const cfg = getConfig(env);
    const now = Date.now();
    const last = typeof lastMaintenanceMs === "number" ? lastMaintenanceMs : undefined;
    const nextAt = (last ?? now) + cfg.maintMs;
    const clamped = nextAt <= now ? now + cfg.maintMs : nextAt;
    return {
      nextMaintenanceIn: formatFromNowShort(clamped - now),
      nextMaintenanceAt: new Date(clamped).toLocaleString(),
    };
  } catch {
    return {};
  }
}

export function registerUiRoutes(router: ReturnType<typeof AutoRouter>) {
  // Owner repos list
  router.get(`/:owner`, async (request, env: Env) => {
    const { owner } = request.params as { owner: string };
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
  });
  // Repo overview page
  router.get(`/:owner/:repo`, async (request, env, ctx) => {
    const { owner, repo } = request.params;
    if (!isValidOwnerRepo(owner) || !isValidOwnerRepo(repo)) {
      return badRequest(env, "Invalid owner/repo", "Owner or repo invalid", { owner, repo });
    }
    const repoId = repoKey(owner, repo);

    // Cache HEAD and refs for 60 seconds for branches, longer for tags
    const cacheKeyRefs = buildCacheKeyFrom(request, "/_cache/refs", {
      repo: repoId,
    });

    const refsData = await cacheOrLoadJSON<{ head: HeadInfo | undefined; refs: Ref[] }>(
      cacheKeyRefs,
      async () => {
        try {
          const result = await getHeadAndRefs(env, repoId);
          return { head: result.head, refs: result.refs };
        } catch {
          return null;
        }
      },
      60,
      ctx
    );
    const head: HeadInfo | undefined = refsData?.head;
    const refs: Ref[] = refsData?.refs || [];

    const defaultRef = head?.target || (refs[0]?.name ?? "refs/heads/main");
    const refShort = defaultRef.replace(/^refs\/(heads|tags)\//, "");
    const refEnc = encodeURIComponent(refShort);
    // Format branches and tags as structured data
    const branchesData = refs
      .filter((r) => r.name.startsWith("refs/heads/"))
      .map((b) => {
        const short = b.name.replace("refs/heads/", "");
        return {
          name: encodeURIComponent(short),
          displayName: short.length > 30 ? short.slice(0, 27) + "..." : short,
        };
      });
    const tagsData = refs
      .filter((r) => r.name.startsWith("refs/tags/"))
      .map((t) => {
        const short = t.name.replace("refs/tags/", "");
        return {
          name: encodeURIComponent(short),
          displayName: short.length > 30 ? short.slice(0, 27) + "..." : short,
        };
      });

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

    // Check unpacking progress (shared helper)
    const progress = await getUnpackProgress(env, repoId);

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
  });

  // Tree/Blob browser using query params: ?ref=<branch|tag|oid>&path=<path>
  router.get(`/:owner/:repo/tree`, async (request, env, ctx) => {
    const { owner, repo } = request.params;
    if (!isValidOwnerRepo(owner) || !isValidOwnerRepo(repo)) {
      return badRequest(env, "Invalid owner/repo", "Owner or repo invalid", { owner, repo });
    }
    const repoId = repoKey(owner, repo);
    const u = new URL(request.url);
    const ref = u.searchParams.get("ref") || "main";
    const path = u.searchParams.get("path") || "";
    if (!isValidRef(ref)) {
      return badRequest(env, "Invalid ref", "Ref format not allowed", {
        owner,
        repo,
        refEnc: encodeURIComponent(ref),
        path,
      });
    }
    if (path && !isValidPath(path)) {
      return badRequest(env, "Invalid path", "Path contains invalid characters or is too long", {
        owner,
        repo,
        refEnc: encodeURIComponent(ref),
        path,
      });
    }

    // Build cache key for tree content
    const cacheKeyTree = buildCacheKeyFrom(request, "/_cache/tree", {
      repo: repoId,
      ref,
      path,
    });

    const result = await cacheOrLoadJSONWithTTL<ReadPathResult | null>(
      cacheKeyTree,
      async () => {
        try {
          const cacheCtx: CacheContext = { req: request, ctx };
          return await readPath(env, repoId, ref, path, cacheCtx);
        } catch {
          return null;
        }
      },
      (value) => (value && value.type === "tree" ? 60 : 300),
      ctx
    );

    // Handle missing tree/blob result gracefully (e.g., non-existent repo or path)
    if (!result) {
      try {
        const errHtml = await renderUiView(env, "error", {
          title: `${owner}/${repo} · Tree`,
          message: "Not found",
          owner,
          repo,
          refEnc: encodeURIComponent(ref),
          path,
        });
        if (errHtml) {
          return new Response(errHtml, {
            status: 404,
            headers: { "Content-Type": "text/html; charset=utf-8" },
          });
        }
      } catch {}
      return new Response("Not found\n", { status: 404 });
    }

    try {
      if (result.type === "tree") {
        // Format tree entries as structured data
        let entries: Array<{
          name: string;
          href: string;
          isDir: boolean;
          iconClass: string;
          shortOid: string;
          size: string;
        }> = [];
        if (result.type === "tree" && result.entries) {
          // Helper to determine if entry is a directory based on git mode
          const isDirectory = (mode: string) => mode.startsWith("40000");

          const sorted = result.entries.sort((a: TreeEntry, b: TreeEntry) => {
            const aIsDir = isDirectory(a.mode);
            const bIsDir = isDirectory(b.mode);
            if (aIsDir !== bIsDir) return aIsDir ? -1 : 1;
            return a.name.localeCompare(b.name);
          });
          entries = sorted.map((e: TreeEntry) => {
            const isDir = isDirectory(e.mode);
            return {
              name: e.name,
              href: isDir
                ? `/${owner}/${repo}/tree?ref=${encodeURIComponent(ref)}&path=${encodeURIComponent(
                    (path ? path + "/" : "") + e.name
                  )}`
                : `/${owner}/${repo}/blob?ref=${encodeURIComponent(ref)}&path=${encodeURIComponent(
                    (path ? path + "/" : "") + e.name
                  )}`,
              isDir,
              iconClass: isDir ? "bi-folder-fill" : getFileIconClass(e.name),
              shortOid: e.oid ? e.oid.slice(0, 7) : "",
              size: "", // Size not available in tree entries, would need separate lookup
            };
          });
        }
        // Generate breadcrumbs and parent link
        const parts = (path || "").split("/").filter(Boolean);
        // Truncate ref if too long (e.g., commit hashes)
        const refDisplay = ref.length > 20 ? ref.slice(0, 7) + "..." : ref;
        const breadcrumbs = [
          {
            name: refDisplay,
            href: parts.length > 0 ? `/${owner}/${repo}/tree?ref=${encodeURIComponent(ref)}` : null,
          },
          ...parts.map((part, i) => {
            const subPath = parts.slice(0, i + 1).join("/");
            const isLast = i === parts.length - 1;
            return {
              name: part,
              href: isLast
                ? null
                : `/${owner}/${repo}/tree?ref=${encodeURIComponent(ref)}&path=${encodeURIComponent(subPath)}`,
            };
          }),
        ];
        const parentHref =
          parts.length > 0
            ? `/${owner}/${repo}/tree?ref=${encodeURIComponent(ref)}&path=${encodeURIComponent(parts.slice(0, -1).join("/"))}`
            : null;
        const progress = await getUnpackProgress(env, repoId);
        const html = await renderUiView(env, "tree", {
          title: `${path || "root"} · ${owner}/${repo}`,
          owner,
          repo,
          refEnc: encodeURIComponent(ref),
          progress,
          breadcrumbs,
          parentHref,
          entries,
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
      } else {
        const raw = `/${owner}/${repo}/raw?oid=${encodeURIComponent(result.oid)}`;
        const text = bytesToText(result.content);
        const lineCount = text === "" ? 0 : text.split(/\r?\n/).length;
        const title = path || result.oid;
        // Infer language and load only what we need (use smart inference with content)
        const langs = getHighlightLangsForBlobSmart(title, text);
        const codeLang = langs[0] || null;
        const html = await renderUiView(env, "blob", {
          title: `${title} · ${owner}/${repo}`,
          owner,
          repo,
          refEnc: encodeURIComponent(ref),
          fileName: title,
          viewRawHref: `/${owner}/${repo}/raw?oid=${encodeURIComponent(result.oid)}&view=1&name=${encodeURIComponent(title)}`,
          rawHref: raw,
          codeText: text,
          codeLang,
          lineCount,
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
    } catch (e: any) {
      return handleError(env, e, `${owner}/${repo} · Tree`, {
        owner,
        repo,
        refEnc: encodeURIComponent(ref),
        path,
      });
    }
  });

  // Blob preview endpoint - renders file content with syntax highlighting and media previews
  router.get(`/:owner/:repo/blob`, async (request, env, ctx) => {
    const { owner, repo } = request.params;
    if (!isValidOwnerRepo(owner) || !isValidOwnerRepo(repo)) {
      return badRequest(env, "Invalid owner/repo", "Owner or repo invalid", { owner, repo });
    }
    const repoId = repoKey(owner, repo);
    const u = new URL(request.url);
    const ref = u.searchParams.get("ref") || "main";
    const path = u.searchParams.get("path") || "";
    if (!isValidRef(ref)) {
      return badRequest(env, "Invalid ref", "Ref format not allowed", {
        owner,
        repo,
        refEnc: encodeURIComponent(ref),
        path,
      });
    }
    if (path && !isValidPath(path)) {
      return badRequest(env, "Invalid path", "Path contains invalid characters or is too long", {
        owner,
        repo,
        refEnc: encodeURIComponent(ref),
        path,
      });
    }
    try {
      const cacheCtx: CacheContext = { req: request, ctx };
      const result = await readPath(env, repoId, ref, path, cacheCtx);
      if (result.type !== "blob") return new Response("Not a blob\n", { status: 400 });
      const fileName = path || result.oid;

      // Too large to render inline
      if (result.tooLarge) {
        const sizeStr = formatSize(result.size || 0);
        const viewRawHref = `/${owner}/${repo}/raw?oid=${encodeURIComponent(result.oid)}&view=1&name=${encodeURIComponent(fileName)}`;
        const rawHref = `/${owner}/${repo}/raw?oid=${encodeURIComponent(result.oid)}&download=1&name=${encodeURIComponent(fileName)}`;
        const html = await renderUiView(env, "blob", {
          title: `${fileName} · ${owner}/${repo}`,
          owner,
          repo,
          refEnc: encodeURIComponent(ref),
          fileName,
          tooLarge: true,
          sizeStr,
          viewRawHref,
          rawHref,
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

      // Binary vs text
      const isBinary = detectBinary(result.content);
      const size = result.content.byteLength;
      const viewRawHref = `/${owner}/${repo}/raw?oid=${encodeURIComponent(result.oid)}&view=1&name=${encodeURIComponent(fileName)}`;
      const rawHref = `/${owner}/${repo}/raw?oid=${encodeURIComponent(result.oid)}&download=1&name=${encodeURIComponent(fileName)}`;
      const templateData: Record<string, unknown> = {
        title: `${fileName} · ${owner}/${repo}`,
        owner,
        repo,
        refEnc: encodeURIComponent(ref),
        fileName,
        viewRawHref,
        rawHref,
      };

      if (isBinary) {
        const ext = (fileName.split(".").pop() || "").toLowerCase();
        const isImage = ["png", "jpg", "jpeg", "gif", "webp", "bmp", "ico", "svg"].includes(ext);
        const isPdf = ext === "pdf";
        if ((isImage || isPdf) && path) {
          const name = encodeURIComponent(fileName);
          const mediaSrc = `/${owner}/${repo}/rawpath?ref=${encodeURIComponent(ref)}&path=${encodeURIComponent(path)}&name=${name}`;
          templateData.isImage = isImage;
          templateData.isPdf = isPdf;
          templateData.mediaSrc = mediaSrc;
          templateData.sizeStr = formatSize(size);
        } else {
          templateData.isBinary = true;
          templateData.sizeStr = formatSize(size);
        }
      } else {
        const text = bytesToText(result.content);
        const lineCount = text === "" ? 0 : text.split(/\r?\n/).length;
        const isMd =
          fileName.toLowerCase().endsWith(".md") || fileName.toLowerCase().endsWith(".markdown");
        if (isMd) {
          const baseDir = (path || "").split("/").filter(Boolean).slice(0, -1).join("/");
          templateData.isMarkdown = true;
          templateData.markdownRaw = text;
          templateData.lineCount = lineCount;
          templateData.mdOwner = owner;
          templateData.mdRepo = repo;
          templateData.mdRef = ref;
          templateData.mdBase = baseDir;
        } else {
          const langs = getHighlightLangsForBlobSmart(fileName, text);
          const codeLang = langs[0] || null;
          templateData.codeText = text;
          templateData.codeLang = codeLang;
          templateData.lineCount = lineCount;
          if (!codeLang) {
            templateData.sizeStr = formatSize(size);
          }
        }
      }

      const html = await renderUiView(env, "blob", templateData);
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
        path,
      });
    }
  });

  // Commit list
  router.get(`/:owner/:repo/commits`, async (request, env, ctx) => {
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
      const progress = await getUnpackProgress(env, repoId);
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
  });

  // Merge expansion fragment endpoint: returns JSON for side-branch commits of a merge
  // Example: /:owner/:repo/commits/fragments/:oid?limit=20
  router.get(`/:owner/:repo/commits/fragments/:oid`, async (request, env, ctx) => {
    const { owner, repo, oid } = request.params as { owner: string; repo: string; oid: string };
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
  });

  // Commit details
  router.get(`/:owner/:repo/commit/:oid`, async (request, env, ctx) => {
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
      const when = c.author ? formatWhen(c.author.when, c.author.tz) : "";
      const parents = (c.parents || []).map((p) => ({ oid: p, short: p.slice(0, 7) }));
      const html = await renderUiView(env, "commit", {
        title: `${c.oid.slice(0, 7)} · ${owner}/${repo}`,
        owner,
        repo,
        refEnc: encodeURIComponent(c.oid),
        commitShort: c.oid.slice(0, 7),
        authorName: c.author?.name || "",
        authorEmail: c.author?.email || "",
        when,
        parents,
        treeShort: (c.tree || "").slice(0, 7),
        message: c.message || "",
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
  });

  // Raw blob endpoint - streams file content without buffering
  router.get(`/:owner/:repo/raw`, async (request: RouteRequest, env) => {
    const { owner, repo } = request.params;
    if (!isValidOwnerRepo(owner) || !isValidOwnerRepo(repo)) {
      return new Response("Bad Request\n", { status: 400 });
    }
    const url = new URL(request.url);
    const oid = url.searchParams.get("oid") || "";
    if (!OID_RE.test(oid)) return new Response("Bad Request\n", { status: 400 });
    const fileName = url.searchParams.get("name") || oid;
    const download = url.searchParams.get("download") === "1";

    if (!oid) return new Response("Missing oid\n", { status: 400 });

    // Use streaming version to avoid buffering entire file in memory
    const streamResponse = await readBlobStream(env, repoKey(owner, repo), oid);
    if (!streamResponse) return new Response("Not found\n", { status: 404 });

    // Use text/plain for all files (like GitHub's raw view)
    // This prevents browser from executing HTML/JS and ensures consistent display
    const headers = new Headers(streamResponse.headers);
    headers.set("Content-Type", "text/plain; charset=utf-8");

    if (download) {
      headers.set("Content-Disposition", `attachment; filename="${fileName}"`);
    } else {
      headers.set("Content-Disposition", `inline; filename="${fileName}"`);
    }

    return new Response(streamResponse.body, {
      status: streamResponse.status,
      headers,
    });
  });

  // Raw blob by ref+path (used for images in Markdown)
  router.get(`/:owner/:repo/rawpath`, async (request: RouteRequest, env, ctx) => {
    const { owner, repo } = request.params;
    if (!isValidOwnerRepo(owner) || !isValidOwnerRepo(repo)) {
      return new Response("Bad Request\n", { status: 400 });
    }
    const url = new URL(request.url);
    const ref = url.searchParams.get("ref") || "main";
    const path = url.searchParams.get("path") || "";
    const name = url.searchParams.get("name") || path.split("/").pop() || "file";
    const download = url.searchParams.get("download") === "1";
    if (!isValidRef(ref) || !isValidPath(path)) {
      return new Response("Bad Request\n", { status: 400 });
    }

    // Basic hotlink protection: require same-origin Referer
    try {
      const referer = request.headers.get("referer") || "";
      const allowed = (() => {
        try {
          const r = new URL(referer);
          return r.host === url.host;
        } catch {
          return false;
        }
      })();
      if (!allowed) {
        return new Response("Hotlinking not allowed\n", { status: 403 });
      }
    } catch {}

    try {
      const repoId = repoKey(owner, repo);
      const cacheCtx: CacheContext = { req: request, ctx };
      const result = await readPath(env, repoId, ref, path, cacheCtx);
      if (result.type !== "blob") return new Response("Not a blob\n", { status: 400 });
      const streamResponse = await readBlobStream(env, repoId, result.oid);
      if (!streamResponse) return new Response("Not found\n", { status: 404 });

      const headers = new Headers(streamResponse.headers);
      headers.set("Content-Type", getContentTypeFromName(name));
      if (download) headers.set("Content-Disposition", `attachment; filename="${name}"`);
      else headers.set("Content-Disposition", `inline; filename="${name}"`);
      return new Response(streamResponse.body, { status: streamResponse.status, headers });
    } catch (e: any) {
      return new Response("Not found\n", { status: 404 });
    }
  });

  // Async refs API for repo_nav dropdown
  router.get(`/:owner/:repo/api/refs`, async (request, env: Env, ctx: ExecutionContext) => {
    const { owner, repo } = request.params;
    if (!isValidOwnerRepo(owner) || !isValidOwnerRepo(repo)) {
      return new Response(JSON.stringify({ branches: [], tags: [] }), {
        status: 400,
        headers: { "Content-Type": "application/json" },
      });
    }
    const repoId = repoKey(owner, repo);
    try {
      const cacheKey = buildCacheKeyFrom(request, "/_cache/refs", { repo: repoId });
      const refsData = await cacheOrLoadJSON<{ refs: Ref[] }>(
        cacheKey,
        async () => {
          try {
            const result = await getHeadAndRefs(env, repoId);
            return { refs: result.refs };
          } catch {
            return null;
          }
        },
        60,
        ctx
      );
      const refs: Ref[] = refsData?.refs || [];
      const branches = refs
        .filter((r: Ref) => r.name && r.name.startsWith("refs/heads/"))
        .map((b: Ref) => {
          const short = b.name.replace("refs/heads/", "");
          return {
            name: encodeURIComponent(short),
            displayName: short.length > 30 ? short.slice(0, 27) + "..." : short,
          };
        });
      const tags = refs
        .filter((r: Ref) => r.name && r.name.startsWith("refs/tags/"))
        .map((t: Ref) => {
          const short = t.name.replace("refs/tags/", "");
          return {
            name: encodeURIComponent(short),
            displayName: short.length > 30 ? short.slice(0, 27) + "..." : short,
          };
        });
      return new Response(JSON.stringify({ branches, tags }), {
        headers: {
          "Content-Type": "application/json",
          "Cache-Control": "public, max-age=60",
        },
      });
    } catch (e: any) {
      return new Response(
        JSON.stringify({ branches: [], tags: [], error: String(e?.message || e) }),
        {
          status: 500,
          headers: { "Content-Type": "application/json" },
        }
      );
    }
  });

  // Admin dashboard for repository management
  router.get(`/:owner/:repo/admin`, async (request, env: Env, ctx: ExecutionContext) => {
    const { owner, repo } = request.params;

    // Validate parameters
    if (!isValidOwnerRepo(owner) || !isValidOwnerRepo(repo)) {
      return badRequest(env, "Invalid owner/repo", "Owner or repo invalid", { owner, repo });
    }

    // Check authentication - admin access required
    if (!(await verifyAuth(env, owner, request, true))) {
      return new Response("Unauthorized\n", {
        status: 401,
        headers: { "WWW-Authenticate": 'Basic realm="Git Admin", charset="UTF-8"' },
      });
    }

    const repoId = repoKey(owner, repo);
    const stub = getRepoStub(env, repoId);

    // Gather admin data in parallel for performance
    const [state, refsData, progress] = await Promise.all([
      stub.debugState().catch(() => ({}) as Partial<DebugState>),
      loadHeadAndRefsCached(env, request, ctx, repoId),
      getUnpackProgress(env, repoId),
    ]);

    const head: HeadInfo | undefined = refsData?.head || undefined;
    const refs: Ref[] = refsData?.refs || [];

    const { storageSize, packCount, packList, hydrationPackCount } = computeStorageMetrics(state);
    const { hydrationStatus, hydrationStartedAt } = computeHydrationStatus(
      state.hydration,
      packCount,
      hydrationPackCount
    );

    const defaultBranch = getDefaultBranchFromHead(head);
    const refEnc = encodeURIComponent(defaultBranch);

    const { nextMaintenanceIn, nextMaintenanceAt } = computeNextMaintenance(
      env,
      typeof state?.lastMaintenanceMs === "number"
        ? (state!.lastMaintenanceMs as number)
        : undefined
    );

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
      hydrationStatus,
      hydrationStartedAt,
      hydrationData: state.hydration,
      hydrationPackCount,
      progress,
      nextMaintenanceIn,
      nextMaintenanceAt,
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
  });
}
