import type { CacheContext } from "@/cache";
import { readPath } from "@/git";
import {
  isValidOwnerRepo,
  isValidRef,
  isValidPath,
  formatSize,
  detectBinary,
  bytesToText,
  getHighlightLangsForBlobSmart,
} from "@/web";
import { renderUiView } from "@/client/server/render";
import { handleError } from "@/client/server/error";
import { repoKey } from "@/keys";
import { badRequest } from "./helpers";
import type { RouteRequest } from "./helpers";

export async function handleBlob(request: RouteRequest, env: Env, ctx: ExecutionContext) {
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

    // Generate breadcrumbs and parent link (same pattern as tree.ts)
    const parts = (path || "").split("/").filter(Boolean);
    const refDisplay = ref.length > 20 ? ref.slice(0, 7) + "..." : ref;
    const breadcrumbs = [
      {
        name: refDisplay,
        href: parts.length > 0 ? `/${owner}/${repo}/tree?ref=${encodeURIComponent(ref)}` : null,
      },
      ...parts.map((part: string, i: number) => {
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
      parts.length > 1
        ? `/${owner}/${repo}/tree?ref=${encodeURIComponent(ref)}&path=${encodeURIComponent(parts.slice(0, -1).join("/"))}`
        : parts.length === 1
          ? `/${owner}/${repo}/tree?ref=${encodeURIComponent(ref)}`
          : null;

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
        breadcrumbs,
        parentHref,
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
      breadcrumbs,
      parentHref,
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
}
