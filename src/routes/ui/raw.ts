import type { CacheContext } from "@/cache";
import { readBlobStream, readPath } from "@/git";
import { isValidOwnerRepo, isValidRef, isValidPath, OID_RE, getContentTypeFromName } from "@/web";
import { repoKey } from "@/keys";
import type { RouteRequest } from "./helpers";

export async function handleRaw(request: RouteRequest, env: Env, ctx: ExecutionContext) {
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

  // This route still avoids whole-pack buffering, but a packed blob may be
  // materialized before the response body is streamed to the client.
  const cacheCtx: CacheContext | undefined = ctx ? { req: request, ctx } : undefined;
  const streamResponse = await readBlobStream(env, repoKey(owner, repo), oid, cacheCtx);
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
}

export async function handleRawPath(request: RouteRequest, env: Env, ctx: ExecutionContext) {
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
    const streamResponse = await readBlobStream(env, repoId, result.oid, cacheCtx);
    if (!streamResponse) return new Response("Not found\n", { status: 404 });

    const headers = new Headers(streamResponse.headers);
    headers.set("Content-Type", getContentTypeFromName(name));
    if (download) headers.set("Content-Disposition", `attachment; filename="${name}"`);
    else headers.set("Content-Disposition", `inline; filename="${name}"`);
    return new Response(streamResponse.body, { status: streamResponse.status, headers });
  } catch (e: any) {
    return new Response("Not found\n", { status: 404 });
  }
}
