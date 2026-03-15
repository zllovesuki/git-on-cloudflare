import type { HeadInfo, Ref } from "@/git";

import { AutoRouter } from "itty-router";
import {
  capabilityAdvertisement,
  parseV2Command,
  handleFetchV2,
  pktLine,
  flushPkt,
  concatChunks,
  getHeadAndRefs,
  inflateAndParseHeader,
  parseTagTarget,
} from "@/git";
import { handleFetchV2Streaming } from "@/git/operations/uploadStream.ts";
import { asBodyInit, getRepoStub } from "@/common";
import { repoKey } from "@/keys";
import { verifyAuth } from "@/auth";
import { addRepoToOwner, removeRepoFromOwner } from "@/registry";
import { buildCacheKeyFrom, cacheOrLoadJSON } from "@/cache";

/**
 * Handles Git upload-pack (fetch) POST requests.
 * Supports both protocol v2 and legacy protocol based on Git-Protocol header.
 * @param env - Worker environment
 * @param repoId - Repository identifier (owner/repo)
 * @param request - Incoming HTTP request
 * @returns Response with pack data or error
 */
async function handleUploadPackPOST(
  env: Env,
  repoId: string,
  request: Request,
  ctx: ExecutionContext
) {
  const body = new Uint8Array(await request.arrayBuffer());
  const gitProto = request.headers.get("Git-Protocol") || "";
  const { command } = parseV2Command(body);
  // Accept either explicit v2 header or a v2-formatted body (contains command=...)
  if (!/version=2/.test(gitProto) && !command) {
    return new Response("Expected Git protocol v2 (set Git-Protocol: version=2)\n", {
      status: 400,
    });
  }

  if (command === "ls-refs") {
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
    const { head, refs } = refsData || { refs: [] };

    // Parse ls-refs arguments (reuse already-read body to avoid double-read of the stream)
    const { args } = parseV2Command(body);
    const refPrefixes: string[] = [];
    let wantPeel = false;
    let wantSymrefs = false;
    for (const a of args) {
      if (a === "peel") wantPeel = true;
      else if (a === "symrefs") wantSymrefs = true;
      else if (a.startsWith("ref-prefix ")) refPrefixes.push(a.slice("ref-prefix ".length));
    }

    // Filter refs by ref-prefix when provided
    let filteredRefs = refs;
    if (refPrefixes.length > 0) {
      filteredRefs = refs.filter((r) => refPrefixes.some((p) => r.name.startsWith(p)));
    }

    // Optional peel of annotated tags
    let peeledByRef = new Map<string, string>();
    if (wantPeel) {
      try {
        const tagRefs = filteredRefs.filter((r) => r.name.startsWith("refs/tags/"));
        if (tagRefs.length > 0) {
          const stub = getRepoStub(env, repoId);
          const oids = tagRefs.map((r) => r.oid);
          const objMap = (await stub.getObjectsBatch(oids)) as Map<string, Uint8Array | null>;
          for (const r of tagRefs) {
            const z = objMap.get(r.oid);
            if (!z) continue;
            const parsed = await inflateAndParseHeader(
              z instanceof Uint8Array ? z : new Uint8Array(z)
            );
            if (!parsed) continue;
            if (parsed.type === "tag") {
              const peeled = parseTagTarget(parsed.payload);
              if (peeled?.targetOid) peeledByRef.set(r.name, peeled.targetOid);
            }
          }
        }
      } catch {}
    }

    const chunks: Uint8Array[] = [];
    // Place HEAD first if available; include symref-target attribute (kept for compatibility)
    if (head && head.target) {
      const t =
        filteredRefs.find((r) => r.name === head.target) ||
        refs.find((r) => r.name === head.target);
      const headOid = head.oid ?? t?.oid;
      const headLineAttrs: string[] = [];
      // Historically we emitted symref-target unconditionally; retain for compatibility
      headLineAttrs.push(`symref-target:${head.target}`);
      if (headOid) {
        const base = [`${headOid} HEAD`, ...headLineAttrs].join(" ");
        chunks.push(pktLine(base + "\n"));
      } else {
        const base = ["unborn HEAD", ...headLineAttrs].join(" ");
        chunks.push(pktLine(base + "\n"));
      }
    }

    for (const r of filteredRefs) {
      const attrs: string[] = [];
      // Only include peeled attribute when requested and available
      if (wantPeel) {
        const peeled = peeledByRef.get(r.name);
        if (peeled) attrs.push(`peeled:${peeled}`);
      }
      const line =
        attrs.length > 0 ? `${r.oid} ${r.name} ${attrs.join(" ")}` : `${r.oid} ${r.name}`;
      chunks.push(pktLine(line + "\n"));
    }
    chunks.push(flushPkt());
    return new Response(asBodyInit(concatChunks(chunks)), {
      status: 200,
      headers: {
        "Content-Type": "application/x-git-upload-pack-result",
        "Cache-Control": "no-cache",
      },
    });
  }

  if (command === "fetch") {
    // Use streaming by default, allow opt-out with X-Git-Streaming: false
    // The buffered implementation is deprecated and will be removed in a future version
    const forceBuffered = request.headers.get("X-Git-Streaming") === "false";

    if (forceBuffered) {
      // Deprecated: buffered mode is only kept for emergency fallback
      return handleFetchV2(env, repoId, body, request.signal, { req: request, ctx });
    } else {
      return handleFetchV2Streaming(env, repoId, body, request.signal, { req: request, ctx });
    }
  }

  return new Response("Unsupported command or malformed request\n", { status: 400 });
}

/**
 * Handles Git receive-pack (push) POST requests.
 * Forwards the request to the repository Durable Object and updates owner registry.
 * @param env - Worker environment
 * @param repoId - Repository identifier (owner/repo)
 * @param request - Incoming HTTP request with push data
 * @returns Response with receive-pack result
 */
async function handleReceivePackPOST(env: Env, repoId: string, request: Request) {
  // Forward raw body to the Durable Object /receive endpoint
  const stub = getRepoStub(env, repoId);
  // Preflight: if the DO is currently unpacking and a one-deep queue is already occupied,
  // return 503 early so clients can retry without uploading the whole pack.
  try {
    const j = await stub.getUnpackProgress();
    const queued = j.queuedCount || 0;
    if (j.unpacking === true && queued >= 1) {
      return new Response("Repository is busy unpacking; please retry shortly.\n", {
        status: 503,
        headers: {
          "Retry-After": "10",
          "Content-Type": "text/plain; charset=utf-8",
        },
      });
    }
  } catch {}
  const ct = request.headers.get("Content-Type") || "application/x-git-receive-pack-request";
  const res = await stub.fetch("https://do/receive", {
    method: "POST",
    body: request.body,
    headers: { "Content-Type": ct },
    signal: request.signal,
  });
  // Proxy DO response through
  const headers = new Headers(res.headers);
  if (!headers.has("Content-Type"))
    headers.set("Content-Type", "application/x-git-receive-pack-result");
  headers.set("Cache-Control", "no-cache");
  // Update owner registry on change signal
  try {
    const changed = res.headers.get("X-Repo-Changed") === "1";
    if (changed) {
      const empty = res.headers.get("X-Repo-Empty") === "1";
      const [owner, repo] = repoId.split("/", 2);
      if (owner && repo) {
        if (empty) await removeRepoFromOwner(env, owner, repo);
        else await addRepoToOwner(env, owner, repo);
      }
    }
  } catch {}
  return new Response(res.body, { status: res.status, headers });
}

/**
 * Registers Git Smart HTTP v2 routes on the router.
 * Sets up handlers for info/refs, upload-pack, and receive-pack endpoints.
 * @param router - The application router instance
 */
export function registerGitRoutes(router: ReturnType<typeof AutoRouter>) {
  // Git info/refs
  router.get(`/:owner/:repo/info/refs`, async (request, env: Env) => {
    const u = new URL(request.url);
    const service = u.searchParams.get("service");
    if (service === "git-upload-pack" || service === "git-receive-pack") {
      const { owner, repo } = request.params;
      return await capabilityAdvertisement(env, service, repoKey(owner, repo));
    }
    return new Response("Missing or unsupported service\n", { status: 400 });
  });

  // git-upload-pack (POST)
  router.post(`/:owner/:repo/git-upload-pack`, async (request, env: Env, ctx: ExecutionContext) => {
    const { owner, repo } = request.params;
    return handleUploadPackPOST(env, repoKey(owner, repo), request, ctx);
  });

  // git-receive-pack (POST)
  router.post(`/:owner/:repo/git-receive-pack`, async (request, env: Env) => {
    const { owner, repo } = request.params;
    if (!(await verifyAuth(env, owner, request, false))) {
      return new Response("Unauthorized\n", {
        status: 401,
        headers: { "WWW-Authenticate": 'Basic realm="Git", charset="UTF-8"' },
      });
    }
    return handleReceivePackPOST(env, repoKey(owner, repo), request);
  });
}
