import { AutoRouter } from "itty-router";
import { renderPage, renderView } from "./web";
import { registerGitRoutes } from "./routes/git";
import { registerAdminRoutes } from "./routes/admin";
import { registerUiRoutes } from "./routes/ui";
import { registerAuthRoutes } from "./routes/auth";

// Router setup with itty-router AutoRouter
const router = AutoRouter();
// Register Git protocol routes (info/refs, upload-pack, receive-pack)
registerGitRoutes(router);
// Register Admin routes
registerAdminRoutes(router);
// Register Auth routes BEFORE UI to avoid /:owner shadowing /auth
registerAuthRoutes(router);

router.get("/", async (request, env: Env) => {
  const html = await renderView(env, "home", {});
  if (html) {
    return new Response(html, {
      headers: {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-store, no-cache, must-revalidate",
        "X-Page-Renderer": "liquid-layout",
      },
    });
  }
  const body = `<h1>git-on-cloudflare</h1><p>Smart HTTP v2 skeleton running. Try <code>/:owner/:repo/info/refs?service=git-upload-pack</code>.</p>`;
  return renderPage(env, request, "git-on-cloudflare", body);
});

// Register UI routes AFTER static/auth so that /:owner doesn't shadow them
registerUiRoutes(router);

// Catch-all 404
router.all("*", async (request, env: Env) => {
  const html = await renderView(env, "404", {});
  if (html) {
    return new Response(html, {
      status: 404,
      headers: {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-store, no-cache, must-revalidate",
        "X-Page-Renderer": "liquid-layout",
      },
    });
  }
  const body = `<h1>Not Found</h1><p class="muted">The page you are looking for doesn't exist.</p><p><a class="btn" href="/">Go home</a></p>`;
  const base = await renderPage(env, request, "404 · git-on-cloudflare", body);
  // Wrap to set proper 404 status while preserving headers and body
  return new Response(base.body, { status: 404, headers: base.headers });
});

export default {
  fetch(request: Request, env: Env, ctx: ExecutionContext) {
    return router.fetch(request, env, ctx);
  },
};

export { RepoDurableObject } from "./do/repo/repoDO";
export { AuthDurableObject } from "./do/auth/authDO";
