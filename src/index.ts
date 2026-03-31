import { AutoRouter } from "itty-router";
import { renderUiView } from "@/client/server/render";
import { registerGitRoutes } from "./routes/git";
import { registerAdminRoutes } from "./routes/admin";
import { registerUiRoutes } from "./routes/ui";
import { registerAuthRoutes } from "./routes/auth";
import { handleRepoMaintenanceQueue, type RepoMaintenanceQueueMessage } from "./maintenance/queue";

// Router setup with itty-router AutoRouter
const router = AutoRouter();
// Register Git protocol routes (info/refs, upload-pack, receive-pack)
registerGitRoutes(router);
// Register Admin routes
registerAdminRoutes(router);
// Register Auth routes BEFORE UI to avoid /:owner shadowing /auth
registerAuthRoutes(router);

router.get("/", async (_request, env: Env) => {
  const html = await renderUiView(env, "home", {});
  if (html) {
    return new Response(html, {
      headers: {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-store, no-cache, must-revalidate",
        "X-Page-Renderer": "react-ssr",
      },
    });
  }
  return new Response("Failed to render page\n", { status: 500 });
});

// Register UI routes AFTER static/auth so that /:owner doesn't shadow them
registerUiRoutes(router);

// Catch-all 404
router.all("*", async (_request, env: Env) => {
  const html = await renderUiView(env, "404", {});
  if (html) {
    return new Response(html, {
      status: 404,
      headers: {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-store, no-cache, must-revalidate",
        "X-Page-Renderer": "react-ssr",
      },
    });
  }
  return new Response("Not found\n", { status: 404 });
});

export default {
  fetch(request: Request, env: Env, ctx: ExecutionContext) {
    return router.fetch(request, env, ctx);
  },
  async queue(batch: MessageBatch<RepoMaintenanceQueueMessage>, env: Env, ctx: ExecutionContext) {
    return await handleRepoMaintenanceQueue(batch, env, ctx);
  },
};

export { RepoDurableObject } from "./do/repo/repoDO";
export { AuthDurableObject } from "./do/auth/authDO";
