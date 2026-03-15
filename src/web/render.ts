import { renderView } from "./templates";
export { renderView };

/**
 * Render a full HTML page using the inline wrapper
 */
export async function renderPage(
  env: Env,
  req: Request | undefined,
  title: string,
  bodyHtml: string
): Promise<Response> {
  // Only use an inline wrapper as a last-resort fallback.
  return new Response(
    `<!DOCTYPE html><html class="h-full"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/><title>${title}</title><link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin><link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet"><link rel="stylesheet" href="/dist/app.css"></head><body class="min-h-screen text-zinc-900 dark:text-zinc-100"><div class="relative z-10 min-h-screen flex flex-col"><main class="flex-1 container py-6 animate-fade-in">${bodyHtml}</main></div></body></html>`,
    {
      headers: {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-store, no-cache, must-revalidate",
        "X-Page-Renderer": "inline",
      },
    }
  );
}
