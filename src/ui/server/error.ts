import { renderUiView } from "@/ui/server/render";
import { escapeHtml } from "@/web/format";
import { isHttpError } from "@/web/http";

export async function handleError(
  env: Env,
  error: unknown,
  fallbackTitle: string,
  extra?: {
    owner?: string;
    repo?: string;
    refEnc?: string;
    path?: string;
  }
): Promise<Response> {
  const debug = String(env.LOG_LEVEL || "").toLowerCase() === "debug";
  const httpError = isHttpError(error) ? error : undefined;
  const status =
    httpError?.status ?? (/not found/i.test(String((error as any)?.message)) ? 404 : 500);

  try {
    const html = await renderUiView(env, "error", {
      title: fallbackTitle,
      message: String((error as any)?.message || error),
      owner: extra?.owner,
      repo: extra?.repo,
      refEnc: extra?.refEnc,
      path: extra?.path,
      stack: debug ? String((error as any)?.stack || "") : undefined,
    });
    if (html) {
      return new Response(html, {
        status,
        headers: { "Content-Type": "text/html; charset=utf-8" },
      });
    }
  } catch {}

  return new Response(
    `<h2>Error</h2><pre>${escapeHtml(String((error as any)?.message || error))}</pre>`,
    {
      status,
      headers: { "Content-Type": "text/html; charset=utf-8" },
    }
  );
}
