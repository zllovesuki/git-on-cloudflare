import { renderToReadableStream } from "react-dom/server";

import { AppLayout } from "@/ui/components/AppLayout";
import { resolveDocumentAssets } from "./assets";
import { Document } from "./document";
import { getViewDefinition } from "./registry";

function needsHighlightTheme(name: string, data: Record<string, unknown>): boolean {
  return (
    name === "blob" ||
    (name === "overview" && typeof data.readmeMd === "string" && data.readmeMd.length > 0)
  );
}

export async function renderUiView(
  env: Env,
  name: string,
  data: Record<string, unknown>
): Promise<BodyInit | null> {
  const definition = getViewDefinition(name);
  if (!definition) {
    return null;
  }

  const page = definition.render(data);
  if (definition.kind === "fragment") {
    // Fragments don't need a full document wrapper — React won't emit a doctype
    return renderToReadableStream(page);
  }

  const assets = await resolveDocumentAssets(env);
  const element = (
    <Document
      title={(data.title as string | undefined) || definition.title}
      assets={assets}
      needsHighlight={needsHighlightTheme(name, data)}
    >
      <AppLayout>{page}</AppLayout>
    </Document>
  );

  // React's renderToReadableStream automatically emits <!DOCTYPE html>
  // when the root element is <html>
  return renderToReadableStream(element);
}
