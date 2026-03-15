import { Marked } from "marked";
import sanitize from "sanitize-html";

import { highlightCode } from "@/ui/components/highlight";
import { escapeHtml } from "@/web/format.ts";

type MarkdownContext = {
  owner: string;
  repo: string;
  ref: string;
  baseDir?: string;
};

type MarkdownContentProps = {
  markdown: string;
  context: MarkdownContext;
};

function isAbsoluteUrl(href: string): boolean {
  return /^(?:[a-z]+:)?\/\//i.test(href) || href.startsWith("/") || href.startsWith("#");
}

function joinRelativePath(baseDir: string, relativePath: string): string {
  const [pathPart, hash = ""] = relativePath.split("#", 2);
  const baseParts = baseDir.split("/").filter(Boolean);

  for (const part of pathPart.split("/").filter(Boolean)) {
    if (part === ".") {
      continue;
    }
    if (part === "..") {
      baseParts.pop();
      continue;
    }
    baseParts.push(part);
  }

  const joined = baseParts.join("/");
  return hash ? `${joined}#${hash}` : joined;
}

function rewriteMarkdownHref(href: string, context: MarkdownContext): string {
  if (!href || isAbsoluteUrl(href)) {
    return href;
  }

  const hashIndex = href.indexOf("#");
  const fragment = hashIndex >= 0 ? href.slice(hashIndex) : "";
  const resolved = joinRelativePath(
    context.baseDir || "",
    hashIndex >= 0 ? href.slice(0, hashIndex) : href
  );

  return `/${encodeURIComponent(context.owner)}/${encodeURIComponent(context.repo)}/blob?ref=${encodeURIComponent(context.ref)}&path=${encodeURIComponent(resolved)}${fragment}`;
}

function rewriteMarkdownImage(href: string, context: MarkdownContext): string {
  if (!href || isAbsoluteUrl(href)) {
    return href;
  }

  const resolved = joinRelativePath(context.baseDir || "", href);
  const name = resolved.split("/").pop() || "file";
  return `/${encodeURIComponent(context.owner)}/${encodeURIComponent(context.repo)}/rawpath?ref=${encodeURIComponent(context.ref)}&path=${encodeURIComponent(resolved)}&name=${encodeURIComponent(name)}`;
}

function escapeAttribute(value: string): string {
  return escapeHtml(value).replace(/'/g, "&#39;");
}

function highlightBlock(code: string, language?: string | null): string {
  const highlighted = highlightCode(code, language);
  return `<pre class="markdown-code-block"><code class="hljs ${highlighted.languageClass}">${highlighted.html}</code></pre>`;
}

function renderMarkdown(markdown: string, context: MarkdownContext): string {
  const marked = new Marked({ gfm: true, async: false });

  marked.use({
    renderer: {
      code({ text, lang }) {
        return highlightBlock(text, lang || null);
      },
      link({ href, title, tokens }) {
        const content = this.parser.parseInline(tokens);
        const resolvedHref = rewriteMarkdownHref(href || "", context);
        const titleAttr = title ? ` title="${escapeAttribute(title)}"` : "";
        return `<a href="${escapeAttribute(resolvedHref)}"${titleAttr}>${content}</a>`;
      },
      image({ href, title, text }) {
        const resolvedHref = rewriteMarkdownImage(href || "", context);
        const alt = escapeAttribute(text || "");
        const titleAttr = title ? ` title="${escapeAttribute(title)}"` : "";
        return `<img src="${escapeAttribute(resolvedHref)}" alt="${alt}"${titleAttr} loading="lazy" />`;
      },
    },
  });

  // async: false is set in the constructor, so parse() returns string synchronously
  const raw = marked.parse(markdown) as string;

  return sanitize(raw, {
    allowedTags: sanitize.defaults.allowedTags.concat([
      "img",
      "details",
      "summary",
      "del",
      "ins",
      "sup",
      "sub",
      "kbd",
      "abbr",
    ]),
    allowedAttributes: {
      ...sanitize.defaults.allowedAttributes,
      a: ["href", "title", "name", "target", "rel"],
      img: ["src", "alt", "title", "loading"],
      code: ["class"],
      span: ["class"],
      pre: ["class"],
      td: ["align"],
      th: ["align"],
    },
    allowedClasses: {
      code: ["hljs", "language-*"],
      span: ["hljs-*"],
      pre: ["markdown-code-block"],
    },
  });
}

export function MarkdownContent({ markdown, context }: MarkdownContentProps) {
  const html = renderMarkdown(markdown, context);
  return <div className="markdown-content" dangerouslySetInnerHTML={{ __html: html }} />;
}
