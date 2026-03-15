import hljs from "highlight.js";

import { escapeHtml } from "@/web/format.ts";

type HighlightedCode = {
  html: string;
  languageClass: string;
};

const SPAN_TAG_RE = /<\/?span\b[^>]*>/gi;

function escapeAttribute(value: string): string {
  return escapeHtml(value).replace(/'/g, "&#39;");
}

export function highlightCode(code: string, language?: string | null): HighlightedCode {
  const normalizedLanguage = language && hljs.getLanguage(language) ? language : null;

  return {
    html: normalizedLanguage
      ? hljs.highlight(code, { language: normalizedLanguage, ignoreIllegals: true }).value
      : escapeHtml(code),
    languageClass: normalizedLanguage
      ? `language-${escapeAttribute(normalizedLanguage)}`
      : "language-plaintext",
  };
}

export function splitHighlightedCodeIntoLines(
  code: string,
  language?: string | null
): HighlightedCode & { lines: string[] } {
  const highlighted = highlightCode(code, language);
  if (highlighted.html === "") {
    return {
      ...highlighted,
      lines: [],
    };
  }

  const rawLines = highlighted.html.split("\n");

  if (rawLines.length > 1 && rawLines[rawLines.length - 1] === "") {
    rawLines.pop();
  }

  const openTags: string[] = [];
  const lines = rawLines.map((line) => {
    const prefix = openTags.join("");
    SPAN_TAG_RE.lastIndex = 0;

    let match: RegExpExecArray | null = null;
    while ((match = SPAN_TAG_RE.exec(line)) !== null) {
      if (match[0][1] === "/") {
        openTags.pop();
      } else {
        openTags.push(match[0]);
      }
    }

    const suffix = openTags
      .slice()
      .reverse()
      .map(() => "</span>")
      .join("");
    const html = `${prefix}${line}${suffix}`;
    return html || "&nbsp;";
  });

  return {
    ...highlighted,
    lines,
  };
}
