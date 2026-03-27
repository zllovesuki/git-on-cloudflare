/// <reference lib="dom" />

import { useState } from "react";
import { Clipboard, Download, FileText, Menu } from "lucide-react";

import { hydrateIsland } from "@/ui/client/hydrate";
import { Button, buttonClasses } from "@/ui/components/ui/button";

export type BlobActionsProps = {
  viewRawHref: string;
  rawHref: string;
  showCopy: boolean;
  isImage?: boolean;
  isPdf?: boolean;
};

export function BlobActionsIsland({
  viewRawHref,
  rawHref,
  showCopy,
  isImage,
  isPdf,
}: BlobActionsProps) {
  const [copyLabel, setCopyLabel] = useState("Copy");

  async function copyRawText() {
    try {
      const response = await fetch(viewRawHref);
      const text = await response.text();
      await navigator.clipboard.writeText(text);
      setCopyLabel("Copied");
      window.setTimeout(() => setCopyLabel("Copy"), 1200);
    } catch (error) {
      console.warn("Copy failed", error);
    }
  }

  return (
    <div className="flex items-center gap-2">
      <div className="hidden items-center gap-2 sm:flex">
        <div className="inline-flex items-center overflow-hidden rounded-full border border-zinc-200 dark:border-zinc-700 bg-white dark:bg-zinc-900">
          {showCopy ? (
            <button
              className="px-3 py-1.5 text-sm no-underline text-zinc-700 dark:text-zinc-200 hover:bg-zinc-100 dark:hover:bg-zinc-800/50"
              type="button"
              onClick={() => void copyRawText()}
            >
              {copyLabel}
            </button>
          ) : null}
          {!isImage && !isPdf ? (
            <a
              className="border-l border-zinc-200 dark:border-zinc-700 px-3 py-1.5 text-sm no-underline text-zinc-700 dark:text-zinc-200 hover:bg-zinc-100 dark:hover:bg-zinc-800/50"
              href={viewRawHref}
            >
              View
            </a>
          ) : null}
          <span className="border-l border-zinc-200 dark:border-zinc-700 bg-zinc-100 dark:bg-zinc-800/50 px-3 py-1.5 text-sm text-zinc-500 dark:text-zinc-400">
            Raw
          </span>
        </div>
        <Button size="sm" href={rawHref}>
          Download
        </Button>
      </div>
      <div className="sm:hidden">
        <details className="ref-menu relative">
          <summary className={buttonClasses("secondary", "sm")}>
            <Menu className="h-4 w-4" aria-hidden="true" />
          </summary>
          <div className="fixed inset-x-0 z-20 mx-3 mt-2 rounded-xl border border-zinc-200 bg-white p-2 shadow-xl dark:border-zinc-800/60 dark:bg-zinc-900">
            <div className="flex flex-col">
              {showCopy ? (
                <button
                  type="button"
                  className="flex items-center gap-2 rounded-lg px-3 py-2 text-left hover:bg-zinc-100 dark:hover:bg-zinc-800"
                  onClick={() => void copyRawText()}
                >
                  <Clipboard className="h-4 w-4" aria-hidden="true" />
                  <span>{copyLabel === "Copy" ? "Copy raw" : "Copied"}</span>
                </button>
              ) : null}
              {!isImage && !isPdf ? (
                <a
                  href={viewRawHref}
                  className="flex items-center gap-2 rounded-lg px-3 py-2 hover:bg-zinc-100 dark:hover:bg-zinc-800"
                >
                  <FileText className="h-4 w-4" aria-hidden="true" />
                  <span>View raw</span>
                </a>
              ) : null}
              <a
                href={rawHref}
                className="flex items-center gap-2 rounded-lg px-3 py-2 hover:bg-zinc-100 dark:hover:bg-zinc-800"
              >
                <Download className="h-4 w-4" aria-hidden="true" />
                <span>Download</span>
              </a>
            </div>
          </div>
        </details>
      </div>
    </div>
  );
}

export function initBlobActions() {
  hydrateIsland<BlobActionsProps>("blob-actions", BlobActionsIsland);
}
