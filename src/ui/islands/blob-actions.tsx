/// <reference lib="dom" />

import { useState } from "react";

import { hydrateIsland } from "@/ui/client/hydrate";

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
        <div className="segmented">
          {showCopy ? (
            <button className="seg action" type="button" onClick={() => void copyRawText()}>
              {copyLabel}
            </button>
          ) : null}
          {!isImage && !isPdf ? (
            <a className="seg action" href={viewRawHref}>
              View
            </a>
          ) : null}
          <span className="seg label">Raw</span>
        </div>
        <a className="btn sm" href={rawHref}>
          Download
        </a>
      </div>
      <div className="sm:hidden">
        <details className="ref-menu relative">
          <summary className="btn secondary sm inline-flex items-center gap-2">
            <i className="bi bi-three-dots h-4 w-4" aria-hidden="true"></i>
          </summary>
          <div className="fixed inset-x-0 z-20 mx-3 mt-2 rounded-xl border border-zinc-200 bg-white p-2 shadow-xl dark:border-zinc-800/60 dark:bg-zinc-900">
            <div className="flex flex-col">
              {showCopy ? (
                <button
                  type="button"
                  className="flex items-center gap-2 rounded-lg px-3 py-2 text-left hover:bg-zinc-100 dark:hover:bg-zinc-800"
                  onClick={() => void copyRawText()}
                >
                  <i className="bi bi-clipboard h-4 w-4" aria-hidden="true"></i>
                  <span>{copyLabel === "Copy" ? "Copy raw" : "Copied"}</span>
                </button>
              ) : null}
              {!isImage && !isPdf ? (
                <a
                  href={viewRawHref}
                  className="flex items-center gap-2 rounded-lg px-3 py-2 hover:bg-zinc-100 dark:hover:bg-zinc-800"
                >
                  <i className="bi bi-file-earmark-text h-4 w-4" aria-hidden="true"></i>
                  <span>View raw</span>
                </a>
              ) : null}
              <a
                href={rawHref}
                className="flex items-center gap-2 rounded-lg px-3 py-2 hover:bg-zinc-100 dark:hover:bg-zinc-800"
              >
                <i className="bi bi-download h-4 w-4" aria-hidden="true"></i>
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
