/// <reference lib="dom" />

function getTargetRow(rowId: string): HTMLElement | null {
  const row = document.getElementById(rowId);
  if (!row || !row.classList.contains("code-line")) {
    return null;
  }

  return row;
}

function clearCurrentHighlight() {
  document.querySelector("#blob-code .line-highlight")?.classList.remove("line-highlight");
}

function highlightRow(rowId: string, smooth: boolean) {
  const row = getTargetRow(rowId);
  if (!row) {
    clearCurrentHighlight();
    return;
  }

  clearCurrentHighlight();
  row.classList.add("line-highlight");
  row.scrollIntoView({ block: "center", behavior: smooth ? "smooth" : "auto" });
}

function highlightCurrentHash(smooth: boolean) {
  const hash = window.location.hash;
  if (!/^#L\d+$/.test(hash)) {
    clearCurrentHighlight();
    return;
  }

  highlightRow(hash.slice(1), smooth);
}

export function initCodeLineAnchors() {
  if (!document.getElementById("blob-code")) {
    return;
  }

  document.addEventListener("click", (event) => {
    const target = event.target as HTMLElement;
    const link = target.closest<HTMLAnchorElement>('#blob-code .line-num a[href^="#L"]');
    const href = link?.getAttribute("href") || "";
    if (!link || !/^#L\d+$/.test(href)) {
      return;
    }

    const rowId = href.slice(1);
    if (!getTargetRow(rowId)) {
      return;
    }

    event.preventDefault();
    const nextHash = `#${rowId}`;
    if (window.location.hash === nextHash) {
      highlightRow(rowId, true);
      return;
    }

    history.pushState(null, "", nextHash);
    highlightRow(rowId, true);
  });

  window.addEventListener("hashchange", () => {
    highlightCurrentHash(true);
  });

  highlightCurrentHash(false);
}
