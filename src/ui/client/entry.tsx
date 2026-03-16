/// <reference lib="dom" />

import "bootstrap-icons/font/bootstrap-icons.css";
import "./styles.css";

import { initAuthAdmin } from "@/ui/islands/auth-admin";
import { initBlobActions } from "@/ui/islands/blob-actions";
import { initCommitDiffExpander } from "@/ui/islands/commit-diff-expander";
import { initCodeLineAnchors } from "@/ui/islands/code-line-anchors";
import { initMergeExpander } from "@/ui/islands/merge-expander";
import { initRefPicker } from "@/ui/islands/ref-picker";
import { initRepoAdmin } from "@/ui/islands/repo-admin";
import { initThemeToggle } from "@/ui/islands/theme-toggle";

function onReady(callback: () => void) {
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", callback, { once: true });
    return;
  }

  callback();
}

onReady(() => {
  initThemeToggle();
  initRefPicker();
  initCommitDiffExpander();
  initMergeExpander();
  initBlobActions();
  initCodeLineAnchors();
  initAuthAdmin();
  initRepoAdmin();
});
