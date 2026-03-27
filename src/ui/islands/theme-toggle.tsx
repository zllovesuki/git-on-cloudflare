/// <reference lib="dom" />

import { useEffect, useState } from "react";
import { Moon, Sun } from "lucide-react";

import { highlightThemeHref } from "@/ui/highlight-theme";
import { hydrateIsland } from "@/ui/client/hydrate";

export type ThemeToggleProps = Record<string, never>;

function applyHighlightTheme(theme: "light" | "dark") {
  const stylesheet = document.getElementById("hljs-theme");
  if (!(stylesheet instanceof HTMLLinkElement)) {
    return;
  }

  stylesheet.href = theme === "dark" ? highlightThemeHref.dark : highlightThemeHref.light;
}

function applyTheme(theme: "light" | "dark") {
  document.documentElement.dataset.theme = theme;
  document.documentElement.classList.toggle("dark", theme === "dark");
  applyHighlightTheme(theme);
}

export function ThemeToggleIsland(_props: ThemeToggleProps) {
  const [theme, setTheme] = useState<"light" | "dark">("dark");

  useEffect(() => {
    const initial = document.documentElement.classList.contains("dark") ? "dark" : "light";
    setTheme(initial);
    applyTheme(initial);
  }, []);

  return (
    <button
      type="button"
      data-theme-toggle
      className="flex items-center gap-2 rounded-lg px-2.5 py-1.5 text-zinc-500 transition-colors hover:bg-zinc-100 hover:text-zinc-800 dark:text-zinc-400 dark:hover:bg-zinc-800 dark:hover:text-zinc-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent-500/50 focus-visible:ring-offset-2 focus-visible:ring-offset-white dark:focus-visible:ring-offset-zinc-950"
      aria-label={`Toggle theme (current: ${theme === "dark" ? "Dark" : "Light"})`}
      onClick={() => {
        const nextTheme = theme === "dark" ? "light" : "dark";
        setTheme(nextTheme);
        applyTheme(nextTheme);
        try {
          localStorage.setItem("theme", nextTheme);
        } catch {}
      }}
    >
      <Sun className="icon-light h-5 w-5" aria-hidden="true" />
      <Moon className="icon-dark h-5 w-5" aria-hidden="true" />
    </button>
  );
}

export function initThemeToggle() {
  hydrateIsland<ThemeToggleProps>("theme-toggle", ThemeToggleIsland);
}
