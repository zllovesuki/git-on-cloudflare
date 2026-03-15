/// <reference lib="dom" />
/// <reference types="react" />

import type { ComponentType } from "react";

import { hydrateRoot } from "react-dom/client";

function readProps<P>(host: HTMLElement): P | null {
  const script = host.querySelector<HTMLScriptElement>("script[data-island-props]");
  if (!script?.textContent) {
    return null;
  }

  try {
    return JSON.parse(script.textContent) as P;
  } catch {
    return null;
  }
}

export function hydrateIsland<P>(name: string, Component: ComponentType<P>) {
  const hosts = document.querySelectorAll<HTMLElement>(`[data-island="${name}"]`);

  for (const host of Array.from(hosts)) {
    const root = host.querySelector<HTMLElement>("[data-island-root]");
    const props = readProps<P>(host);
    if (!root || !props) {
      continue;
    }

    hydrateRoot(root, <Component {...props} />);
  }
}
