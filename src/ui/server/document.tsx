import type { ReactNode } from "react";

import { highlightThemeHref } from "@/ui/highlight-theme";
import type { DocumentAssets } from "./assets";

type DocumentProps = {
  title?: string;
  assets: DocumentAssets;
  needsHighlight?: boolean;
  children: ReactNode;
};

const themeBootstrap = `(function(){try{var saved=localStorage.getItem("theme");var theme=saved==="light"?"light":"dark";var root=document.documentElement;root.dataset.theme=theme;root.classList.toggle("dark",theme==="dark");var hljs=document.getElementById("hljs-theme");if(hljs){hljs.setAttribute("href",theme==="dark"?"${highlightThemeHref.dark}":"${highlightThemeHref.light}");}}catch(_err){document.documentElement.dataset.theme="dark";document.documentElement.classList.add("dark");}})();`;
const reactRefreshPreamble = `import RefreshRuntime from "/@react-refresh";RefreshRuntime.injectIntoGlobalHook(window);window.$RefreshReg$=()=>{};window.$RefreshSig$=()=>type=>type;window.__vite_plugin_react_preamble_installed__=true;`;

export function Document({ title, assets, needsHighlight = false, children }: DocumentProps) {
  const pageTitle = title || "git-on-cloudflare";

  return (
    <html className="h-full" lang="en">
      <head>
        <meta charSet="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <meta name="color-scheme" content="light dark" />
        <title>{pageTitle}</title>
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link rel="preconnect" href="https://fonts.gstatic.com" crossOrigin="anonymous" />
        <link
          href="https://fonts.googleapis.com/css2?family=Hanken+Grotesk:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap"
          rel="stylesheet"
        />
        {needsHighlight ? (
          <link id="hljs-theme" rel="stylesheet" href={highlightThemeHref.dark} />
        ) : null}
        {import.meta.env.DEV ? (
          <script type="module" dangerouslySetInnerHTML={{ __html: reactRefreshPreamble }} />
        ) : null}
        <script dangerouslySetInnerHTML={{ __html: themeBootstrap }} />
        {assets.preloads.map((href) => (
          <link key={href} rel="modulepreload" href={href} />
        ))}
        {assets.styles.map((href) => (
          <link key={href} rel="stylesheet" href={href} />
        ))}
      </head>
      <body className="min-h-screen text-zinc-900 dark:text-zinc-100">
        {children}
        {assets.moduleScripts.map((src) => (
          <script key={src} type="module" src={src}></script>
        ))}
      </body>
    </html>
  );
}
