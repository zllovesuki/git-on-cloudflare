import type { ReactNode } from "react";

import { highlightThemeHref } from "@/client/highlight-theme";
import type { DocumentAssets } from "./assets";

type DocumentProps = {
  title?: string;
  assets: DocumentAssets;
  needsHighlight?: boolean;
  children: ReactNode;
};

const initialHighlightThemeHref = highlightThemeHref.dark || highlightThemeHref.light;
const themeBootstrap = `(function(){try{var saved=localStorage.getItem("theme");var theme=saved==="light"?"light":"dark";var root=document.documentElement;root.dataset.theme=theme;root.classList.toggle("dark",theme==="dark");var hljs=document.getElementById("hljs-theme");var darkHref="${highlightThemeHref.dark}";var lightHref="${highlightThemeHref.light}";if(hljs&&(darkHref||lightHref)){var nextHref=theme==="dark"?darkHref:lightHref;if(nextHref){hljs.setAttribute("href",nextHref);}else{hljs.removeAttribute("href");}}}catch(_err){document.documentElement.dataset.theme="dark";document.documentElement.classList.add("dark");}})();`;
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
          href="https://fonts.googleapis.com/css2?family=Hanken+Grotesk:wght@400..700&family=JetBrains+Mono:wght@400;500&family=IBM+Plex+Serif:wght@600;700&display=swap"
          rel="stylesheet"
        />
        {needsHighlight && initialHighlightThemeHref ? (
          <link id="hljs-theme" rel="stylesheet" href={initialHighlightThemeHref} />
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
