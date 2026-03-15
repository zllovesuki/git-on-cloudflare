import githubDarkThemeHref from "highlight.js/styles/github-dark.min.css?url";
import stackoverflowLightThemeHref from "highlight.js/styles/stackoverflow-light.min.css?url";

export const highlightThemeHref = {
  dark: githubDarkThemeHref,
  light: stackoverflowLightThemeHref,
} as const;
