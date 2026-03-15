import { getClientManifest } from "./manifest";

export type DocumentAssets = {
  styles: string[];
  moduleScripts: string[];
  preloads: string[];
};

const CLIENT_ENTRY = "src/ui/client/entry.tsx";

function unique(values: string[]): string[] {
  return [...new Set(values)];
}

export async function resolveDocumentAssets(env: Env): Promise<DocumentAssets> {
  if (import.meta.env.DEV) {
    return {
      styles: ["/src/ui/client/styles.css"],
      moduleScripts: ["/@vite/client", "/src/ui/client/entry.tsx"],
      preloads: [],
    };
  }

  const manifest = await getClientManifest(env);
  const entry =
    manifest?.[CLIENT_ENTRY] ||
    (manifest
      ? Object.entries(manifest).find(([key]) => key.endsWith(CLIENT_ENTRY) || key === "entry")?.[1]
      : undefined);

  if (!entry) {
    return {
      styles: [],
      moduleScripts: [],
      preloads: [],
    };
  }

  const preloadSet = new Set<string>();

  function collectImports(key: string | undefined) {
    if (!key || preloadSet.has(key) || !manifest?.[key]) {
      return;
    }

    preloadSet.add(key);
    for (const importedKey of manifest[key].imports || []) {
      collectImports(importedKey);
    }
  }

  for (const importedKey of entry.imports || []) {
    collectImports(importedKey);
  }

  return {
    styles: unique(entry.css?.map((href) => `/${href}`) || []),
    moduleScripts: [`/${entry.file}`],
    preloads: unique(
      [...preloadSet]
        .map((key) => manifest?.[key]?.file)
        .filter((value): value is string => Boolean(value))
        .map((href) => `/${href}`)
    ),
  };
}
