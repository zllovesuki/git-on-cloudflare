type ViteManifestEntry = {
  file: string;
  css?: string[];
  imports?: string[];
};

type ViteManifest = Record<string, ViteManifestEntry>;

let manifestPromise: Promise<ViteManifest | null> | null = null;

async function fetchManifest(env: Env): Promise<ViteManifest | null> {
  if (!env.ASSETS) {
    return null;
  }

  const url = new URL("/manifest.json", "https://assets.local");
  const response = await env.ASSETS.fetch(new Request(url.toString()));
  if (!response.ok) {
    return null;
  }

  return (await response.json()) as ViteManifest;
}

export async function getClientManifest(env: Env): Promise<ViteManifest | null> {
  if (import.meta.env.DEV) {
    return null;
  }

  if (manifestPromise) {
    return manifestPromise;
  }

  const promise = fetchManifest(env);
  manifestPromise = promise;
  promise.catch(() => {
    manifestPromise = null;
  });
  return promise.catch(() => null);
}
