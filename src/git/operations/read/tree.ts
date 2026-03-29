import type { CacheContext } from "@/cache/index.ts";
import type { TreeEntry } from "./types.ts";
import { readLooseObjectRaw, parseTree } from "./objects.ts";
import { resolveRef } from "./refs.ts";
import { readCommit } from "./commits.ts";
import { parseTagTarget } from "@/git/core/index.ts";

export async function readTree(
  env: Env,
  repoId: string,
  oid: string,
  cacheCtx?: CacheContext
): Promise<TreeEntry[]> {
  const obj = await readLooseObjectRaw(env, repoId, oid, cacheCtx);
  if (!obj || obj.type !== "tree") {
    throw new Error("Not a tree");
  }
  return parseTree(obj.payload);
}

export async function readPath(
  env: Env,
  repoId: string,
  ref: string,
  path?: string,
  cacheCtx?: CacheContext
): Promise<ReadPathResult> {
  let startOid: string | undefined = await resolveRef(env, repoId, ref);
  if (!startOid && /^[0-9a-f]{40}$/i.test(ref)) startOid = ref.toLowerCase();
  if (!startOid) throw new Error("Ref not found");

  const startObj = await readLooseObjectRaw(env, repoId, startOid, cacheCtx);
  if (!startObj) throw new Error("Object not found");

  let currentTreeOid: string | undefined;
  if (startObj.type === "commit") {
    const { tree } = await readCommit(env, repoId, startOid, cacheCtx);
    currentTreeOid = tree;
  } else if (startObj.type === "tree") {
    currentTreeOid = startOid;
  } else if (startObj.type === "tag") {
    const t = parseTagTarget(startObj.payload);
    if (!t || !t.targetOid) throw new Error("Unsupported object type");
    const target = t.targetOid;
    const { tree } = await readCommit(env, repoId, target, cacheCtx);
    currentTreeOid = tree;
  } else if (startObj.type === "blob") {
    if (path && path !== "") throw new Error("Path not a directory");
    return { type: "blob", oid: startOid, content: startObj.payload, base: "" };
  } else {
    throw new Error("Unsupported object type");
  }

  const parts = (path || "").split("/").filter(Boolean);
  let base = "";
  for (let i = 0; i < parts.length; i++) {
    const entries = await readTree(env, repoId, currentTreeOid, cacheCtx);
    const ent = entries.find((e) => e.name === parts[i]);
    if (!ent) throw new Error("Path not found");
    base = parts.slice(0, i + 1).join("/");
    if (ent.mode.startsWith("40000")) {
      currentTreeOid = ent.oid;
      if (i === parts.length - 1) {
        const finalEntries = await readTree(env, repoId, currentTreeOid, cacheCtx);
        return { type: "tree", entries: finalEntries, base };
      }
    } else {
      if (i !== parts.length - 1) throw new Error("Path not a directory");
      const MAX_SIZE = 5 * 1024 * 1024;
      const blob = await readLooseObjectRaw(env, repoId, ent.oid, cacheCtx);
      if (!blob || blob.type !== "blob") throw new Error("Not a blob");

      // Pack reads are authoritative here, so the UI size gate must be based
      // on the resolved blob payload instead of legacy loose-object metadata.
      const actualSize = blob.payload.byteLength;
      if (actualSize > MAX_SIZE) {
        return {
          type: "blob",
          oid: ent.oid,
          content: new Uint8Array(0),
          base,
          size: actualSize,
          tooLarge: true,
        };
      }

      return { type: "blob", oid: ent.oid, content: blob.payload, base };
    }
  }
  const rootEntries = await readTree(env, repoId, currentTreeOid, cacheCtx);
  return { type: "tree", entries: rootEntries, base };
}

export type ReadPathResult =
  | { type: "tree"; entries: TreeEntry[]; base: string }
  | {
      type: "blob";
      oid: string;
      content: Uint8Array;
      base: string;
      size?: number;
      tooLarge?: boolean;
    };

export function isTreeMode(mode: string): boolean {
  return mode.startsWith("40000");
}

export function joinTreePath(basePath: string, name: string): string {
  return basePath ? `${basePath}/${name}` : name;
}
