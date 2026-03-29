import type { DebugCommitCheck, DebugOidCheck } from "./types.ts";

import { objKey } from "../repoState.ts";
import { doPrefix, r2LooseKey } from "@/keys.ts";
import { isValidOid } from "@/common/index.ts";
import { readCommitFromStore } from "../storage.ts";
import { getDb, listPackCatalog } from "../db/index.ts";
import { getActivePackCatalogSnapshot } from "../catalog.ts";
import { parseCommitText } from "@/git/core/commitParse.ts";
import { findCatalogPacksContainingOid, readPackedObjectFromCatalogRows } from "./packed.ts";

export async function debugCheckCommit(
  ctx: DurableObjectState,
  env: Env,
  commit: string
): Promise<DebugCommitCheck> {
  const q = (commit || "").toLowerCase();
  if (!isValidOid(q)) {
    throw new Error("Invalid commit");
  }

  const db = getDb(ctx.storage);
  const prefix = doPrefix(ctx.id.toString());
  await getActivePackCatalogSnapshot(ctx, env, prefix);
  const catalogRows = await listPackCatalog(db);
  const activeCatalogRows = catalogRows.filter((row) => row.state === "active");
  const membership: Record<string, { hasCommit: boolean; hasTree: boolean }> = {};

  for (const row of catalogRows) {
    membership[row.packKey] = { hasCommit: false, hasTree: false };
  }

  for (const packKey of await findCatalogPacksContainingOid(env, catalogRows, q)) {
    membership[packKey] = membership[packKey] || { hasCommit: false, hasTree: false };
    membership[packKey].hasCommit = true;
  }

  let tree: string | undefined;
  let parents: string[] = [];

  try {
    const packedInfo = await readPackedObjectFromCatalogRows(env, activeCatalogRows, q);
    if (packedInfo?.type === "commit") {
      const parsed = parseCommitText(new TextDecoder().decode(packedInfo.payload));
      tree = parsed.tree.toLowerCase();
      parents = parsed.parents;
    } else {
      const legacyInfo = await readCommitFromStore(ctx, env, prefix, q);
      if (legacyInfo) {
        tree = legacyInfo.tree.toLowerCase();
        parents = legacyInfo.parents;
      }
    }
  } catch {}

  const hasLooseCommit = !!(await ctx.storage.get(objKey(q)));
  let hasLooseTree = false;
  let hasR2LooseTree = false;

  if (tree) {
    hasLooseTree = !!(await ctx.storage.get(objKey(tree)));
    try {
      const head = await env.REPO_BUCKET.head(r2LooseKey(prefix, tree));
      hasR2LooseTree = !!head;
    } catch {}

    for (const packKey of await findCatalogPacksContainingOid(env, catalogRows, tree)) {
      membership[packKey] = membership[packKey] || { hasCommit: false, hasTree: false };
      membership[packKey].hasTree = true;
    }
  }

  return {
    commit: { oid: q, parents, tree },
    presence: { hasLooseCommit, hasLooseTree, hasR2LooseTree },
    membership,
  };
}

export async function debugCheckOid(
  ctx: DurableObjectState,
  env: Env,
  oid: string
): Promise<DebugOidCheck> {
  if (!isValidOid(oid)) {
    throw new Error(`Invalid OID: ${oid}`);
  }

  const prefix = doPrefix(ctx.id.toString());
  const hasLoose = !!(await ctx.storage.get(objKey(oid)));

  let hasR2Loose = false;
  try {
    const head = await env.REPO_BUCKET.head(r2LooseKey(prefix, oid));
    hasR2Loose = !!head;
  } catch {}

  const db = getDb(ctx.storage);
  await getActivePackCatalogSnapshot(ctx, env, prefix);
  const catalogRows = await listPackCatalog(db);
  const inPacks = await findCatalogPacksContainingOid(env, catalogRows, oid);

  return {
    oid,
    presence: {
      hasLoose,
      hasR2Loose,
    },
    inPacks,
  };
}
