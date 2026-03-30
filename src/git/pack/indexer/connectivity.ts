/**
 * Pack-first connectivity checker for receive-pack validation.
 *
 * Validates that each updated ref's target object is reachable by searching
 * both the newly indexed pack and the existing active pack catalog. Uses the
 * project's pack-first object store (readObject / findObject) instead of
 * isomorphic-git, and avoids buffering the entire pack in memory.
 *
 * The key trick: we seed the CacheContext memo with the new pack's catalog row
 * and IdxView so the existing object store automatically searches the new pack
 * first — no new R2-reading code is needed.
 */

import type { CacheContext, RequestMemo } from "@/cache/index.ts";
import type { PackCatalogRow } from "@/git/object-store/types.ts";
import { parseCommitRefs, parseTagTarget } from "@/git/core/object-parse.ts";
import { readObject, findObject } from "@/git/object-store/store.ts";

import type { ConnectivityCheckOptions } from "./types.ts";

const MAX_TAG_DEPTH = 8;

export async function runPackConnectivityCheck(opts: ConnectivityCheckOptions): Promise<void> {
  const {
    env,
    repoId,
    newPackKey,
    newIdxView,
    newPackSize,
    activeCatalog,
    commands,
    statuses,
    log,
    cacheCtx,
  } = opts;

  const newRow: PackCatalogRow = {
    packKey: newPackKey,
    kind: "receive",
    state: "active",
    tier: 0,
    seqLo: 0,
    seqHi: 0,
    objectCount: newIdxView.count,
    packBytes: newPackSize,
    idxBytes: 0, // not needed for lookup
    createdAt: Date.now(),
    supersededBy: null,
  };

  // Keep staged-pack visibility scoped to this connectivity pass. The receive
  // path may reuse the caller cache context after a rejection, so leaking the
  // staged pack into the shared memo would make later reads observe an
  // uncommitted pack as if it were active.
  const scopedCacheCtx = createScopedConnectivityCacheContext(
    cacheCtx,
    repoId,
    [newRow, ...activeCatalog],
    newPackKey,
    newIdxView
  );

  // Per-run caches to avoid repeated reads.
  const hasCache = new Map<string, boolean>();
  const kindCache = new Map<string, FinalKind>();

  const hasObject = async (oid: string): Promise<boolean> => {
    const lc = oid.toLowerCase();
    const cached = hasCache.get(lc);
    if (cached !== undefined) return cached;
    const found = !!(await findObject(env, repoId, lc, scopedCacheCtx));
    hasCache.set(lc, found);
    return found;
  };

  const readKind = async (oid: string): Promise<FinalKind> => {
    const lc = oid.toLowerCase();
    const cached = kindCache.get(lc);
    if (cached) return cached;

    const obj = await readObject(env, repoId, lc, scopedCacheCtx);
    if (!obj) {
      const k: FinalKind = { type: "unknown", oid: lc };
      kindCache.set(lc, k);
      return k;
    }

    if (obj.type === "commit") {
      const refs = parseCommitRefs(obj.payload);
      const k: FinalKind = {
        type: "commit",
        oid: lc,
        tree: refs.tree || "",
        parents: refs.parents,
      };
      kindCache.set(lc, k);
      return k;
    }
    if (obj.type === "tree") {
      const k: FinalKind = { type: "tree", oid: lc };
      kindCache.set(lc, k);
      return k;
    }
    if (obj.type === "blob") {
      const k: FinalKind = { type: "blob", oid: lc };
      kindCache.set(lc, k);
      return k;
    }
    if (obj.type === "tag") {
      const tag = parseTagTarget(obj.payload);
      if (tag) {
        const k: FinalKind = {
          type: "tag",
          oid: lc,
          targetOid: tag.targetOid,
          targetType: tag.targetType,
        };
        kindCache.set(lc, k);
        return k;
      }
    }

    const k: FinalKind = { type: "unknown", oid: lc };
    kindCache.set(lc, k);
    return k;
  };

  const unwrapTag = async (
    initialTag: Extract<FinalKind, { type: "tag" }>,
    maxDepth = MAX_TAG_DEPTH
  ): Promise<FinalKind> => {
    // The limit is "8 tag hops", not "8 readKind() calls". The caller already
    // proved that `newOid` is a tag, so start from that tag and count only the
    // edges we actually follow through a tag chain.
    let currentTag = initialTag;
    let tagHops = 1;
    while (tagHops <= maxDepth) {
      const next = await readKind(currentTag.targetOid);
      if (next.type !== "tag") return next;
      currentTag = next;
      tagHops++;
    }
    return { type: "unknown", oid: currentTag.oid };
  };

  try {
    // ---- Validate each non-delete command ----
    for (let i = 0; i < commands.length; i++) {
      const cmd = commands[i];
      const st = statuses[i];
      if (!st?.ok) continue;
      if (/^0{40}$/i.test(cmd.newOid)) continue; // delete — skip

      try {
        const newOidLc = cmd.newOid.toLowerCase();
        const initialKind = await readKind(newOidLc);
        const kind = initialKind.type === "tag" ? await unwrapTag(initialKind) : initialKind;

        switch (kind.type) {
          case "commit": {
            // Require root tree exists.
            if (!kind.tree || !(await hasObject(kind.tree))) {
              log.warn("connectivity:missing-tree", { ref: cmd.ref, tree: kind.tree });
              statuses[i] = { ref: cmd.ref, ok: false, msg: "missing-objects" };
              break;
            }
            // Require all parents exist.
            for (const p of kind.parents) {
              if (!(await hasObject(p))) {
                log.warn("connectivity:missing-parent", { ref: cmd.ref, parent: p });
                statuses[i] = { ref: cmd.ref, ok: false, msg: "missing-objects" };
                break;
              }
            }
            break;
          }
          case "tree":
          case "blob":
            // Already found by readObject — accept.
            break;
          case "unknown":
            log.warn("connectivity:unknown-type-or-missing", { ref: cmd.ref, oid: newOidLc });
            statuses[i] = { ref: cmd.ref, ok: false, msg: "missing-objects" };
            break;
        }
      } catch (e) {
        log.warn("connectivity:check-error", { ref: cmd.ref, error: String(e) });
        statuses[i] = { ref: cmd.ref, ok: false, msg: "missing-objects" };
      }
    }
  } finally {
    syncScopedSubrequestBudget(cacheCtx, scopedCacheCtx);
  }
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

type FinalKind =
  | { type: "commit"; oid: string; tree: string; parents: string[] }
  | { type: "tree"; oid: string }
  | { type: "blob"; oid: string }
  | { type: "tag"; oid: string; targetOid: string; targetType: string }
  | { type: "unknown"; oid: string };

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function createScopedConnectivityCacheContext(
  cacheCtx: CacheContext,
  repoId: string,
  packCatalog: PackCatalogRow[],
  newPackKey: string,
  newIdxView: ConnectivityCheckOptions["newIdxView"]
): CacheContext {
  const parentMemo = cacheCtx.memo;
  const sameRepo = !parentMemo?.repoId || parentMemo.repoId === repoId;
  const childMemo: RequestMemo = {
    repoId,
    subreqBudget: parentMemo?.subreqBudget,
    limiter: parentMemo?.limiter,
    flags: parentMemo?.flags ? new Set(parentMemo.flags) : undefined,
    // Clone only the request-scoped idx memo. Packed object results and pack
    // catalog snapshots intentionally do not cross this boundary because those
    // would make later reads observe the staged pack after this validation
    // scope has ended.
    idxViews: sameRepo && parentMemo?.idxViews ? new Map(parentMemo.idxViews) : new Map(),
  };

  childMemo.packCatalog = packCatalog;
  childMemo.idxViews!.set(newPackKey, newIdxView);

  return {
    req: cacheCtx.req,
    ctx: cacheCtx.ctx,
    memo: childMemo,
  };
}

function syncScopedSubrequestBudget(parent: CacheContext, child: CacheContext): void {
  if (child.memo?.subreqBudget === undefined) return;
  parent.memo = parent.memo || {};
  // Only the soft budget is synchronized back out. The scoped pack catalog and
  // staged-pack idx view must stay isolated so a rejected receive cannot leak
  // uncommitted visibility into the caller's memo.
  parent.memo.subreqBudget = child.memo.subreqBudget;
}
