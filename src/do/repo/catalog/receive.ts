import type { Logger } from "@/common/logger.ts";
import type { PackCatalogRow } from "../db/schema.ts";
import type { RepoStateSchema, RepoStorageMode } from "../repoState.ts";

import { asTypedStorage } from "../repoState.ts";
import {
  applyReceiveCommands,
  isValidRefName,
  type ReceiveCommand,
  type ReceiveStatus,
  validateReceiveCommands,
} from "@/git/operations/validation.ts";
import { getDb, listActivePackCatalog, upsertPackCatalogRow } from "../db/index.ts";
import {
  DEFAULT_HEAD,
  bumpPacksetVersion,
  ensureRepoMetadataDefaults,
  mirrorLegacyPackKeys,
} from "./shared.ts";
import { catalogNeedsCompaction, scheduleCompactionWake } from "./compaction/plan.ts";

export type FinalizeReceiveResult =
  | {
      status: "committed";
      statuses: ReceiveStatus[];
      changed: boolean;
      empty: boolean;
      shouldQueueCompaction: boolean;
      currentMode: RepoStorageMode;
    }
  | {
      status: "ref_conflict";
      statuses: ReceiveStatus[];
      message: string;
      currentMode: RepoStorageMode;
    }
  | {
      status: "lease_mismatch" | "mode_mismatch";
      message: string;
      currentMode: RepoStorageMode;
    };

function resolveHeadAfterReceive(args: {
  storedHead:
    | {
        target: string;
        oid?: string;
        unborn?: boolean;
      }
    | undefined;
  refs: Array<{ name: string; oid: string }>;
}) {
  const target = args.storedHead?.target || DEFAULT_HEAD.target;
  const match = args.refs.find((ref) => ref.name === target);
  if (match) {
    return { target, oid: match.oid } as const;
  }
  return { target, unborn: true } as const;
}

export async function finalizeReceiveState(args: {
  ctx: DurableObjectState;
  env: Env;
  token: string;
  commands: ReceiveCommand[];
  stagedPack?:
    | {
        packKey: string;
        packBytes: number;
        idxBytes: number;
        objectCount: number;
      }
    | undefined;
  logger?: Logger;
}): Promise<FinalizeReceiveResult> {
  const store = asTypedStorage<RepoStateSchema>(args.ctx.storage);
  const currentMode = await ensureRepoMetadataDefaults(store);
  if (currentMode !== "streaming") {
    return {
      status: "mode_mismatch",
      message: "Streaming receive finalization is only valid while the repo is in streaming mode.",
      currentMode,
    };
  }

  const lease = await store.get("receiveLease");
  if (!lease || lease.token !== args.token) {
    return {
      status: "lease_mismatch",
      message: "Receive lease is no longer active for this request.",
      currentMode,
    };
  }

  const currentRefs = (await store.get("refs")) || [];
  const invalidStatuses = args.commands
    .filter((command) => !isValidRefName(command.ref))
    .map((command) => ({ ref: command.ref, ok: false, msg: "invalid" satisfies string }));
  if (invalidStatuses.length > 0) {
    await store.delete("receiveLease");
    args.logger?.warn("receive:finalize-invalid-ref", {
      invalidCount: invalidStatuses.length,
    });
    return {
      status: "ref_conflict",
      statuses: invalidStatuses,
      message: "Receive finalization rejected invalid refs.",
      currentMode,
    };
  }

  const statuses = validateReceiveCommands(currentRefs, args.commands);
  if (!statuses.every((status) => status.ok)) {
    await store.delete("receiveLease");
    args.logger?.warn("receive:finalize-ref-conflict", {
      conflictCount: statuses.filter((status) => !status.ok).length,
    });
    return {
      status: "ref_conflict",
      statuses,
      message: "Ref expectations changed before the receive could be committed.",
      currentMode,
    };
  }

  const nextRefs = applyReceiveCommands(currentRefs, args.commands);
  const storedHead = await store.get("head");
  const nextHead = resolveHeadAfterReceive({ storedHead, refs: nextRefs });
  const nextRefsVersion = ((await store.get("refsVersion")) || 0) + 1;

  let shouldQueueCompaction = false;
  if (args.stagedPack) {
    const nextPackSeq = (await store.get("nextPackSeq")) || 1;
    const db = getDb(args.ctx.storage);
    await upsertPackCatalogRow(db, {
      packKey: args.stagedPack.packKey,
      kind: "receive",
      state: "active",
      tier: 0,
      seqLo: nextPackSeq,
      seqHi: nextPackSeq,
      objectCount: args.stagedPack.objectCount,
      packBytes: args.stagedPack.packBytes,
      idxBytes: args.stagedPack.idxBytes,
      createdAt: Date.now(),
      supersededBy: null,
    });
    await store.put("nextPackSeq", nextPackSeq + 1);
    const activeCatalog = await listActivePackCatalog(db);
    await bumpPacksetVersion(store);
    await mirrorLegacyPackKeys(store, activeCatalog);
    shouldQueueCompaction = catalogNeedsCompaction(activeCatalog);
    if (shouldQueueCompaction) {
      await store.put("compactionWantedAt", Date.now());
      await scheduleCompactionWake(args.ctx, args.env);
    }
  }

  await store.put("refs", nextRefs);
  await store.put("head", nextHead);
  await store.put("refsVersion", nextRefsVersion);
  await store.delete("receiveLease");

  args.logger?.info("receive:finalize-committed", {
    commandCount: args.commands.length,
    refCount: nextRefs.length,
    empty: nextRefs.length === 0,
    stagedPackKey: args.stagedPack?.packKey,
    shouldQueueCompaction,
  });

  return {
    status: "committed",
    statuses,
    changed: args.commands.length > 0,
    empty: nextRefs.length === 0,
    shouldQueueCompaction,
    currentMode,
  };
}
