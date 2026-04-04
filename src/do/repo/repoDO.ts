import type { Head, RepoStateSchema, RepoStorageMode, TypedStorage } from "./repoState.ts";
import type { RepoActivity, UnpackProgress } from "@/common/index.ts";
import type { PackCatalogRow } from "./db/schema.ts";
import type {
  RepoStorageModeControl,
  RepoStorageModeMutationResult,
} from "@/contracts/repoStorageMode.ts";

import { DurableObject } from "cloudflare:workers";
import type { DrizzleSqliteDODatabase } from "drizzle-orm/durable-sqlite";
import { asTypedStorage } from "./repoState.ts";
import { doPrefix } from "@/keys.ts";
import { text, createLogger, isValidOid } from "@/common/index.ts";
import {
  enqueueHydrationTask,
  summarizeHydrationPlan,
  clearHydrationState,
} from "./hydration/index.ts";
import { purgeRepo, removePack } from "./packOperations.ts";
import { hasLoose, storeObject, getObjectsBatch } from "./storage.ts";
import { getPacks } from "./packs.ts";
import {
  abortCompactionLease,
  abortReceiveLease,
  type BeginCompactionResult,
  beginCompactionState,
  beginReceiveLease,
  type ClearCompactionRequestResult,
  clearCompactionRequestState,
  clearExpiredLeases,
  completeLegacyCompatBackfillState,
  type CommitCompactionResult,
  commitCompactionState,
  failLegacyCompatBackfillState,
  finalizeReceiveState,
  type LegacyCompatBackfillBatch,
  type LegacyCompatBackfillCursor,
  type PreviewCompactionResult,
  previewCompactionState,
  requestLegacyCompatBackfillState,
  type RequestCompactionResult,
  requestCompactionState,
  rearmCompactionQueueFromAlarm,
  beginLegacyCompatBackfillState,
  storeLegacyCompatBatchState,
  getActivePackCatalogSnapshot,
  getRepoActivitySnapshot,
  getRepoStorageModeControl,
  getRepoStorageModeValue,
  setRepoStorageModeGuarded,
  setRepoStorageModeValue,
} from "./catalog.ts";
import { getRefs, setRefs, resolveHead, setHead, getHeadAndRefs } from "./refs.ts";
import { handleUnpackWork, getUnpackProgress } from "./unpack.ts";
import { handleIdleAndMaintenance } from "./maintenance.ts";
import {
  debugState,
  debugCheckCommit,
  debugCheckOid,
  type DebugCommitCheck,
  type DebugOidCheck,
  type DebugStateSnapshot,
} from "./debug.ts";
import { migrate } from "drizzle-orm/durable-sqlite/migrator";
import { getDb } from "./db/index.ts";
import { migrateKvToSql } from "./db/migrate.ts";
import migrations from "@/drizzle/migrations";
import {
  ensureAccessAndAlarm,
  touchAndMaybeSchedule,
  type RepoDOAccessContext,
} from "./repoDO/access.ts";
import { sanitizeRawStorageMode } from "./catalog/shared.ts";
import { handleReceiveRequest } from "./repoDO/receive.ts";
import { handleHydrationAlarmWork } from "./repoDO/hydration.ts";
import { seedMinimalRepoState } from "./repoDO/seeding.ts";

/**
 * Repository Durable Object (per-repo authority)
 *
 * Responsibilities
 * - Acts as the strongly consistent source of truth for repository metadata.
 * - Stores refs, HEAD, storage mode, and pack catalog state in DO storage/SQLite.
 * - Exposes focused internal HTTP endpoints:
 *   - `POST /receive` — legacy receive-pack (delegates to git/operations/receive.ts)
 * - All other operations are provided as typed RPC methods on the class.
 *
 * Read Path (RPC)
 * - Correctness reads live in worker-local pack-first helpers under `src/git/object-store/`.
 * - There is no public HTTP endpoint for object reads; this keeps internal state access typed
 *   and easy to audit.
 *
 * Write Path
 * - Streaming receive (default): the Worker writes staged `.pack` and `.idx` data to R2, then
 *   commits refs and pack-catalog metadata through typed DO RPCs.
 * - Legacy receive (rollback window): the `POST /receive` path buffers the request body,
 *   stores the raw `.pack` to R2, performs a fast index-only step to produce `.idx`, and
 *   queues asynchronous unpack work. This path remains callable for repos explicitly set
 *   to `legacy` mode.
 *
 * Maintenance & Background Work
 * - `alarm()` is mode-aware:
 *   1) `streaming` repos: lightweight lease cleanup, compaction queue re-arm, idle cleanup.
 *   2) `legacy` repos: legacy unpack and hydration work, plus idle cleanup and periodic
 *      maintenance. Hydration packs (`pack-hydr-*`) and hydration coverage help make the
 *      serving set complete for legacy fetch.
 * - The DO is the metadata authority; the data plane lives in R2 packs.
 */
export class RepoDurableObject extends DurableObject {
  declare env: Env;
  private lastAccessMemMs: number | undefined;
  private db: DrizzleSqliteDODatabase<any> | undefined;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => {
      this.lastAccessMemMs = await ctx.storage.get("lastAccessMs");
      this.db = getDb(ctx.storage);
      await migrate(this.db, migrations);
      await migrateKvToSql(this.ctx, this.db, this.logger);
      await sanitizeRawStorageMode(ctx.storage, this.logger);
      await this.ensureAccessAndAlarm();
    });
  }

  async fetch(request: Request): Promise<Response> {
    try {
      await this.touchAndMaybeSchedule();
    } catch {}
    const url = new URL(request.url);
    this.logger.debug("fetch", { path: url.pathname, method: request.method });

    if (url.pathname === "/receive" && request.method === "POST") {
      return this.handleReceive(request);
    }

    return text("Not found\n", 404);
  }

  async alarm(): Promise<void> {
    const store = asTypedStorage<RepoStateSchema>(this.ctx.storage);
    this.logger.debug("alarm:start", {});

    await clearExpiredLeases(this.ctx, this.logger);

    const currentMode = await getRepoStorageModeValue(this.ctx);
    if (currentMode === "streaming") {
      if (
        await rearmCompactionQueueFromAlarm({ ctx: this.ctx, env: this.env, logger: this.logger })
      ) {
        return;
      }
    } else {
      if (await handleUnpackWork(this.ctx, this.env, this.prefix(), this.logger)) {
        return;
      }

      if (await this.handleHydrationWork(store)) {
        return;
      }
    }

    await handleIdleAndMaintenance(this.ctx, this.env, this.logger);
    this.logger.debug("alarm:end", {});
  }

  private async touchAndMaybeSchedule(): Promise<void> {
    await touchAndMaybeSchedule(this.accessContext());
  }

  private async ensureAccessAndAlarm(): Promise<void> {
    await ensureAccessAndAlarm(this.accessContext());
  }

  private accessContext(): RepoDOAccessContext {
    return {
      ctx: this.ctx,
      env: this.env,
      logger: this.logger,
      getLastAccessMemMs: () => this.lastAccessMemMs,
      setLastAccessMemMs: (value) => {
        this.lastAccessMemMs = value;
      },
    };
  }

  public async listRefs(): Promise<{ name: string; oid: string }[]> {
    await this.ensureAccessAndAlarm();
    return await getRefs(this.ctx);
  }

  public async setRefs(refs: { name: string; oid: string }[]): Promise<void> {
    await this.ensureAccessAndAlarm();
    await setRefs(this.ctx, refs);
  }

  public async getHead(): Promise<Head> {
    await this.ensureAccessAndAlarm();
    return await resolveHead(this.ctx);
  }

  public async setHead(head: Head): Promise<void> {
    await this.ensureAccessAndAlarm();
    await setHead(this.ctx, head);
  }

  public async getHeadAndRefs(): Promise<{ head: Head; refs: { name: string; oid: string }[] }> {
    await this.ensureAccessAndAlarm();
    return await getHeadAndRefs(this.ctx);
  }

  public async hasLoose(oid: string): Promise<boolean> {
    await this.ensureAccessAndAlarm();
    return await hasLoose(this.ctx, this.env, this.prefix(), oid);
  }

  public async getPacks(): Promise<string[]> {
    await this.ensureAccessAndAlarm();
    return await getPacks(this.ctx, this.env);
  }

  public async getActivePackCatalog(): Promise<PackCatalogRow[]> {
    await this.ensureAccessAndAlarm();
    return await getActivePackCatalogSnapshot(this.ctx, this.env, this.prefix(), this.logger);
  }

  public async getRepoStorageMode(): Promise<RepoStorageMode> {
    await this.ensureAccessAndAlarm();
    return await getRepoStorageModeValue(this.ctx);
  }

  public async getRepoStorageModeControl(): Promise<RepoStorageModeControl> {
    await this.ensureAccessAndAlarm();
    return await getRepoStorageModeControl(this.ctx, this.env, this.prefix(), this.logger);
  }

  public async getRepoActivity(): Promise<RepoActivity | null> {
    await this.ensureAccessAndAlarm();
    const snapshot = await getRepoActivitySnapshot(this.ctx);
    if (snapshot.state === "idle") return null;
    return {
      state: snapshot.state,
      startedAt: snapshot.lease.createdAt,
      expiresAt: snapshot.lease.expiresAt,
    };
  }

  public async setRepoStorageMode(mode: RepoStorageMode): Promise<RepoStorageMode> {
    await this.ensureAccessAndAlarm();
    return await setRepoStorageModeValue(this.ctx, mode, this.logger);
  }

  public async setRepoStorageModeGuarded(mode: string): Promise<RepoStorageModeMutationResult> {
    await this.ensureAccessAndAlarm();
    return await setRepoStorageModeGuarded(this.ctx, this.env, this.prefix(), mode, this.logger);
  }

  public async beginReceive() {
    await this.ensureAccessAndAlarm();
    return await beginReceiveLease(this.ctx, this.env, this.prefix(), this.logger);
  }

  public async abortReceive(token: string): Promise<boolean> {
    await this.ensureAccessAndAlarm();
    return await abortReceiveLease(this.ctx, token);
  }

  public async finalizeReceive(args: {
    token: string;
    commands: Array<{ oldOid: string; newOid: string; ref: string }>;
    stagedPack?:
      | {
          packKey: string;
          packBytes: number;
          idxBytes: number;
          objectCount: number;
        }
      | undefined;
  }) {
    await this.ensureAccessAndAlarm();
    return await finalizeReceiveState({
      ctx: this.ctx,
      env: this.env,
      token: args.token,
      commands: args.commands,
      stagedPack: args.stagedPack,
      logger: this.logger,
    });
  }

  public async beginCompaction(): Promise<BeginCompactionResult> {
    await this.ensureAccessAndAlarm();
    return await beginCompactionState({
      ctx: this.ctx,
      env: this.env,
      prefix: this.prefix(),
      logger: this.logger,
    });
  }

  public async abortCompaction(token: string): Promise<boolean> {
    await this.ensureAccessAndAlarm();
    return await abortCompactionLease(this.ctx, token);
  }

  public async commitCompaction(args: {
    token: string;
    sourcePacks: PackCatalogRow[];
    targetTier: number;
    packsetVersion: number;
    stagedPack: {
      packKey: string;
      packBytes: number;
      idxBytes: number;
      objectCount: number;
    };
  }): Promise<CommitCompactionResult> {
    await this.ensureAccessAndAlarm();
    return await commitCompactionState({
      ctx: this.ctx,
      env: this.env,
      token: args.token,
      sourcePacks: args.sourcePacks,
      targetTier: args.targetTier,
      packsetVersion: args.packsetVersion,
      stagedPack: args.stagedPack,
      logger: this.logger,
    });
  }

  public async requestLegacyCompatBackfill() {
    await this.ensureAccessAndAlarm();
    return await requestLegacyCompatBackfillState(this.ctx, this.logger);
  }

  public async beginLegacyCompatBackfill(jobId: string, targetPacksetVersion: number) {
    await this.ensureAccessAndAlarm();
    return await beginLegacyCompatBackfillState({
      ctx: this.ctx,
      env: this.env,
      prefix: this.prefix(),
      jobId,
      targetPacksetVersion,
      logger: this.logger,
    });
  }

  public async storeLegacyCompatBatch(args: {
    jobId: string;
    targetPacksetVersion: number;
    batch: LegacyCompatBackfillBatch;
    nextProgress: LegacyCompatBackfillCursor;
  }) {
    await this.ensureAccessAndAlarm();
    return await storeLegacyCompatBatchState({
      ctx: this.ctx,
      jobId: args.jobId,
      targetPacksetVersion: args.targetPacksetVersion,
      batch: args.batch,
      nextProgress: args.nextProgress,
      logger: this.logger,
    });
  }

  public async completeLegacyCompatBackfill(jobId: string, targetPacksetVersion: number) {
    await this.ensureAccessAndAlarm();
    return await completeLegacyCompatBackfillState({
      ctx: this.ctx,
      jobId,
      targetPacksetVersion,
      logger: this.logger,
    });
  }

  public async failLegacyCompatBackfill(
    jobId: string,
    targetPacksetVersion: number,
    error: string
  ): Promise<void> {
    await this.ensureAccessAndAlarm();
    await failLegacyCompatBackfillState({
      ctx: this.ctx,
      jobId,
      targetPacksetVersion,
      error,
      logger: this.logger,
    });
  }

  public async getUnpackProgress(): Promise<UnpackProgress> {
    await this.ensureAccessAndAlarm();
    return await getUnpackProgress(this.ctx);
  }

  public async previewCompaction(): Promise<PreviewCompactionResult> {
    await this.ensureAccessAndAlarm();
    return await previewCompactionState({
      ctx: this.ctx,
      env: this.env,
      prefix: this.prefix(),
      logger: this.logger,
    });
  }

  public async requestCompaction(): Promise<RequestCompactionResult> {
    await this.ensureAccessAndAlarm();
    return await requestCompactionState({
      ctx: this.ctx,
      env: this.env,
      prefix: this.prefix(),
      logger: this.logger,
    });
  }

  public async clearCompactionRequest(): Promise<ClearCompactionRequestResult> {
    await this.ensureAccessAndAlarm();
    return await clearCompactionRequestState({
      ctx: this.ctx,
      logger: this.logger,
    });
  }

  public async debugState(): Promise<DebugStateSnapshot> {
    await this.ensureAccessAndAlarm();
    return await debugState(this.ctx, this.env);
  }

  public async debugCheckCommit(commit: string): Promise<DebugCommitCheck> {
    await this.ensureAccessAndAlarm();
    return await debugCheckCommit(this.ctx, this.env, commit);
  }

  public async debugCheckOid(oid: string): Promise<DebugOidCheck> {
    await this.ensureAccessAndAlarm();
    return await debugCheckOid(this.ctx, this.env, oid);
  }

  public async getObjectsBatch(oids: string[]): Promise<Map<string, Uint8Array | null>> {
    await this.ensureAccessAndAlarm();
    return await getObjectsBatch(this.ctx, oids);
  }

  private async handleReceive(request: Request) {
    return await handleReceiveRequest({
      ctx: this.ctx,
      env: this.env,
      prefix: this.prefix(),
      request,
      logger: this.logger,
    });
  }

  private prefix() {
    return doPrefix(this.ctx.id.toString());
  }

  private get logger() {
    return createLogger(this.env.LOG_LEVEL, {
      service: "RepoDO",
      doId: this.ctx.id.toString(),
    });
  }

  private async handleHydrationWork(store: TypedStorage<RepoStateSchema>): Promise<boolean> {
    return await handleHydrationAlarmWork({
      ctx: this.ctx,
      env: this.env,
      prefix: this.prefix(),
      store,
      logger: this.logger,
    });
  }

  public async startHydration(options?: { dryRun?: boolean }): Promise<{
    queued: boolean;
    dryRun?: boolean;
    plan?: unknown;
    workId?: string;
    queueLength?: number;
  }> {
    await this.ensureAccessAndAlarm();
    const dry = options?.dryRun !== false;
    if (dry) {
      const plan = await summarizeHydrationPlan(this.ctx, this.env, this.prefix());
      return { queued: false, dryRun: true, plan };
    }
    const res = await enqueueHydrationTask(this.ctx, this.env, { dryRun: false });
    return { queued: true, dryRun: false, workId: res.workId, queueLength: res.queueLength };
  }

  public async clearHydration(): Promise<{
    clearedWork: boolean;
    clearedQueue: number;
    removedPacks: number;
  }> {
    await this.ensureAccessAndAlarm();
    return await clearHydrationState(this.ctx, this.env);
  }

  public async seedMinimalRepo(
    withPack: boolean = true
  ): Promise<{ commitOid: string; treeOid: string }> {
    await this.ensureAccessAndAlarm();
    return await seedMinimalRepoState({
      ctx: this.ctx,
      env: this.env,
      prefix: this.prefix(),
      withPack,
    });
  }

  public async putLooseObject(oid: string, zdata: Uint8Array): Promise<void> {
    await this.ensureAccessAndAlarm();
    if (!isValidOid(oid)) throw new Error("Bad oid");
    await storeObject(this.ctx, this.env, this.prefix(), oid, zdata);
  }

  public async purgeRepo(): Promise<{ deletedR2: number; deletedDO: boolean }> {
    await this.ensureAccessAndAlarm();
    return await purgeRepo(this.ctx, this.env);
  }

  public async removePack(packKey: string): Promise<{
    removed: boolean;
    deletedPack: boolean;
    deletedIndex: boolean;
    deletedMetadata: boolean;
    rejected?: "active-pack" | "non-superseded-pack";
    packState?: "active" | "superseded" | "unknown";
  }> {
    await this.ensureAccessAndAlarm();
    return await removePack(this.ctx, this.env, packKey);
  }
}
