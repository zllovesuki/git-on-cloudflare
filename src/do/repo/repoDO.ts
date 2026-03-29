import type { RepoStateSchema, Head, RepoStorageMode } from "./repoState.ts";
import type { UnpackProgress } from "@/common/index.ts";
import type { PackCatalogRow } from "./db/schema.ts";

import { DurableObject } from "cloudflare:workers";
import type { DrizzleSqliteDODatabase } from "drizzle-orm/durable-sqlite";
import { asTypedStorage, objKey } from "./repoState.ts";
import { doPrefix } from "@/keys.ts";
import { text, createLogger, isValidOid } from "@/common/index.ts";
import {
  enqueueHydrationTask,
  summarizeHydrationPlan,
  clearHydrationState,
} from "./hydration/index.ts";
import { ensureScheduled } from "./scheduler.ts";
import { purgeRepo, removePack } from "./packOperations.ts";
import {
  getObjectStream,
  getObject,
  hasLoose,
  hasLooseBatch,
  getObjectSize,
  storeObject,
  getObjectsBatch,
  getObjectRefsBatch,
} from "./storage.ts";
import { getPackLatest, getPacks, getPackOids, getPackOidsBatch } from "./packs.ts";
import {
  abortCompactionLease,
  abortReceiveLease,
  beginCompactionLease,
  beginReceiveLease,
  clearExpiredLeases,
  getActivePackCatalogSnapshot,
  getRepoStorageModeValue,
  setRepoStorageModeValue,
} from "./catalog.ts";
import { getRefs, setRefs, resolveHead, setHead, getHeadAndRefs } from "./refs.ts";
import { handleUnpackWork, getUnpackProgress } from "./unpack.ts";
import { handleIdleAndMaintenance } from "./maintenance.ts";
import { debugState, debugCheckCommit, debugCheckOid } from "./debug.ts";
import { migrate } from "drizzle-orm/durable-sqlite/migrator";
import { getDb } from "./db/index.ts";
import { migrateKvToSql } from "./db/migrate.ts";
import migrations from "@/drizzle/migrations";
import {
  ensureAccessAndAlarm,
  touchAndMaybeSchedule,
  type RepoDOAccessContext,
} from "./repoDO/access.ts";
import { handleReceiveRequest } from "./repoDO/receive.ts";
import { handleHydrationAlarmWork } from "./repoDO/hydration.ts";
import { seedMinimalRepoState } from "./repoDO/seeding.ts";

/**
 * Repository Durable Object (per-repo authority)
 *
 * Responsibilities
 * - Acts as the strongly consistent source of truth for a single repository
 * - Stores refs and HEAD in DO storage
 * - Caches loose objects (zlib-compressed) in DO storage
 * - Mirrors loose objects to R2 under `do/<id>/objects/loose/<oid>` for cheap reads
 * - Writes received packfiles to R2 under `do/<id>/objects/pack/*.pack` (and .idx)
 * - Exposes focused internal HTTP endpoints:
 *   - `POST /receive` — receive-pack implementation (delegates to git/operations/receive.ts)
 * - All other operations are provided as typed RPC methods on the class.
 *
 * Read Path (RPC)
 * - Loose object reads are exposed via RPC methods such as `getObjectStream()` and `getObject()`.
 * - Reads prefer R2 (range-friendly and cheap) and fall back to DO storage if missing.
 * - There is no public HTTP endpoint for object reads; this reduces the attack surface and
 *   keeps all internal state access typed and testable.
 *
 * Write Path
 * - Loose object writes: DO storage first, then mirror to R2 via `r2LooseKey()`.
 * - Pushes: `POST /receive` stores the raw `.pack` to R2 (under the DO prefix) and performs a
 *   fast index-only step to produce `.idx`. It then queues asynchronous unpack work which runs in
 *   small time-budgeted chunks under the DO `alarm()` (mirrors loose objects to R2 as it goes).
 *   Pack metadata is maintained to enable efficient fetch assembly.
 *
 * Maintenance & Background Work
 * - `alarm()` combines three duties:
 *   1) Unpack work: Process pending pack objects in time-limited chunks to avoid long blocking.
 *   2) Idle cleanup: If a repo looks empty and idle long enough, purge DO storage and its R2 prefix.
 *   3) Pack maintenance: Periodically prune old pack files in R2 and their metadata in DO.
 * - Listing/sweeping uses helpers from `keys.ts` to avoid path mismatches.
 */
export class RepoDurableObject extends DurableObject {
  declare env: Env;
  // Throttle lastAccessMs writes to storage to reduce per-request write amplification
  private lastAccessMemMs: number | undefined;
  private db: DrizzleSqliteDODatabase<any> | undefined;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => {
      this.lastAccessMemMs = await ctx.storage.get("lastAccessMs");
      this.db = getDb(ctx.storage);
      await migrate(this.db, migrations);
      await migrateKvToSql(this.ctx, this.db, this.logger);
      await this.ensureAccessAndAlarm();
    });
  }

  // Thin request router: delegates to focused handlers below.
  // Keep this mapping explicit and small so behavior is easy to audit.
  async fetch(request: Request): Promise<Response> {
    // Touch access and (re)schedule an idle cleanup alarm
    try {
      await touchAndMaybeSchedule(this.accessContext());
    } catch {}
    const url = new URL(request.url);
    this.logger.debug("fetch", { path: url.pathname, method: request.method });
    const store = asTypedStorage<RepoStateSchema>(this.ctx.storage);

    // Receive-pack: parse update commands section and packfile, store pack to R2,
    // update refs atomically if valid, and respond with report-status. This remains
    // on HTTP (instead of RPC) to preserve streaming semantics end-to-end without
    // buffering the pack in memory.
    if (url.pathname === "/receive" && request.method === "POST") {
      return this.handleReceive(request);
    }

    return text("Not found\n", 404);
  }

  // --- Alarm-based tasks ---
  // Combines three responsibilities:
  // 1) Unpack work: Process pending pack objects in chunks to avoid blocking
  // 2) Idle cleanup: If the DO remains idle beyond IDLE_MS and appears empty/unused, purge storage and R2 mirror.
  // 3) Maintenance: Periodically prune stale packs and metadata even for active repos.
  async alarm(): Promise<void> {
    const store = asTypedStorage<RepoStateSchema>(this.ctx.storage);
    this.logger.debug("alarm:start", {});

    await clearExpiredLeases(this.ctx, this.logger);

    // Priority 1: Handle pending unpack work
    if (await handleUnpackWork(this.ctx, this.env, this.prefix(), this.logger)) {
      return; // Exit early to let unpack continue
    }

    // Priority 2: Hydration work (resumable, time-sliced)
    if (await this.handleHydrationWork(store)) {
      return; // Exit early; hydration requested another slice soon
    }

    // Priority 3: Check for idle cleanup or maintenance needs
    await handleIdleAndMaintenance(this.ctx, this.env, this.logger);

    this.logger.debug("alarm:end", {});
  }

  private async touchAndMaybeSchedule(): Promise<void> {
    await touchAndMaybeSchedule(this.accessContext());
  }

  /**
   * Safe wrapper around touchAndMaybeSchedule() for RPC and internal entrypoints.
   * Ensures last access time is updated and an alarm is scheduled without
   * forcing every method to duplicate try/catch.
   */
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

  public async getObjectStream(oid: string): Promise<ReadableStream | null> {
    await this.ensureAccessAndAlarm();
    return await getObjectStream(this.ctx, this.env, this.prefix(), oid);
  }

  public async getObject(oid: string): Promise<ArrayBuffer | Uint8Array | null> {
    await this.ensureAccessAndAlarm();
    return await getObject(this.ctx, this.env, this.prefix(), oid);
  }

  public async hasLoose(oid: string): Promise<boolean> {
    await this.ensureAccessAndAlarm();
    return await hasLoose(this.ctx, this.env, this.prefix(), oid);
  }

  public async hasLooseBatch(oids: string[]): Promise<boolean[]> {
    await this.ensureAccessAndAlarm();
    return await hasLooseBatch(this.ctx, this.env, this.prefix(), oids, this.logger);
  }

  public async getPackLatest(): Promise<{ key: string; oids: string[] } | null> {
    await this.ensureAccessAndAlarm();
    return await getPackLatest(this.ctx);
  }

  public async getPacks(): Promise<string[]> {
    await this.ensureAccessAndAlarm();
    return await getPacks(this.ctx, this.env);
  }

  public async getPackOids(key: string): Promise<string[]> {
    await this.ensureAccessAndAlarm();
    return await getPackOids(this.ctx, key);
  }

  public async getPackOidsBatch(keys: string[]): Promise<Map<string, string[]>> {
    await this.ensureAccessAndAlarm();
    return await getPackOidsBatch(this.ctx, keys, this.logger);
  }

  public async getActivePackCatalog(): Promise<PackCatalogRow[]> {
    await this.ensureAccessAndAlarm();
    return await getActivePackCatalogSnapshot(this.ctx, this.env, this.prefix(), this.logger);
  }

  public async getRepoStorageMode(): Promise<RepoStorageMode> {
    await this.ensureAccessAndAlarm();
    return await getRepoStorageModeValue(this.ctx);
  }

  public async setRepoStorageMode(mode: RepoStorageMode): Promise<RepoStorageMode> {
    await this.ensureAccessAndAlarm();
    return await setRepoStorageModeValue(this.ctx, mode, this.logger);
  }

  public async beginReceive() {
    await this.ensureAccessAndAlarm();
    return await beginReceiveLease(this.ctx, this.env, this.prefix(), this.logger);
  }

  public async abortReceive(token: string): Promise<boolean> {
    await this.ensureAccessAndAlarm();
    return await abortReceiveLease(this.ctx, token);
  }

  public async beginCompaction() {
    await this.ensureAccessAndAlarm();
    return await beginCompactionLease(this.ctx, this.env, this.prefix(), this.logger);
  }

  public async abortCompaction(token: string): Promise<boolean> {
    await this.ensureAccessAndAlarm();
    return await abortCompactionLease(this.ctx, token);
  }

  public async getUnpackProgress(): Promise<UnpackProgress> {
    await this.ensureAccessAndAlarm();
    return await getUnpackProgress(this.ctx);
  }

  public async debugState(): Promise<ReturnType<typeof debugState>> {
    await this.ensureAccessAndAlarm();
    return await debugState(this.ctx, this.env);
  }

  public async debugCheckCommit(commit: string): Promise<ReturnType<typeof debugCheckCommit>> {
    await this.ensureAccessAndAlarm();
    return await debugCheckCommit(this.ctx, this.env, commit);
  }

  public async debugCheckOid(oid: string): Promise<ReturnType<typeof debugCheckOid>> {
    await this.ensureAccessAndAlarm();
    return await debugCheckOid(this.ctx, this.env, oid);
  }

  public async getObjectsBatch(oids: string[]): Promise<Map<string, Uint8Array | null>> {
    await this.ensureAccessAndAlarm();
    return await getObjectsBatch(this.ctx, oids);
  }

  public async getObjectRefsBatch(oids: string[]): Promise<Map<string, string[]>> {
    await this.ensureAccessAndAlarm();
    return await getObjectRefsBatch(this.ctx, oids, this.logger);
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
    // Tests and R2 layout expect Durable Object data under the 'do/<id>' prefix
    return doPrefix(this.ctx.id.toString());
  }

  private get logger() {
    return createLogger(this.env.LOG_LEVEL, {
      service: "RepoDO",
      doId: this.ctx.id.toString(),
    });
  }

  private async handleHydrationWork(
    store: ReturnType<typeof asTypedStorage<RepoStateSchema>>
  ): Promise<boolean> {
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
    const dry = options?.dryRun !== false; // default to dry-run when undefined
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
    // Delegate to hydration helper
    const res = await clearHydrationState(this.ctx, this.env);
    return res;
  }

  /**
   * RPC: Seed a minimal repository with an empty tree and a single commit pointing to it.
   * Used by tests to initialize a valid repo state without using HTTP fetch routes.
   *
   * @param withPack - If true, creates a pack file instead of loose objects (default: true for streaming compatibility)
   */
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

  /**
   * RPC: Store a loose object (zlib-compressed with Git header) by its OID.
   * Mirrors to R2 best-effort. Throws on invalid OID.
   */
  public async putLooseObject(oid: string, zdata: Uint8Array): Promise<void> {
    await this.ensureAccessAndAlarm();
    if (!isValidOid(oid)) throw new Error("Bad oid");
    await storeObject(this.ctx, this.env, this.prefix(), oid, zdata);
  }

  public async getObjectSize(oid: string): Promise<number | null> {
    await this.ensureAccessAndAlarm();
    return await getObjectSize(this.ctx, this.env, this.prefix(), oid);
  }

  /**
   * RPC: DANGEROUS - Completely purge this repository.
   * Deletes all R2 objects and all DO storage.
   */
  public async purgeRepo(): Promise<{ deletedR2: number; deletedDO: boolean }> {
    await this.ensureAccessAndAlarm();
    return await purgeRepo(this.ctx, this.env);
  }

  /**
   * RPC: Remove a specific pack file and its associated data
   * @param packKey - The pack key to remove
   */
  public async removePack(packKey: string): Promise<{
    removed: boolean;
    deletedPack: boolean;
    deletedIndex: boolean;
    deletedMetadata: boolean;
  }> {
    await this.ensureAccessAndAlarm();
    return await removePack(this.ctx, this.env, packKey);
  }
}
