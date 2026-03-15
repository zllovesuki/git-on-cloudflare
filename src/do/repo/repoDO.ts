import type { RepoStateSchema, Head } from "./repoState.ts";
import type { UnpackProgress } from "@/common/index.ts";

import { DurableObject } from "cloudflare:workers";
import type { DrizzleSqliteDODatabase } from "drizzle-orm/durable-sqlite";
import { asTypedStorage, objKey } from "./repoState.ts";
import { doPrefix } from "@/keys.ts";
import {
  encodeGitObjectAndDeflate,
  receivePack,
  buildPackV2,
  parseGitObject,
  indexPackOnly,
} from "@/git/index.ts";
import {
  text,
  createLogger,
  isValidOid,
  bytesToHex,
  createInflateStream,
  createBlobFromBytes,
} from "@/common/index.ts";
import { r2PackKey } from "@/keys.ts";
import {
  enqueueHydrationTask,
  processHydrationSlice,
  summarizeHydrationPlan,
  clearHydrationState,
} from "./hydration.ts";
import { ensureScheduled, scheduleAlarmIfSooner } from "./scheduler.ts";
import { getConfig } from "./repoConfig.ts";
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
import { getRefs, setRefs, resolveHead, setHead, getHeadAndRefs } from "./refs.ts";
import { handleUnpackWork, getUnpackProgress } from "./unpack.ts";
import { handleIdleAndMaintenance } from "./maintenance.ts";
import { debugState, debugCheckCommit, debugCheckOid } from "./debug.ts";
import { migrate } from "drizzle-orm/durable-sqlite/migrator";
import { getDb, insertPackOids } from "./db/index.ts";
import { migrateKvToSql } from "./db/migrate.ts";
import migrations from "@/drizzle/migrations";

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
      await this.touchAndMaybeSchedule();
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
    const cfg = getConfig(this.env);
    const now = Date.now();
    const store = asTypedStorage<RepoStateSchema>(this.ctx.storage);

    // Update last access time with throttling (max once per 60s)
    try {
      if (!this.lastAccessMemMs || now - this.lastAccessMemMs >= 60_000) {
        await store.put("lastAccessMs", now);
        this.lastAccessMemMs = now;
      }
    } catch {}

    await ensureScheduled(this.ctx, this.env, now);
  }

  /**
   * Safe wrapper around touchAndMaybeSchedule() for RPC and internal entrypoints.
   * Ensures last access time is updated and an alarm is scheduled without
   * forcing every method to duplicate try/catch.
   */
  private async ensureAccessAndAlarm(): Promise<void> {
    try {
      await this.touchAndMaybeSchedule();
    } catch (e) {
      try {
        this.logger.warn("touch:schedule:failed", { error: String(e) });
      } catch {}
    }
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
    // Delegate to extracted implementation for clarity and testability.
    this.logger.info("receive:start", {});
    // Pre-body guard: block when current unpack is running and a next pack is already queued
    try {
      const store = asTypedStorage<RepoStateSchema>(this.ctx.storage);
      const work = await store.get("unpackWork");
      const next = await store.get("unpackNext");
      if (work && next) {
        this.logger.warn("receive:block-busy", { retryAfter: 10 });
        return new Response("Repository is busy unpacking; please retry shortly.\n", {
          status: 503,
          headers: {
            "Retry-After": "10",
            "Content-Type": "text/plain; charset=utf-8",
          },
        });
      }
    } catch {}
    const res = await receivePack(this.ctx, this.env, this.prefix(), request);
    this.logger.info("receive:end", { status: res.status });
    return res;
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
    try {
      const work = await store.get("hydrationWork");
      const queue = await store.get("hydrationQueue");
      const hasQueue = Array.isArray(queue) ? queue.length > 0 : !!queue;
      if (!work && !hasQueue) return false;
      const cont = await processHydrationSlice(this.ctx, this.env, this.prefix());
      if (cont) return true;
      return false;
    } catch (e) {
      this.logger.error("alarm:hydration:error", { error: String(e) });
      await scheduleAlarmIfSooner(this.ctx, this.env, Date.now() + 1000);
      return true;
    }
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
    const store = asTypedStorage<RepoStateSchema>(this.ctx.storage);
    const db = getDb(this.ctx.storage);
    // Build empty tree object (content is empty)
    const treeContent = new Uint8Array(0);
    const { oid: treeOid, zdata: treeZ } = await encodeGitObjectAndDeflate("tree", treeContent);

    // Build a simple commit pointing to empty tree
    const author = `You <you@example.com> 0 +0000`;
    const committer = author;
    const msg = "initial\n";
    const commitPayload =
      `tree ${treeOid}\n` + `author ${author}\n` + `committer ${committer}\n` + `\n${msg}`;
    const { oid: commitOid, zdata: commitZ } = await encodeGitObjectAndDeflate(
      "commit",
      new TextEncoder().encode(commitPayload)
    );

    if (withPack) {
      // Create a real pack file in R2 containing the tree and commit
      // This ensures streaming fetch operations will work correctly

      // First, decompress the objects to get their raw payloads
      const treeStream = createBlobFromBytes(treeZ).stream().pipeThrough(createInflateStream());
      const treeRaw = new Uint8Array(await new Response(treeStream).arrayBuffer());
      const treeParsed = parseGitObject(treeRaw);

      const commitStream = createBlobFromBytes(commitZ).stream().pipeThrough(createInflateStream());
      const commitRaw = new Uint8Array(await new Response(commitStream).arrayBuffer());
      const commitParsed = parseGitObject(commitRaw);

      // Build a pack file with these objects
      const packData = await buildPackV2([
        { type: treeParsed.type, payload: treeParsed.payload },
        { type: commitParsed.type, payload: commitParsed.payload },
      ]);

      // Generate pack key with proper R2 prefix
      const packFileName = `pack-test-${Date.now()}.pack`;
      const packKey = r2PackKey(this.prefix(), packFileName);

      // Store the actual pack file in R2
      await this.env.REPO_BUCKET.put(packKey, packData);

      // Create and store the index file
      const packOids = await indexPackOnly(packData, this.env, packKey, this.ctx, this.prefix());

      // Store pack metadata in DO storage (use full R2 key like receive.ts does)
      await store.put("lastPackKey", packKey); // Store the full R2 key
      await store.put("lastPackOids", packOids);
      await store.put("packList", [packKey]);

      // Also persist pack membership into SQLite for consistency with runtime paths
      try {
        await insertPackOids(db, packKey, packOids);
      } catch {}

      // Also store objects as loose in DO for direct access
      await store.put(objKey(treeOid), treeZ);
      await store.put(objKey(commitOid), commitZ);
    } else {
      // Store as loose objects only (legacy behavior)
      await store.put(objKey(treeOid), treeZ);
      await store.put(objKey(commitOid), commitZ);
    }

    // Update refs
    await store.put("refs", [{ name: "refs/heads/main", oid: commitOid }]);
    await store.put("head", { target: "refs/heads/main" });

    return { treeOid, commitOid };
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
