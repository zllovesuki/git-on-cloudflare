import type { CacheContext } from "@/cache/index.ts";
import { createLogger } from "@/common/index.ts";
import { loadRepoStorageMode } from "./catalog.ts";
import { readObject } from "./store.ts";
import { logOnce, logPackedObjectMismatch } from "./support.ts";

export async function validatePackedObjectShadowRead(
  env: Env,
  repoId: string,
  oid: string,
  legacy: { type: string; payload: Uint8Array } | undefined,
  cacheCtx?: CacheContext
): Promise<void> {
  const log = createLogger(env.LOG_LEVEL, {
    service: "PackedObjectShadow",
    repoId,
  });
  const mode = await loadRepoStorageMode(env, repoId, cacheCtx);
  if (mode !== "shadow-read") return;
  logOnce(cacheCtx, "shadow-validate-logged", () => {
    log.debug("shadow:validate", { oid, legacyPresent: !!legacy });
  });

  let packed: Awaited<ReturnType<typeof readObject>> | undefined;
  try {
    packed = await readObject(env, repoId, oid, cacheCtx);
  } catch (error) {
    logPackedObjectMismatch({
      env,
      repoId,
      oid,
      reason: "packed-read-error",
      details: { error: String(error) },
    });
    return;
  }

  if (!legacy && !packed) return;
  if (!legacy && packed) {
    logPackedObjectMismatch({ env, repoId, oid, reason: "legacy-missing" });
    return;
  }
  if (legacy && !packed) {
    logPackedObjectMismatch({ env, repoId, oid, reason: "packed-missing" });
    return;
  }
  if (!legacy || !packed) return;
  if (legacy.type !== packed.type) {
    logPackedObjectMismatch({
      env,
      repoId,
      oid,
      reason: "type-mismatch",
      details: { legacyType: legacy.type, packedType: packed.type },
    });
    return;
  }
  if (legacy.payload.byteLength !== packed.payload.byteLength) {
    logPackedObjectMismatch({
      env,
      repoId,
      oid,
      reason: "size-mismatch",
      details: {
        legacyBytes: legacy.payload.byteLength,
        packedBytes: packed.payload.byteLength,
      },
    });
    return;
  }
  for (let i = 0; i < legacy.payload.byteLength; i++) {
    if (legacy.payload[i] !== packed.payload[i]) {
      logPackedObjectMismatch({
        env,
        repoId,
        oid,
        reason: "payload-mismatch",
        details: { offset: i },
      });
      return;
    }
  }
  logOnce(cacheCtx, "shadow-match-logged", () => {
    log.debug("shadow:match", {
      oid,
      type: packed.type,
      bytes: packed.payload.byteLength,
      packKey: packed.packKey,
    });
  });
}
