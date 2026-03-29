import type { DrizzleSqliteDODatabase } from "drizzle-orm/durable-sqlite";

import { eq, inArray, sql } from "drizzle-orm";
import { packObjects } from "../schema.ts";
import { normalizePackKey, SAFE_ROWS_1COL, SAFE_ROWS_2COL } from "./shared.ts";

export async function oidExistsInPacks(db: DrizzleSqliteDODatabase, oid: string): Promise<boolean> {
  const result = await db
    .select({ packKey: packObjects.packKey })
    .from(packObjects)
    .where(eq(packObjects.oid, oid.toLowerCase()))
    .limit(1);
  return result.length > 0;
}

export async function getPackObjectCount(
  db: DrizzleSqliteDODatabase,
  packKey: string
): Promise<number> {
  const key = normalizePackKey(packKey);
  return await db.$count(packObjects, eq(packObjects.packKey, key));
}

export async function normalizePackKeysInPlace(
  db: DrizzleSqliteDODatabase,
  logger?: {
    debug?: (m: string, d?: Record<string, unknown>) => void;
    info?: (m: string, d?: Record<string, unknown>) => void;
    warn?: (m: string, d?: Record<string, unknown>) => void;
  }
): Promise<{ checked: number; updated: number }> {
  const rows = await db
    .select({ packKey: packObjects.packKey })
    .from(packObjects)
    .where(sql`instr(${packObjects.packKey}, '/') > 0`)
    .groupBy(packObjects.packKey);

  let updated = 0;
  for (const row of rows) {
    const oldKey = row.packKey;
    const newKey = normalizePackKey(oldKey);
    if (newKey === oldKey) continue;
    try {
      await db.update(packObjects).set({ packKey: newKey }).where(eq(packObjects.packKey, oldKey));
      updated++;
    } catch (error) {
      logger?.warn?.("normalize:packKey:update-failed", {
        oldKey,
        newKey,
        error: String(error),
      });
    }
  }

  if (updated > 0) logger?.info?.("normalize:packKey:updated", { updated, checked: rows.length });
  else logger?.debug?.("normalize:packKey:noop", { checked: rows.length });

  return { checked: rows.length, updated };
}

export async function findPacksContainingOid(
  db: DrizzleSqliteDODatabase,
  oid: string
): Promise<string[]> {
  const rows = await db
    .select({ packKey: packObjects.packKey })
    .from(packObjects)
    .where(eq(packObjects.oid, oid.toLowerCase()));
  return rows.map((row) => row.packKey);
}

export async function filterOidsInPacks(
  db: DrizzleSqliteDODatabase,
  oids: string[]
): Promise<Set<string>> {
  if (oids.length === 0) return new Set();

  const normalizedOids = oids.map((oid) => oid.toLowerCase());
  const found = new Set<string>();

  for (let i = 0; i < normalizedOids.length; i += SAFE_ROWS_2COL) {
    const batch = normalizedOids.slice(i, i + SAFE_ROWS_2COL);
    const rows = await db
      .select({ oid: packObjects.oid })
      .from(packObjects)
      .where(inArray(packObjects.oid, batch));
    for (const row of rows) found.add(row.oid);
  }

  return found;
}

export async function getPackOids(db: DrizzleSqliteDODatabase, packKey: string): Promise<string[]> {
  const key = normalizePackKey(packKey);
  const rows = await db
    .select({ oid: packObjects.oid })
    .from(packObjects)
    .where(eq(packObjects.packKey, key));
  return rows.map((row) => row.oid);
}

export async function getPackOidsSlice(
  db: DrizzleSqliteDODatabase,
  packKey: string,
  offset: number,
  limit: number
): Promise<string[]> {
  if (limit <= 0) return [];
  const key = normalizePackKey(packKey);
  const rows = await db
    .select({ oid: packObjects.oid })
    .from(packObjects)
    .where(eq(packObjects.packKey, key))
    .orderBy(packObjects.oid)
    .limit(limit)
    .offset(offset);
  return rows.map((row) => row.oid);
}

export async function getPackOidsBatch(
  db: DrizzleSqliteDODatabase,
  packKeys: string[]
): Promise<Map<string, string[]>> {
  if (packKeys.length === 0) return new Map();

  const result = new Map<string, string[]>();
  for (const originalKey of packKeys) result.set(originalKey, []);

  const normalizedToOriginal = new Map<string, string[]>();
  for (const originalKey of packKeys) {
    const normalizedKey = normalizePackKey(originalKey);
    const originals = normalizedToOriginal.get(normalizedKey) || [];
    originals.push(originalKey);
    normalizedToOriginal.set(normalizedKey, originals);
  }

  const normalizedKeys = Array.from(normalizedToOriginal.keys());
  for (let i = 0; i < normalizedKeys.length; i += SAFE_ROWS_1COL) {
    const batch = normalizedKeys.slice(i, i + SAFE_ROWS_1COL);
    const rows = await db
      .select({ packKey: packObjects.packKey, oid: packObjects.oid })
      .from(packObjects)
      .where(inArray(packObjects.packKey, batch));

    const grouped = new Map<string, string[]>();
    for (const row of rows) {
      const oids = grouped.get(row.packKey) || [];
      oids.push(row.oid);
      grouped.set(row.packKey, oids);
    }

    for (const normalizedKey of batch) {
      const oids = grouped.get(normalizedKey) || [];
      const originals = normalizedToOriginal.get(normalizedKey) || [];
      for (const originalKey of originals) {
        result.set(originalKey, oids.slice(0));
      }
    }
  }

  return result;
}

export async function insertPackOids(
  db: DrizzleSqliteDODatabase,
  packKey: string,
  oids: readonly string[]
): Promise<void> {
  if (!oids.length) return;
  const key = normalizePackKey(packKey);
  for (let i = 0; i < oids.length; i += SAFE_ROWS_2COL) {
    const rows = oids
      .slice(i, i + SAFE_ROWS_2COL)
      .map((oid) => ({ packKey: key, oid: String(oid).toLowerCase() }));
    if (rows.length > 0) await db.insert(packObjects).values(rows).onConflictDoNothing();
  }
}

export async function deletePackObjects(
  db: DrizzleSqliteDODatabase,
  packKey: string
): Promise<void> {
  const key = normalizePackKey(packKey);
  await db.delete(packObjects).where(eq(packObjects.packKey, key));
}
