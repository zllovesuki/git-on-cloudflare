import type { DrizzleSqliteDODatabase } from "drizzle-orm/durable-sqlite";
import type { PackCatalogRow } from "../schema.ts";

import { desc, eq, inArray, sql } from "drizzle-orm";
import { packCatalog } from "../schema.ts";
import { SAFE_ROWS_1COL } from "./shared.ts";

export async function getPackCatalogCount(db: DrizzleSqliteDODatabase): Promise<number> {
  return await db.$count(packCatalog);
}

export async function listPackCatalog(db: DrizzleSqliteDODatabase): Promise<PackCatalogRow[]> {
  return await db
    .select()
    .from(packCatalog)
    .orderBy(desc(packCatalog.seqHi), desc(packCatalog.tier));
}

export async function listActivePackCatalog(
  db: DrizzleSqliteDODatabase
): Promise<PackCatalogRow[]> {
  return await db
    .select()
    .from(packCatalog)
    .where(eq(packCatalog.state, "active"))
    .orderBy(desc(packCatalog.seqHi), desc(packCatalog.tier));
}

export async function getPackCatalogRow(
  db: DrizzleSqliteDODatabase,
  packKey: string
): Promise<PackCatalogRow | undefined> {
  const rows = await db.select().from(packCatalog).where(eq(packCatalog.packKey, packKey)).limit(1);
  return rows[0];
}

export async function getPackCatalogSeqMax(db: DrizzleSqliteDODatabase): Promise<number> {
  const rows = await db
    .select({
      maxSeqHi: sql<number>`coalesce(max(${packCatalog.seqHi}), 0)`,
    })
    .from(packCatalog);
  return rows[0]?.maxSeqHi || 0;
}

export async function upsertPackCatalogRow(
  db: DrizzleSqliteDODatabase,
  row: PackCatalogRow
): Promise<void> {
  await db
    .insert(packCatalog)
    .values(row)
    .onConflictDoUpdate({
      target: packCatalog.packKey,
      set: {
        kind: row.kind,
        state: row.state,
        tier: row.tier,
        seqLo: row.seqLo,
        seqHi: row.seqHi,
        objectCount: row.objectCount,
        packBytes: row.packBytes,
        idxBytes: row.idxBytes,
        createdAt: row.createdAt,
        supersededBy: row.supersededBy,
      },
    });
}

export async function supersedePackCatalogRows(
  db: DrizzleSqliteDODatabase,
  packKeys: string[],
  supersededBy: string | null = null
): Promise<void> {
  if (!packKeys.length) return;
  for (let i = 0; i < packKeys.length; i += SAFE_ROWS_1COL) {
    const batch = packKeys.slice(i, i + SAFE_ROWS_1COL);
    await db
      .update(packCatalog)
      .set({ state: "superseded", supersededBy })
      .where(inArray(packCatalog.packKey, batch));
  }
}

export async function deletePackCatalogRows(
  db: DrizzleSqliteDODatabase,
  packKeys: string[]
): Promise<void> {
  if (!packKeys.length) return;
  for (let i = 0; i < packKeys.length; i += SAFE_ROWS_1COL) {
    const batch = packKeys.slice(i, i + SAFE_ROWS_1COL);
    await db.delete(packCatalog).where(inArray(packCatalog.packKey, batch));
  }
}
