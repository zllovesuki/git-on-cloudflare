import type { DrizzleSqliteDODatabase } from "drizzle-orm/durable-sqlite";

import { and, eq, inArray } from "drizzle-orm";
import { hydrCover, hydrPending } from "../schema.ts";
import { SAFE_ROWS_1COL, SAFE_ROWS_2COL, SAFE_ROWS_3COL } from "./shared.ts";

export async function insertHydrCoverOids(
  db: DrizzleSqliteDODatabase,
  workId: string,
  oids: readonly string[]
): Promise<void> {
  if (!oids.length) return;
  for (let i = 0; i < oids.length; i += SAFE_ROWS_2COL) {
    const rows = oids
      .slice(i, i + SAFE_ROWS_2COL)
      .map((oid) => ({ workId, oid: String(oid).toLowerCase() }));
    if (rows.length > 0) await db.insert(hydrCover).values(rows).onConflictDoNothing();
  }
}

export async function insertHydrPendingOids(
  db: DrizzleSqliteDODatabase,
  workId: string,
  kind: "base" | "loose",
  oids: readonly string[]
): Promise<void> {
  if (!oids.length) return;
  for (let i = 0; i < oids.length; i += SAFE_ROWS_3COL) {
    const rows = oids
      .slice(i, i + SAFE_ROWS_3COL)
      .map((oid) => ({ workId, kind, oid: String(oid).toLowerCase() }));
    if (rows.length > 0) await db.insert(hydrPending).values(rows).onConflictDoNothing();
  }
}

export async function getHydrPendingOids(
  db: DrizzleSqliteDODatabase,
  workId: string,
  kind: "base" | "loose",
  limit?: number
): Promise<string[]> {
  const query = db
    .select({ oid: hydrPending.oid })
    .from(hydrPending)
    .where(and(eq(hydrPending.workId, workId), eq(hydrPending.kind, kind)))
    .orderBy(hydrPending.oid);

  if (limit && limit > 0) query.limit(limit);

  const rows = await query;
  return rows.map((row) => row.oid);
}

export async function hasHydrCoverForWork(
  db: DrizzleSqliteDODatabase,
  workId: string
): Promise<boolean> {
  return (await db.$count(hydrCover, eq(hydrCover.workId, workId))) > 0;
}

export async function filterUncoveredAgainstHydrCover(
  db: DrizzleSqliteDODatabase,
  workId: string,
  candidates: string[]
): Promise<string[]> {
  if (!candidates.length) return [];
  const out: string[] = [];
  for (let i = 0; i < candidates.length; i += SAFE_ROWS_1COL) {
    const batch = candidates.slice(i, i + SAFE_ROWS_1COL).map((oid) => oid.toLowerCase());
    const rows = await db
      .select({ oid: hydrCover.oid })
      .from(hydrCover)
      .where(and(eq(hydrCover.workId, workId), inArray(hydrCover.oid, batch)));
    const covered = new Set(rows.map((row) => row.oid));
    for (const oid of batch) {
      if (!covered.has(oid)) out.push(oid);
    }
  }
  return out;
}

export async function getHydrPendingCounts(
  db: DrizzleSqliteDODatabase,
  workId: string
): Promise<{ bases: number; loose: number }> {
  const bases = await db.$count(
    hydrPending,
    and(eq(hydrPending.workId, workId), eq(hydrPending.kind, "base"))
  );
  const loose = await db.$count(
    hydrPending,
    and(eq(hydrPending.workId, workId), eq(hydrPending.kind, "loose"))
  );
  return { bases, loose };
}

export async function deleteHydrPendingOids(
  db: DrizzleSqliteDODatabase,
  workId: string,
  kind: "base" | "loose",
  oids: string[]
): Promise<void> {
  if (!oids.length) return;
  for (let i = 0; i < oids.length; i += SAFE_ROWS_3COL) {
    const batch = oids.slice(i, i + SAFE_ROWS_3COL).map((oid) => oid.toLowerCase());
    await db
      .delete(hydrPending)
      .where(
        and(
          eq(hydrPending.workId, workId),
          eq(hydrPending.kind, kind),
          inArray(hydrPending.oid, batch)
        )
      );
  }
}

export async function clearHydrPending(db: DrizzleSqliteDODatabase, workId: string): Promise<void> {
  await db.delete(hydrPending).where(eq(hydrPending.workId, workId));
}

export async function clearHydrCover(db: DrizzleSqliteDODatabase, workId: string): Promise<void> {
  await db.delete(hydrCover).where(eq(hydrCover.workId, workId));
}

export async function getHydrCoverCount(
  db: DrizzleSqliteDODatabase,
  workId: string
): Promise<number> {
  return await db.$count(hydrCover, eq(hydrCover.workId, workId));
}

export async function getHydrCoverOids(
  db: DrizzleSqliteDODatabase,
  workId: string,
  limit?: number
): Promise<string[]> {
  const query = db
    .select({ oid: hydrCover.oid })
    .from(hydrCover)
    .where(eq(hydrCover.workId, workId))
    .orderBy(hydrCover.oid);

  if (limit && limit > 0) query.limit(limit);

  const rows = await query;
  return rows.map((row) => row.oid);
}
