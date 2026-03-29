import { sql, desc } from "drizzle-orm";
import { sqliteTable, text, primaryKey, index, check, integer } from "drizzle-orm/sqlite-core";

// Table: pack_objects
// Stores exact membership of OIDs per pack key.
// NOTE: pack_key stores only the pack basename (e.g., "pack-12345.pack"),
// not the full R2 path. See DAL normalizePackKey() and migration that
// rewrites legacy full paths to basenames to reduce storage usage.
export const packObjects = sqliteTable(
  "pack_objects",
  {
    packKey: text("pack_key").notNull(),
    oid: text("oid").notNull(),
  },
  (t) => [
    primaryKey({ columns: [t.packKey, t.oid], name: "pack_objects_pk" }),
    index("idx_pack_objects_oid").on(t.oid),
  ]
);

// Table: hydr_cover
// Stores coverage set per hydration work id
export const hydrCover = sqliteTable(
  "hydr_cover",
  {
    workId: text("work_id").notNull(),
    oid: text("oid").notNull(),
  },
  (t) => [
    primaryKey({ columns: [t.workId, t.oid], name: "hydr_cover_pk" }),
    index("idx_hydr_cover_oid").on(t.oid),
  ]
);

// Table: hydr_pending
// Stores pending OIDs to be hydrated per work id
export const hydrPending = sqliteTable(
  "hydr_pending",
  {
    workId: text("work_id").notNull(),
    kind: text("kind").notNull(), // 'base' or 'loose'
    oid: text("oid").notNull(),
  },
  (t) => [
    primaryKey({ columns: [t.workId, t.kind, t.oid], name: "hydr_pending_pk" }),
    index("idx_hydr_pending_work_kind").on(t.workId, t.kind),
    check("chk_hydr_pending_kind", sql`"kind" IN ('base','loose')`),
  ]
);

export const packCatalog = sqliteTable(
  "pack_catalog",
  {
    packKey: text("pack_key").notNull(),
    kind: text("kind").notNull(),
    state: text("state").notNull(),
    tier: integer("tier").notNull(),
    seqLo: integer("seq_lo").notNull(),
    seqHi: integer("seq_hi").notNull(),
    objectCount: integer("object_count").notNull(),
    packBytes: integer("pack_bytes").notNull(),
    idxBytes: integer("idx_bytes").notNull(),
    createdAt: integer("created_at").notNull(),
    supersededBy: text("superseded_by"),
  },
  (t) => [
    primaryKey({ columns: [t.packKey], name: "pack_catalog_pk" }),
    index("idx_pack_catalog_state_seqhi").on(t.state, desc(t.seqHi)),
    index("idx_pack_catalog_state_tier_seqlo").on(t.state, t.tier, t.seqLo),
    check("chk_pack_catalog_kind", sql`"kind" IN ('receive','compact','legacy')`),
    check("chk_pack_catalog_state", sql`"state" IN ('active','superseded')`),
    check("chk_pack_catalog_tier", sql`"tier" >= 0`),
    check("chk_pack_catalog_seq", sql`"seq_lo" <= "seq_hi"`),
    check("chk_pack_catalog_object_count", sql`"object_count" >= 0`),
    check("chk_pack_catalog_pack_bytes", sql`"pack_bytes" >= 0`),
    check("chk_pack_catalog_idx_bytes", sql`"idx_bytes" >= 0`),
  ]
);

export type PackObjectsRow = typeof packObjects.$inferSelect;
export type HydrCoverRow = typeof hydrCover.$inferSelect;
export type HydrPendingRow = typeof hydrPending.$inferSelect;
export type PackCatalogRow = typeof packCatalog.$inferSelect;
