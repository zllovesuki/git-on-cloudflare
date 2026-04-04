import { sql, desc } from "drizzle-orm";
import { sqliteTable, text, primaryKey, index, check, integer } from "drizzle-orm/sqlite-core";

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

export type PackCatalogRow = typeof packCatalog.$inferSelect;
