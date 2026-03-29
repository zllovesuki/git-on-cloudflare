CREATE TABLE `pack_catalog` (
	`pack_key` text PRIMARY KEY NOT NULL,
	`kind` text NOT NULL,
	`state` text NOT NULL,
	`tier` integer NOT NULL,
	`seq_lo` integer NOT NULL,
	`seq_hi` integer NOT NULL,
	`object_count` integer NOT NULL,
	`pack_bytes` integer NOT NULL,
	`idx_bytes` integer NOT NULL,
	`created_at` integer NOT NULL,
	`superseded_by` text,
	CONSTRAINT "chk_pack_catalog_kind" CHECK("kind" IN ('receive','compact','legacy')),
	CONSTRAINT "chk_pack_catalog_state" CHECK("state" IN ('active','superseded')),
	CONSTRAINT "chk_pack_catalog_tier" CHECK("tier" >= 0),
	CONSTRAINT "chk_pack_catalog_seq" CHECK("seq_lo" <= "seq_hi"),
	CONSTRAINT "chk_pack_catalog_object_count" CHECK("object_count" >= 0),
	CONSTRAINT "chk_pack_catalog_pack_bytes" CHECK("pack_bytes" >= 0),
	CONSTRAINT "chk_pack_catalog_idx_bytes" CHECK("idx_bytes" >= 0)
);
--> statement-breakpoint
CREATE INDEX `idx_pack_catalog_state_seqhi` ON `pack_catalog` (`state`,"seq_hi" desc);--> statement-breakpoint
CREATE INDEX `idx_pack_catalog_state_tier_seqlo` ON `pack_catalog` (`state`,`tier`,`seq_lo`);