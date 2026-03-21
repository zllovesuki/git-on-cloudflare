import test from "ava";
import { formatWhen } from "@/web/index.ts";

const SAMPLE_EPOCH = 1774136183; // 2026-03-21 23:36:23 UTC

test("formatWhen renders commit-local time for negative offsets", (t) => {
  t.is(formatWhen(SAMPLE_EPOCH, "-0700"), "2026-03-21 16:36:23 -0700");
});

test("formatWhen renders commit-local time for positive offsets", (t) => {
  t.is(formatWhen(SAMPLE_EPOCH, "+0530"), "2026-03-22 05:06:23 +0530");
});

test("formatWhen falls back to UTC when offset is malformed", (t) => {
  t.is(formatWhen(SAMPLE_EPOCH, "UTC"), "2026-03-21 23:36:23 UTC");
});
