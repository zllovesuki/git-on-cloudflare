import { describe, expect, it } from "vitest";

import {
  isValidSlug,
  namespaceSlugCandidates,
  normalizeSlugLabel,
  RESERVED_SLUGS,
  validateSlugForRoute,
} from "@/shared/slugs";

describe("isValidSlug", () => {
  it("accepts simple lowercase ASCII slugs", () => {
    expect(isValidSlug("rachel")).toBe(true);
    expect(isValidSlug("git-on-cloudflare")).toBe(true);
    expect(isValidSlug("a")).toBe(true);
    expect(isValidSlug("a1")).toBe(true);
  });

  it("rejects empty, too long, and bad-shape slugs", () => {
    expect(isValidSlug("")).toBe(false);
    expect(isValidSlug("-leading")).toBe(false);
    expect(isValidSlug("trailing-")).toBe(false);
    expect(isValidSlug("Two--Caps")).toBe(false);
    expect(isValidSlug("space inside")).toBe(false);
    expect(isValidSlug("a".repeat(41))).toBe(false);
    expect(isValidSlug("émigré")).toBe(false);
  });

  it("rejects every reserved slug", () => {
    for (const reserved of RESERVED_SLUGS) {
      expect(isValidSlug(reserved)).toBe(false);
    }
  });
});

describe("validateSlugForRoute", () => {
  it("reports length, reserved, and format reasons distinctly", () => {
    expect(validateSlugForRoute("")).toEqual({ ok: false, reason: "length" });
    expect(validateSlugForRoute("auth")).toEqual({ ok: false, reason: "reserved" });
    expect(validateSlugForRoute("BadCase")).toEqual({ ok: false, reason: "format" });
    expect(validateSlugForRoute("ok-slug")).toEqual({ ok: true, slug: "ok-slug" });
  });
});

describe("normalizeSlugLabel", () => {
  it("lowercases and converts separators to single dashes", () => {
    expect(normalizeSlugLabel("Pei Li Philip")).toBe("pei-li-philip");
    expect(normalizeSlugLabel("pei.li_philip+git")).toBe("pei-li-philip-git");
    expect(normalizeSlugLabel("--Weird__Name--")).toBe("weird-name");
  });

  it("strips diacritics and bounds the length", () => {
    expect(normalizeSlugLabel("émigré")).toBe("emigre");
    expect(normalizeSlugLabel("a".repeat(50))).toBe("a".repeat(40));
  });

  it("returns null when nothing slug-shaped remains", () => {
    expect(normalizeSlugLabel("")).toBeNull();
    expect(normalizeSlugLabel("___")).toBeNull();
    expect(normalizeSlugLabel("!!!")).toBeNull();
  });
});

describe("namespaceSlugCandidates", () => {
  it("prefers the unsuffixed slug and appends numeric fallbacks", () => {
    expect(namespaceSlugCandidates("pei.li.philip")).toEqual([
      "pei-li-philip",
      "pei-li-philip-2",
      "pei-li-philip-3",
      "pei-li-philip-4",
      "pei-li-philip-5",
    ]);
  });

  it("skips reserved base slugs but keeps valid suffixed variants", () => {
    expect(namespaceSlugCandidates("auth", 2)).toEqual(["auth-2", "auth-3"]);
  });

  it("keeps suffixed candidates within the length limit", () => {
    const candidates = namespaceSlugCandidates("a".repeat(40));
    expect(candidates.every((slug) => isValidSlug(slug))).toBe(true);
    expect(candidates).toHaveLength(5);
  });

  it("returns no candidates for unusable labels", () => {
    expect(namespaceSlugCandidates("")).toEqual([]);
    expect(namespaceSlugCandidates("!!!")).toEqual([]);
  });
});
