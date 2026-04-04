import { it, expect } from "vitest";
import { SELF } from "cloudflare:test";
import { uniqueRepoId } from "./util/test-helpers.ts";
import { decodePktLines } from "@/git";
import { seedPackFirstRepo } from "./util/pack-first.ts";

it("advertises streaming receive-pack capabilities including side-band-64k", async () => {
  const owner = "o";
  const repo = uniqueRepoId("r-recv-adv");
  const repoId = `${owner}/${repo}`;

  await seedPackFirstRepo(repoId);

  const url = new URL(`https://example.com/${owner}/${repo}/info/refs`);
  url.searchParams.set("service", "git-receive-pack");

  const res = await SELF.fetch(new Request(url, { method: "GET" }));
  expect(res.status).toBe(200);
  expect(res.headers.get("Content-Type")).toContain("git-receive-pack-advertisement");

  const bytes = new Uint8Array(await res.arrayBuffer());
  const items = decodePktLines(bytes);
  const lines = items.filter((i) => i.type === "line").map((i: any) => i.text);

  // First line should be the prelude
  expect(lines[0]).toBe("# service=git-receive-pack\n");

  // Capabilities are on the first ref line after the flush
  const capsLine = lines.find((l) => l.includes("\0")) || "";
  expect(capsLine).toContain("atomic");
  expect(capsLine).toContain("report-status");
  expect(capsLine).toContain("ofs-delta");
  expect(capsLine).toContain("agent=git-on-cloudflare/0.1");
  // Streaming capabilities are always advertised
  expect(capsLine).toContain("side-band-64k");
  expect(capsLine).toContain("quiet");
});
