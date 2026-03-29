import { it, expect } from "vitest";
import { SELF, env } from "cloudflare:test";
import type { RepoDurableObject } from "@/index";
import { pktLine, delimPkt, flushPkt, concatChunks, decodePktLines } from "@/git";
import { uniqueRepoId, runDOWithRetry } from "./util/test-helpers.ts";
import { gzip } from "@/common/index.ts";

function buildFetchBody({
  wants,
  haves,
  done,
}: {
  wants: string[];
  haves?: string[];
  done?: boolean;
}) {
  const chunks: Uint8Array[] = [];
  chunks.push(pktLine("command=fetch\n"));
  chunks.push(delimPkt());
  for (const w of wants) chunks.push(pktLine(`want ${w}\n`));
  for (const h of haves || []) chunks.push(pktLine(`have ${h}\n`));
  if (done) chunks.push(pktLine("done\n"));
  chunks.push(flushPkt());
  return concatChunks(chunks);
}

function buildLsRefsBody(args: string[] = []) {
  const chunks: Uint8Array[] = [];
  chunks.push(pktLine("command=ls-refs\n"));
  chunks.push(delimPkt());
  for (const a of args) chunks.push(pktLine(a + "\n"));
  chunks.push(flushPkt());
  return concatChunks(chunks);
}

it("upload-pack fetch accepts gzip-encoded request bodies", async () => {
  const owner = "o";
  const repo = uniqueRepoId("r-gzip-fetch");
  const repoId = `${owner}/${repo}`;
  const id = env.REPO_DO.idFromName(repoId);
  const { commitOid } = await runDOWithRetry(
    () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>,
    async (instance: RepoDurableObject) => instance.seedMinimalRepo()
  );

  const body = await gzip(buildFetchBody({ wants: [commitOid], done: true }));
  const url = `https://example.com/${owner}/${repo}/git-upload-pack`;
  const res = await SELF.fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-git-upload-pack-request",
      "Content-Encoding": "gzip",
      "Git-Protocol": "version=2",
    },
    body,
  } as any);

  expect(res.status).toBe(200);
  expect(res.headers.get("Content-Type") || "").toContain("git-upload-pack-result");
  const text = new TextDecoder().decode(new Uint8Array(await res.arrayBuffer()));
  expect(text.includes("packfile\n")).toBe(true);
});

it("upload-pack ls-refs accepts gzip-encoded request bodies", async () => {
  const owner = "o";
  const repo = uniqueRepoId("r-gzip-lsrefs");
  const body = await gzip(buildLsRefsBody(["ref-prefix refs/heads/"]));
  const url = `https://example.com/${owner}/${repo}/git-upload-pack`;
  const res = await SELF.fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-git-upload-pack-request",
      "Content-Encoding": "gzip",
      "Git-Protocol": "version=2",
    },
    body,
  } as any);

  expect(res.status).toBe(200);
  const bytes = new Uint8Array(await res.arrayBuffer());
  const lines = decodePktLines(bytes)
    .filter((i) => i.type === "line")
    .map((i: any) => i.text);
  expect(lines[0]).toBe("unborn HEAD symref-target:refs/heads/main\n");
});

it("upload-pack rejects unsupported content encodings", async () => {
  const owner = "o";
  const repo = uniqueRepoId("r-encoding-unsupported");
  const body = buildLsRefsBody(["ref-prefix refs/heads/"]);
  const url = `https://example.com/${owner}/${repo}/git-upload-pack`;
  const res = await SELF.fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-git-upload-pack-request",
      "Content-Encoding": "br",
      "Git-Protocol": "version=2",
    },
    body,
  } as any);

  expect(res.status).toBe(415);
  expect(await res.text()).toContain("Unsupported Content-Encoding");
});

it("upload-pack rejects invalid gzip payloads", async () => {
  const owner = "o";
  const repo = uniqueRepoId("r-encoding-invalid");
  const body = new Uint8Array([0x1f, 0x8b, 0x08, 0x00, 0x00]);
  const url = `https://example.com/${owner}/${repo}/git-upload-pack`;
  const res = await SELF.fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-git-upload-pack-request",
      "Content-Encoding": "gzip",
      "Git-Protocol": "version=2",
    },
    body,
  } as any);

  expect(res.status).toBe(400);
  expect(await res.text()).toContain("Invalid gzip request body");
});
