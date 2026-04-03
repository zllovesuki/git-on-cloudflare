import { it, expect } from "vitest";
import { env, SELF } from "cloudflare:test";
import type { RepoDurableObject } from "@/index";
import { pktLine, delimPkt, flushPkt, concatChunks } from "@/git";
import { uniqueRepoId, runDOWithRetry } from "./util/test-helpers.ts";

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

it("upload-pack fetch returns acknowledgments before the final packfile response", async () => {
  const owner = "o";
  const repo = uniqueRepoId("r");
  const repoId = `${owner}/${repo}`;
  // Seed tiny repo and get commit OID via DO instance
  const id = env.REPO_DO.idFromName(repoId);
  const { commitOid } = await runDOWithRetry(
    () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>,
    async (instance: RepoDurableObject) => instance.seedMinimalRepo()
  );

  const url = `https://example.com/${owner}/${repo}/git-upload-pack`;

  // Negotiation (done=false) should return only acknowledgments.
  const negotiateBody = buildFetchBody({ wants: [commitOid], done: false });
  const negotiateRes = await SELF.fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-git-upload-pack-request",
      "Git-Protocol": "version=2",
    },
    body: negotiateBody,
  } as any);
  expect(negotiateRes.status).toBe(200);
  const negotiateBytes = new Uint8Array(await negotiateRes.arrayBuffer());
  const negotiateText = new TextDecoder().decode(negotiateBytes);
  expect(negotiateText.includes("acknowledgments\n")).toBe(true);
  expect(negotiateText.includes("packfile\n")).toBe(false);

  // The final fetch (done=true) should return the packfile without acknowledgments.
  const fetchBody = buildFetchBody({ wants: [commitOid], done: true });
  const fetchRes = await SELF.fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-git-upload-pack-request",
      "Git-Protocol": "version=2",
    },
    body: fetchBody,
  } as any);
  expect(fetchRes.status).toBe(200);
  expect(fetchRes.headers.get("Content-Type") || "").toContain("git-upload-pack-result");
  const fetchBytes = new Uint8Array(await fetchRes.arrayBuffer());
  const fetchText = new TextDecoder().decode(fetchBytes);
  expect(fetchText.includes("acknowledgments\n")).toBe(false);
  expect(fetchText.includes("packfile\n")).toBe(true);
});
