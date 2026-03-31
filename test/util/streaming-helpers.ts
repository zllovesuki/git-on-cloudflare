import { expect } from "vitest";
import { SELF } from "cloudflare:test";

import { concatChunks, flushPkt, pktLine, decodePktLines } from "@/git/core/index.ts";
import { encodeGitObject } from "@/git/core/objects.ts";
import { buildPack } from "./git-pack.ts";
import { buildTreePayload } from "./packed-repo.ts";

export async function promoteToStreaming(owner: string, repo: string) {
  const shadow = await SELF.fetch(`https://example.com/${owner}/${repo}/admin/storage-mode`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ mode: "shadow-read" }),
  });
  expect(shadow.status).toBe(200);

  const streaming = await SELF.fetch(`https://example.com/${owner}/${repo}/admin/storage-mode`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ mode: "streaming" }),
  });
  expect(streaming.status).toBe(200);
}

export function decodeReportStatus(responseBody: Uint8Array): string[] {
  return decodePktLines(responseBody)
    .filter((item) => item.type === "line")
    .map((item: any) => String(item.text).trim());
}

export async function pushStreamingUpdate(
  owner: string,
  repo: string,
  parentOid: string,
  nextText: string
): Promise<{
  commitOid: string;
  blob: { oid: string };
  tree: { oid: string };
  commit: { oid: string };
}> {
  const author = "You <you@example.com> 0 +0000";
  const blobPayload = new TextEncoder().encode(nextText);
  const blob = await encodeGitObject("blob", blobPayload);
  const treePayload = buildTreePayload([{ mode: "100644", name: "README.md", oid: blob.oid }]);
  const tree = await encodeGitObject("tree", treePayload);
  const commitPayload = new TextEncoder().encode(
    `tree ${tree.oid}\n` +
      `parent ${parentOid}\n` +
      `author ${author}\n` +
      `committer ${author}\n\n` +
      `streaming update\n`
  );
  const commit = await encodeGitObject("commit", commitPayload);
  const pack = await buildPack([
    { type: "blob", payload: blobPayload },
    { type: "tree", payload: treePayload },
    { type: "commit", payload: commitPayload },
  ]);

  const response = await SELF.fetch(`https://example.com/${owner}/${repo}/git-receive-pack`, {
    method: "POST",
    headers: { "Content-Type": "application/x-git-receive-pack-request" },
    body: concatChunks([
      pktLine(`${parentOid} ${commit.oid} refs/heads/main\0 report-status ofs-delta agent=test\n`),
      flushPkt(),
      pack,
    ]),
  } as any);
  expect(response.status).toBe(200);
  expect(decodeReportStatus(new Uint8Array(await response.arrayBuffer()))).toContain(
    "ok refs/heads/main"
  );
  return { commitOid: commit.oid, blob, tree, commit };
}
