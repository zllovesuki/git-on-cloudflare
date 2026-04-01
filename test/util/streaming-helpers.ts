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

export function decodeReceiveSideband(responseBody: Uint8Array): {
  progress: string[];
  fatal: string[];
  reportStatus: string[];
} {
  const progress: string[] = [];
  const fatal: string[] = [];
  const reportStatusChunks: Uint8Array[] = [];

  for (const item of decodePktLines(responseBody)) {
    if (item.type !== "line") continue;
    const raw = item.raw;
    const band = raw[0];
    const payload = raw.subarray(1);

    if (band === 1) {
      reportStatusChunks.push(payload);
      continue;
    }
    if (band === 2) {
      progress.push(new TextDecoder().decode(payload));
      continue;
    }
    if (band === 3) {
      fatal.push(new TextDecoder().decode(payload));
    }
  }

  const reportStatus =
    reportStatusChunks.length > 0 ? decodeReportStatus(concatChunks(reportStatusChunks)) : [];

  return { progress, fatal, reportStatus };
}

export async function buildStreamingReceiveBody(args: {
  parentOid: string;
  nextText: string;
  commitMessage: string;
  capabilities: string;
}): Promise<{
  body: Uint8Array;
  blob: { oid: string };
  tree: { oid: string };
  commit: { oid: string };
}> {
  const author = "You <you@example.com> 0 +0000";
  const blobPayload = new TextEncoder().encode(args.nextText);
  const blob = await encodeGitObject("blob", blobPayload);
  const treePayload = buildTreePayload([{ mode: "100644", name: "README.md", oid: blob.oid }]);
  const tree = await encodeGitObject("tree", treePayload);
  const commitPayload = new TextEncoder().encode(
    `tree ${tree.oid}\n` +
      `parent ${args.parentOid}\n` +
      `author ${author}\n` +
      `committer ${author}\n\n` +
      `${args.commitMessage}\n`
  );
  const commit = await encodeGitObject("commit", commitPayload);
  const pack = await buildPack([
    { type: "blob", payload: blobPayload },
    { type: "tree", payload: treePayload },
    { type: "commit", payload: commitPayload },
  ]);

  return {
    body: concatChunks([
      pktLine(`${args.parentOid} ${commit.oid} refs/heads/main\0 ${args.capabilities}\n`),
      flushPkt(),
      pack,
    ]),
    blob,
    tree,
    commit,
  };
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
  const built = await buildStreamingReceiveBody({
    parentOid,
    nextText,
    commitMessage: "streaming update",
    capabilities: "report-status ofs-delta agent=test",
  });

  const response = await SELF.fetch(`https://example.com/${owner}/${repo}/git-receive-pack`, {
    method: "POST",
    headers: { "Content-Type": "application/x-git-receive-pack-request" },
    body: built.body,
  } as any);
  expect(response.status).toBe(200);
  expect(decodeReportStatus(new Uint8Array(await response.arrayBuffer()))).toContain(
    "ok refs/heads/main"
  );
  return { commitOid: built.commit.oid, blob: built.blob, tree: built.tree, commit: built.commit };
}
