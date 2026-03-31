import { describe, expect, it } from "vitest";
import { createExecutionContext, env, SELF } from "cloudflare:test";

import { concatChunks, flushPkt, pktLine } from "@/git/core/index.ts";
import { computeOid, encodeGitObject } from "@/git/core/objects.ts";
import { handleStreamingReceivePackPOST } from "@/git/receive/streamReceivePack.ts";
import { buildFetchBody } from "./util/fetch-protocol.ts";
import { buildAppendOnlyDelta, buildPack, zero40 } from "./util/git-pack.ts";
import { buildTreePayload } from "./util/packed-repo.ts";
import {
  callStubWithRetry,
  deleteLooseObjectCopies,
  toRequestBody,
  uniqueRepoId,
} from "./util/test-helpers.ts";
import { seedPackFirstRepo } from "./util/pack-first.ts";
import { doPrefix, r2PackDirPrefix } from "@/keys.ts";
import {
  decodeReportStatus,
  promoteToStreaming,
  pushStreamingUpdate,
} from "./util/streaming-helpers.ts";

function streamBody(bytes: Uint8Array, chunkSize = 1024): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      for (let offset = 0; offset < bytes.byteLength; offset += chunkSize) {
        controller.enqueue(bytes.subarray(offset, offset + chunkSize));
      }
      controller.close();
    },
  });
}

function abortingStreamBody(
  bytes: Uint8Array,
  abortController: AbortController,
  options?: {
    chunkSize?: number;
    abortAfterChunks?: number;
  }
): ReadableStream<Uint8Array> {
  const chunkSize = options?.chunkSize ?? 256;
  const abortAfterChunks = options?.abortAfterChunks ?? 1;
  let offset = 0;
  let emittedChunks = 0;

  return new ReadableStream<Uint8Array>({
    pull(controller) {
      if (abortController.signal.aborted) {
        const error = new Error("client aborted");
        error.name = "AbortError";
        controller.error(error);
        return;
      }
      if (offset >= bytes.byteLength) {
        controller.close();
        return;
      }

      controller.enqueue(bytes.subarray(offset, offset + chunkSize));
      offset += chunkSize;
      emittedChunks++;

      if (emittedChunks >= abortAfterChunks && !abortController.signal.aborted) {
        abortController.abort();
      }
    },
  });
}

async function listStagedReceivePacks(repoId: string): Promise<string[]> {
  const doId = env.REPO_DO.idFromName(repoId);
  const prefix = r2PackDirPrefix(doPrefix(doId.toString()));
  const listed = await env.REPO_BUCKET.list({ prefix });
  return listed.objects.map((object) => object.key).filter((key) => key.includes("/pack-rx-"));
}

async function pushBody(
  url: string,
  body: Uint8Array,
  options?: {
    stream?: boolean;
  }
): Promise<Response> {
  return await SELF.fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/x-git-receive-pack-request" },
    body: options?.stream ? streamBody(body) : body,
  } as any);
}

async function readOwnerRegistry(owner: string): Promise<string[]> {
  const response = await SELF.fetch(`https://example.com/${owner}/admin/registry`);
  expect(response.status).toBe(200);
  const payload = (await response.json()) as { repos?: string[] };
  return Array.isArray(payload.repos) ? payload.repos : [];
}

describe("streaming receive-pack", () => {
  it("returns 499 when the request is already aborted before receive work starts", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stream-receive-aborted-start");
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    await promoteToStreaming(owner, repo);

    const abortController = new AbortController();
    abortController.abort();

    const body = concatChunks([
      pktLine(
        `${seeded.nextCommit.oid} ${seeded.nextCommit.oid} refs/heads/main\0 report-status ofs-delta agent=test\n`
      ),
      flushPkt(),
    ]);
    const request = new Request(`https://example.com/${owner}/${repo}/git-receive-pack`, {
      method: "POST",
      headers: { "Content-Type": "application/x-git-receive-pack-request" },
      body: toRequestBody(body),
      signal: abortController.signal,
    });

    const response = await handleStreamingReceivePackPOST(
      env,
      repoId,
      request,
      createExecutionContext()
    );
    expect(response.status).toBe(499);

    const activity = await callStubWithRetry(seeded.getStub, (stub) => stub.getRepoActivity());
    expect(activity).toBeNull();
    expect(await listStagedReceivePacks(repoId)).toEqual([]);
  });

  it("streams a create push and fetch still works after deleting all loose copies", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stream-receive-create");
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    await promoteToStreaming(owner, repo);

    const author = "You <you@example.com> 0 +0000";
    const blobPayload = new TextEncoder().encode("version three\n");
    const blob = await encodeGitObject("blob", blobPayload);
    const treePayload = buildTreePayload([{ mode: "100644", name: "README.md", oid: blob.oid }]);
    const tree = await encodeGitObject("tree", treePayload);
    const commitPayload = new TextEncoder().encode(
      `tree ${tree.oid}\n` +
        `parent ${seeded.nextCommit.oid}\n` +
        `author ${author}\n` +
        `committer ${author}\n\n` +
        `third commit\n`
    );
    const commit = await encodeGitObject("commit", commitPayload);
    const pack = await buildPack([
      { type: "blob", payload: blobPayload },
      { type: "tree", payload: treePayload },
      { type: "commit", payload: commitPayload },
    ]);
    const body = concatChunks([
      pktLine(
        `${seeded.nextCommit.oid} ${commit.oid} refs/heads/main\0 report-status ofs-delta agent=test\n`
      ),
      flushPkt(),
      pack,
    ]);

    const response = await pushBody(`https://example.com/${owner}/${repo}/git-receive-pack`, body, {
      stream: true,
    });
    expect(response.status).toBe(200);
    expect(decodeReportStatus(new Uint8Array(await response.arrayBuffer()))).toContain(
      "ok refs/heads/main"
    );

    await deleteLooseObjectCopies(env, seeded.getStub, seeded.objectOids);

    const rawResponse = await SELF.fetch(
      `https://example.com/${owner}/${repo}/raw?oid=${blob.oid}&name=README.md`
    );
    expect(rawResponse.status).toBe(200);
    expect(await rawResponse.text()).toBe("version three\n");

    const fetchResponse = await SELF.fetch(`https://example.com/${owner}/${repo}/git-upload-pack`, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-git-upload-pack-request",
        "Git-Protocol": "version=2",
      },
      body: toRequestBody(
        buildFetchBody({
          wants: [commit.oid],
          haves: [seeded.nextCommit.oid],
          done: true,
        })
      ),
    });
    expect(fetchResponse.status).toBe(200);
    const fetchBytes = new Uint8Array(await fetchResponse.arrayBuffer());
    expect(new TextDecoder().decode(fetchBytes.subarray(4, 13))).toBe("packfile\n");
  });

  it("handles delete-only pushes in streaming mode", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stream-receive-delete");
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    await promoteToStreaming(owner, repo);

    const author = "You <you@example.com> 0 +0000";
    const blobPayload = new TextEncoder().encode("feature branch\n");
    const blob = await encodeGitObject("blob", blobPayload);
    const treePayload = buildTreePayload([{ mode: "100644", name: "README.md", oid: blob.oid }]);
    const tree = await encodeGitObject("tree", treePayload);
    const commitPayload = new TextEncoder().encode(
      `tree ${tree.oid}\n` +
        `parent ${seeded.nextCommit.oid}\n` +
        `author ${author}\n` +
        `committer ${author}\n\n` +
        `feature commit\n`
    );
    const commit = await encodeGitObject("commit", commitPayload);
    const createPack = await buildPack([
      { type: "blob", payload: blobPayload },
      { type: "tree", payload: treePayload },
      { type: "commit", payload: commitPayload },
    ]);

    const createResponse = await pushBody(
      `https://example.com/${owner}/${repo}/git-receive-pack`,
      concatChunks([
        pktLine(
          `${zero40()} ${commit.oid} refs/heads/feature\0 report-status ofs-delta agent=test\n`
        ),
        flushPkt(),
        createPack,
      ])
    );
    expect(createResponse.status).toBe(200);

    const deleteResponse = await pushBody(
      `https://example.com/${owner}/${repo}/git-receive-pack`,
      concatChunks([
        pktLine(`${commit.oid} ${zero40()} refs/heads/feature\0 report-status\n`),
        flushPkt(),
      ])
    );
    expect(deleteResponse.status).toBe(200);
    expect(decodeReportStatus(new Uint8Array(await deleteResponse.arrayBuffer()))).toContain(
      "ok refs/heads/feature"
    );

    const refsResponse = await SELF.fetch(`https://example.com/${owner}/${repo}/admin/refs`);
    const refs = (await refsResponse.json()) as Array<{ name: string; oid: string }>;
    expect(refs.find((ref) => ref.name === "refs/heads/feature")).toBeUndefined();
  });

  it("updates owner registry entries after streaming pushes add or remove the live refs set", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stream-receive-registry");
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    await promoteToStreaming(owner, repo);

    expect(await readOwnerRegistry(owner)).not.toContain(repo);

    const pushed = await pushStreamingUpdate(
      owner,
      repo,
      seeded.nextCommit.oid,
      "registry update\n"
    );
    expect(await readOwnerRegistry(owner)).toContain(repo);

    const deleteResponse = await pushBody(
      `https://example.com/${owner}/${repo}/git-receive-pack`,
      concatChunks([
        pktLine(`${pushed.commitOid} ${zero40()} refs/heads/main\0 report-status\n`),
        flushPkt(),
      ]),
      { stream: true }
    );
    expect(deleteResponse.status).toBe(200);
    expect(decodeReportStatus(new Uint8Array(await deleteResponse.arrayBuffer()))).toContain(
      "ok refs/heads/main"
    );

    expect(await readOwnerRegistry(owner)).not.toContain(repo);
  });

  it("rejects stale old-oids and leaves no staged receive packs behind", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stream-receive-stale");
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    await promoteToStreaming(owner, repo);

    const author = "You <you@example.com> 0 +0000";
    const blobPayload = new TextEncoder().encode("stale branch\n");
    const blob = await encodeGitObject("blob", blobPayload);
    const treePayload = buildTreePayload([{ mode: "100644", name: "README.md", oid: blob.oid }]);
    const tree = await encodeGitObject("tree", treePayload);
    const commitPayload = new TextEncoder().encode(
      `tree ${tree.oid}\n` +
        `parent ${seeded.nextCommit.oid}\n` +
        `author ${author}\n` +
        `committer ${author}\n\n` +
        `stale commit\n`
    );
    const commit = await encodeGitObject("commit", commitPayload);
    const pack = await buildPack([
      { type: "blob", payload: blobPayload },
      { type: "tree", payload: treePayload },
      { type: "commit", payload: commitPayload },
    ]);

    const response = await pushBody(
      `https://example.com/${owner}/${repo}/git-receive-pack`,
      concatChunks([
        pktLine(`${zero40()} ${commit.oid} refs/heads/main\0 report-status ofs-delta agent=test\n`),
        flushPkt(),
        pack,
      ]),
      { stream: true }
    );
    expect(response.status).toBe(200);
    const lines = decodeReportStatus(new Uint8Array(await response.arrayBuffer()));
    expect(lines.some((line) => line.startsWith("ng refs/heads/main stale old-oid"))).toBe(true);
    expect(await listStagedReceivePacks(repoId)).toEqual([]);
  });

  it("accepts thin packs with active external bases, rejects missing ones, and clears the receive lease after failure", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stream-receive-thin");
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    await promoteToStreaming(owner, repo);

    const author = "You <you@example.com> 0 +0000";
    const basePayload = new TextEncoder().encode("version two\n");
    const suffix = new TextEncoder().encode("delta tail\n");
    const delta = buildAppendOnlyDelta(basePayload, suffix);
    const blobPayload = new Uint8Array(basePayload.byteLength + suffix.byteLength);
    blobPayload.set(basePayload, 0);
    blobPayload.set(suffix, basePayload.byteLength);
    const blobOid = await computeOid("blob", blobPayload);
    const treePayload = buildTreePayload([{ mode: "100644", name: "README.md", oid: blobOid }]);
    const tree = await encodeGitObject("tree", treePayload);
    const commitPayload = new TextEncoder().encode(
      `tree ${tree.oid}\n` +
        `parent ${seeded.nextCommit.oid}\n` +
        `author ${author}\n` +
        `committer ${author}\n\n` +
        `thin commit\n`
    );
    const commit = await encodeGitObject("commit", commitPayload);
    const goodPack = await buildPack([
      { type: "ref-delta", baseOid: seeded.nextBlob.oid, delta },
      { type: "tree", payload: treePayload },
      { type: "commit", payload: commitPayload },
    ]);

    const goodResponse = await pushBody(
      `https://example.com/${owner}/${repo}/git-receive-pack`,
      concatChunks([
        pktLine(
          `${seeded.nextCommit.oid} ${commit.oid} refs/heads/main\0 report-status ofs-delta agent=test\n`
        ),
        flushPkt(),
        goodPack,
      ])
    );
    expect(goodResponse.status).toBe(200);
    expect(decodeReportStatus(new Uint8Array(await goodResponse.arrayBuffer()))).toContain(
      "ok refs/heads/main"
    );
    const packKeysBeforeBadPush = await listStagedReceivePacks(repoId);

    const badTreePayload = buildTreePayload([
      { mode: "100644", name: "README.md", oid: "ab".repeat(20) },
    ]);
    const badTree = await encodeGitObject("tree", badTreePayload);
    const badCommitPayload = new TextEncoder().encode(
      `tree ${badTree.oid}\n` +
        `parent ${commit.oid}\n` +
        `author ${author}\n` +
        `committer ${author}\n\n` +
        `bad thin commit\n`
    );
    const badCommit = await encodeGitObject("commit", badCommitPayload);
    const missingBasePack = await buildPack([
      {
        type: "ref-delta",
        baseOid: "cd".repeat(20),
        delta: buildAppendOnlyDelta(
          new TextEncoder().encode("base\n"),
          new TextEncoder().encode("missing\n")
        ),
      },
      { type: "tree", payload: badTreePayload },
      { type: "commit", payload: badCommitPayload },
    ]);

    const badResponse = await pushBody(
      `https://example.com/${owner}/${repo}/git-receive-pack`,
      concatChunks([
        pktLine(
          `${commit.oid} ${badCommit.oid} refs/heads/main\0 report-status ofs-delta agent=test\n`
        ),
        flushPkt(),
        missingBasePack,
      ]),
      { stream: true }
    );
    expect(badResponse.status).toBe(400);
    expect(await listStagedReceivePacks(repoId)).toEqual(packKeysBeforeBadPush);

    const activityAfterBadPush = await callStubWithRetry(seeded.getStub, (stub) =>
      stub.getRepoActivity()
    );
    expect(activityAfterBadPush).toBeNull();

    const retryPush = await pushStreamingUpdate(owner, repo, commit.oid, "cleanup retry\n");
    expect(retryPush.commitOid).not.toBe(commit.oid);
  });

  it("returns 499 and cleans up when the request aborts during the streaming upload", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stream-receive-abort-upload");
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    await promoteToStreaming(owner, repo);

    const author = "You <you@example.com> 0 +0000";
    const blobPayload = new TextEncoder().encode("aborted upload\n");
    const blob = await encodeGitObject("blob", blobPayload);
    const treePayload = buildTreePayload([{ mode: "100644", name: "README.md", oid: blob.oid }]);
    const tree = await encodeGitObject("tree", treePayload);
    const commitPayload = new TextEncoder().encode(
      `tree ${tree.oid}\n` +
        `parent ${seeded.nextCommit.oid}\n` +
        `author ${author}\n` +
        `committer ${author}\n\n` +
        `aborted upload\n`
    );
    const commit = await encodeGitObject("commit", commitPayload);
    const pack = await buildPack([
      { type: "blob", payload: blobPayload },
      { type: "tree", payload: treePayload },
      { type: "commit", payload: commitPayload },
    ]);
    const body = concatChunks([
      pktLine(
        `${seeded.nextCommit.oid} ${commit.oid} refs/heads/main\0 report-status ofs-delta agent=test\n`
      ),
      flushPkt(),
      pack,
    ]);

    const abortController = new AbortController();
    const request = new Request(`https://example.com/${owner}/${repo}/git-receive-pack`, {
      method: "POST",
      headers: { "Content-Type": "application/x-git-receive-pack-request" },
      body: abortingStreamBody(body, abortController),
      signal: abortController.signal,
    });

    const response = await handleStreamingReceivePackPOST(
      env,
      repoId,
      request,
      createExecutionContext()
    );
    expect(response.status).toBe(499);
    expect(await listStagedReceivePacks(repoId)).toEqual([]);

    const activity = await callStubWithRetry(seeded.getStub, (stub) => stub.getRepoActivity());
    expect(activity).toBeNull();

    const retryPush = await pushStreamingUpdate(
      owner,
      repo,
      seeded.nextCommit.oid,
      "after abort\n"
    );
    expect(retryPush.commitOid).not.toBe(seeded.nextCommit.oid);
  });

  it("returns 503 when a streaming receive lease is already active", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stream-receive-busy");
    const repoId = `${owner}/${repo}`;
    const seeded = await seedPackFirstRepo(repoId);
    await promoteToStreaming(owner, repo);

    const begin = await callStubWithRetry<any>(seeded.getStub, (stub) => stub.beginReceive());
    if (!begin.ok) {
      throw new Error("expected test receive lease to be granted");
    }

    const response = await pushBody(
      `https://example.com/${owner}/${repo}/git-receive-pack`,
      concatChunks([
        pktLine(`${zero40()} ${seeded.nextCommit.oid} refs/heads/main\0 report-status\n`),
        flushPkt(),
      ])
    );
    expect(response.status).toBe(503);
    expect(response.headers.get("Retry-After")).toBe("10");

    await callStubWithRetry(seeded.getStub, (stub) => stub.abortReceive(begin.lease.token));
  });

  it("rejects invalid refs without leaving staged receive packs behind", async () => {
    const owner = "o";
    const repo = uniqueRepoId("stream-receive-invalid-ref");
    const repoId = `${owner}/${repo}`;
    await seedPackFirstRepo(repoId);
    await promoteToStreaming(owner, repo);

    const response = await pushBody(
      `https://example.com/${owner}/${repo}/git-receive-pack`,
      concatChunks([
        pktLine(`${zero40()} ${"a".repeat(40)} HEAD\0 report-status ofs-delta agent=test\n`),
        flushPkt(),
      ])
    );
    expect(response.status).toBe(200);
    const lines = decodeReportStatus(new Uint8Array(await response.arrayBuffer()));
    expect(lines.some((line) => line.startsWith("unpack error invalid-ref"))).toBe(true);
    expect(await listStagedReceivePacks(repoId)).toEqual([]);
  });
});
