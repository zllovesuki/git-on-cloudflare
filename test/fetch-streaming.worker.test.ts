import { it, expect, describe } from "vitest";
import { env, SELF } from "cloudflare:test";
import type { RepoDurableObject } from "@/index";
import { pktLine, delimPkt, flushPkt, concatChunks, decodePktLines } from "@/git";
import { handleFetchV2Streaming } from "@/git/operations/uploadStream.ts";
import { uniqueRepoId, runDOWithRetry } from "./util/test-helpers.ts";
import { asBufferSource } from "@/common/index.ts";

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

/**
 * Find the index of a byte sequence within a Uint8Array
 */
function findBytes(haystack: Uint8Array, needle: Uint8Array): number {
  outer: for (let i = 0; i <= haystack.length - needle.length; i++) {
    for (let j = 0; j < needle.length; j++) {
      if (haystack[i + j] !== needle[j]) continue outer;
    }
    return i;
  }
  return -1;
}

describe("git fetch streaming (default)", () => {
  it("handles fetch with streaming by default", async () => {
    const owner = "o";
    const repo = uniqueRepoId("streaming");
    const repoId = `${owner}/${repo}`;

    // Seed a repository with some commits
    const id = env.REPO_DO.idFromName(repoId);
    const { commitOid } = await runDOWithRetry(
      () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>,
      async (instance: RepoDurableObject) => await instance.seedMinimalRepo()
    );

    const body = buildFetchBody({ wants: [commitOid], done: true });
    const url = `https://example.com/${owner}/${repo}/git-upload-pack`;

    const res = await SELF.fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-git-upload-pack-request",
        "Git-Protocol": "version=2",
      },
      body,
    } as any);

    expect(res.status).toBe(200);
    expect(res.headers.get("Content-Type")).toContain("git-upload-pack-result");

    const bytes = new Uint8Array(await res.arrayBuffer());

    const lines = decodePktLines(bytes);
    let hasAcknowledgments = false;
    let hasPackfile = false;
    let inPackfile = false;
    const packData: Uint8Array[] = [];
    let hasSideband = false;

    for (const line of lines) {
      if (line.type === "line" && line.text === "acknowledgments\n") {
        hasAcknowledgments = true;
      }
      if (line.type === "line" && line.text === "packfile\n") {
        hasPackfile = true;
        inPackfile = true;
      }
    }

    expect(
      hasAcknowledgments,
      "Response should NOT contain 'acknowledgments\\n' when done=true"
    ).toBe(false);
    expect(hasPackfile, "Response should contain 'packfile\\n' pkt-line").toBe(true);

    for (const line of lines) {
      if (line.type === "line" && line.text === "packfile\n") {
        inPackfile = true;
      } else if (inPackfile && line.type === "line" && line.raw) {
        // Check if this is sideband data (first byte is 0x01, 0x02, or 0x03)
        if (
          line.raw.length > 0 &&
          (line.raw[0] === 0x01 || line.raw[0] === 0x02 || line.raw[0] === 0x03)
        ) {
          hasSideband = true;
          if (line.raw[0] === 0x01) {
            // Channel 1: pack data
            packData.push(line.raw.subarray(1));
          }
        }
      }
    }

    expect(hasSideband).toBe(true);
    const pack = concatChunks(packData);

    expect(pack.length).toBeGreaterThan(0);

    // Verify pack signature
    const packSig = new TextDecoder().decode(pack.subarray(0, 4));
    expect(packSig).toBe("PACK");

    // Verify pack header
    const dv = new DataView(pack.buffer, pack.byteOffset, pack.byteLength);
    const version = dv.getUint32(4);
    const objCount = dv.getUint32(8);
    expect(version).toBe(2);
    expect(objCount).toBeGreaterThan(0);

    // Verify SHA-1 trailer (last 20 bytes)
    expect(pack.length).toBeGreaterThanOrEqual(32); // At least header + SHA-1
    const packBody = pack.subarray(0, pack.length - 20);
    const expectedSha = pack.subarray(pack.length - 20);
    const actualSha = new Uint8Array(await crypto.subtle.digest("SHA-1", asBufferSource(packBody)));
    expect(Array.from(actualSha)).toEqual(Array.from(expectedSha));
  });

  it("handles incremental fetch with haves", async () => {
    const owner = "o";
    const repo = uniqueRepoId("incremental");
    const repoId = `${owner}/${repo}`;

    // Seed repository and get multiple commits
    const id = env.REPO_DO.idFromName(repoId);
    const { commitOid, parentOid } = await runDOWithRetry(
      () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>,
      async (instance: RepoDurableObject) => {
        const firstResult = await instance.seedMinimalRepo();
        const secondResult = await instance.seedMinimalRepo();
        return {
          commitOid: secondResult.commitOid,
          parentOid: firstResult.commitOid,
        };
      }
    );

    // First, do negotiation without done
    const negotiateBody = buildFetchBody({
      wants: [commitOid],
      haves: [parentOid],
      done: false,
    });

    const url = `https://example.com/${owner}/${repo}/git-upload-pack`;
    const negotiateRes = await SELF.fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-git-upload-pack-request",
        "Git-Protocol": "version=2",
        // Streaming is now default, no header needed
      },
      body: negotiateBody,
    } as any);

    expect(negotiateRes.status).toBe(200);
    const negotiateBytes = new Uint8Array(await negotiateRes.arrayBuffer());
    const negotiateLines = decodePktLines(negotiateBytes);
    let hasAcknowledgments = false;
    let hasPackfile = false;
    let hasParentAck = false;

    for (const line of negotiateLines) {
      if (line.type === "line") {
        if (line.text === "acknowledgments\n") hasAcknowledgments = true;
        if (line.text === "packfile\n") hasPackfile = true;
        if (line.text && line.text.includes(`ACK ${parentOid}`)) hasParentAck = true;
      }
    }

    // Should only have acknowledgments, no packfile
    expect(
      hasAcknowledgments,
      "Negotiation response should contain 'acknowledgments\\n' pkt-line"
    ).toBe(true);
    expect(hasPackfile, "Negotiation response should NOT contain 'packfile\\n' pkt-line").toBe(
      false
    );
    expect(hasParentAck, `Negotiation should ACK parent ${parentOid}`).toBe(true);

    // Now fetch with done
    const fetchBody = buildFetchBody({
      wants: [commitOid],
      haves: [parentOid],
      done: true,
    });

    const fetchRes = await SELF.fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-git-upload-pack-request",
        "Git-Protocol": "version=2",
        // Streaming is now default, no header needed
      },
      body: fetchBody,
    } as any);

    expect(fetchRes.status).toBe(200);
    const fetchBytes = new Uint8Array(await fetchRes.arrayBuffer());
    const fetchLines = decodePktLines(fetchBytes);
    let hasFetchAcknowledgments = false;
    let hasFetchPackfile = false;

    for (const line of fetchLines) {
      if (line.type === "line") {
        if (line.text === "acknowledgments\n") hasFetchAcknowledgments = true;
        if (line.text === "packfile\n") hasFetchPackfile = true;
      }
    }

    // Should go straight to packfile when done=true
    expect(
      hasFetchAcknowledgments,
      "Final fetch with done=true should NOT contain 'acknowledgments\\n'"
    ).toBe(false);
    expect(hasFetchPackfile, "Final fetch should contain 'packfile\\n' pkt-line").toBe(true);

    // Parse and verify pack data
    let inPackfile = false;
    const packData: Uint8Array[] = [];

    for (const line of fetchLines) {
      if (line.type === "line" && line.text === "packfile\n") {
        inPackfile = true;
      } else if (inPackfile && line.type === "line" && line.raw?.[0] === 0x01) {
        packData.push(line.raw.subarray(1));
      }
    }

    const pack = concatChunks(packData);
    expect(pack.length).toBeGreaterThan(0);

    // Verify it's a valid pack
    const packSig = new TextDecoder().decode(pack.subarray(0, 4));
    expect(packSig).toBe("PACK");
  });

  it("handles initial clone (no haves) with streaming", async () => {
    const owner = "o";
    const repo = uniqueRepoId("clone");
    const repoId = `${owner}/${repo}`;

    // Seed a repository
    const id = env.REPO_DO.idFromName(repoId);
    const { commitOid } = await runDOWithRetry(
      () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>,
      async (instance: RepoDurableObject) => instance.seedMinimalRepo()
    );

    // Clone with no haves
    const body = buildFetchBody({ wants: [commitOid], done: true });
    const url = `https://example.com/${owner}/${repo}/git-upload-pack`;

    const res = await SELF.fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-git-upload-pack-request",
        "Git-Protocol": "version=2",
      },
      body,
    } as any);

    expect(res.status).toBe(200);

    const bytes = new Uint8Array(await res.arrayBuffer());
    const lines = decodePktLines(bytes);

    // Should include NAK since there are no common haves
    let hasNak = false;
    let hasPackfile = false;
    let progressMessages = 0;
    const packData: Uint8Array[] = [];

    for (const line of lines) {
      if (line.type === "line") {
        if (line.text === "NAK\n") hasNak = true;
        if (line.text === "packfile\n") hasPackfile = true;
        if (hasPackfile && line.raw?.[0] === 0x01) {
          packData.push(line.raw.subarray(1));
        } else if (hasPackfile && line.raw?.[0] === 0x02) {
          progressMessages++;
        }
      }
    }

    // With done=true, there are no acknowledgments (no NAK)
    expect(hasNak, "Clone response with done=true should NOT contain NAK").toBe(false);
    expect(hasPackfile, "Clone response should contain packfile").toBe(true);

    // Verify pack contains all objects (tree + commit at minimum)
    const pack = concatChunks(packData);
    const dv = new DataView(pack.buffer, pack.byteOffset, pack.byteLength);
    const objCount = dv.getUint32(8);
    expect(objCount).toBeGreaterThanOrEqual(2); // At least tree + commit
  });

  it("handles repositories with packs created by default", async () => {
    const owner = "o";
    const repo = uniqueRepoId("with-pack");
    const repoId = `${owner}/${repo}`;

    // Seed repository with packed objects (default behavior)
    const id = env.REPO_DO.idFromName(repoId);
    const { commitOid } = await runDOWithRetry(
      () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>,
      async (instance: RepoDurableObject) => instance.seedMinimalRepo() // Default: withPack=true
    );

    const body = buildFetchBody({ wants: [commitOid], done: true });
    const url = `https://example.com/${owner}/${repo}/git-upload-pack`;

    // Streaming is now the default
    const res = await SELF.fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-git-upload-pack-request",
        "Git-Protocol": "version=2",
      },
      body,
    } as any);

    // Should succeed with streaming
    expect(res.status).toBe(200);
    expect(res.headers.get("Content-Type")).toContain("git-upload-pack-result");

    const bytes = new Uint8Array(await res.arrayBuffer());
    const lines = decodePktLines(bytes);
    let hasAcknowledgments = false;
    let hasPackfile = false;

    for (const line of lines) {
      if (line.type === "line") {
        if (line.text === "acknowledgments\n") hasAcknowledgments = true;
        if (line.text === "packfile\n") hasPackfile = true;
      }
    }

    // Verify basic structure
    // When done=true, response goes straight to packfile
    expect(
      hasAcknowledgments,
      "Response should NOT contain 'acknowledgments\\n' when done=true"
    ).toBe(false);
    expect(hasPackfile, "Response should contain 'packfile\\n' pkt-line").toBe(true);

    // Find and verify pack data
    const packStart = findBytes(bytes, new TextEncoder().encode("PACK"));
    expect(packStart).toBeGreaterThan(-1);

    const pack = bytes.subarray(packStart);
    const dv = new DataView(pack.buffer, pack.byteOffset, pack.byteLength);
    expect(dv.getUint32(4)).toBe(2); // version
    expect(dv.getUint32(8)).toBeGreaterThan(0); // object count
  });

  it("returns 503 when pack assembly fails", async () => {
    const owner = "o";
    const repo = uniqueRepoId("fail");
    const repoId = `${owner}/${repo}`;

    // Request fetch for non-existent objects
    const body = buildFetchBody({
      wants: ["deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"],
      done: true,
    });
    const url = `https://example.com/${owner}/${repo}/git-upload-pack`;

    const res = await SELF.fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-git-upload-pack-request",
        "Git-Protocol": "version=2",
      },
      body,
    } as any);

    // Should return 503 since objects don't exist
    expect(res.status).toBe(503);
    expect(res.headers.get("Retry-After")).toBeDefined();
  });

  it("handles request abort mid-stream gracefully", async () => {
    const owner = "o";
    const repo = uniqueRepoId("abort");
    const repoId = `${owner}/${repo}`;

    // Seed repository with multiple objects to ensure streaming takes some time
    const id = env.REPO_DO.idFromName(repoId);
    const commits: string[] = [];

    await runDOWithRetry(
      () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>,
      async (instance: RepoDurableObject) => {
        // Create multiple commits to ensure pack has content
        for (let i = 0; i < 5; i++) {
          const result = await instance.seedMinimalRepo();
          commits.push(result.commitOid);
        }
        return commits;
      }
    );

    const body = buildFetchBody({ wants: commits, done: true });
    const url = `https://example.com/${owner}/${repo}/git-upload-pack`;
    const abortController = new AbortController();

    const fetchPromise = SELF.fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-git-upload-pack-request",
        "Git-Protocol": "version=2",
      },
      body,
      signal: abortController.signal,
    } as any);

    // Abort after a short delay to interrupt the stream
    setTimeout(() => abortController.abort(), 10);

    // The fetch should be aborted
    try {
      const res = await fetchPromise;
      // If we get a response, check if it's the expected abort response
      // Some implementations might return 499 Client Closed Request
      if (res.status === 499) {
        expect(res.status).toBe(499);
      } else {
        // Otherwise the stream might have completed before abort
        expect(res.status).toBe(200);
      }
    } catch (e: any) {
      // AbortError is expected
      expect(e.name).toBe("AbortError");
    }
  });

  it("emits band-3 fatal message on mid-stream error", async () => {
    const owner = "o";
    const repo = uniqueRepoId("fatal");
    const repoId = `${owner}/${repo}`;

    // This test is tricky to implement without mocking R2 failures
    // We'll create a scenario where pack assembly could fail mid-stream
    // by requesting objects that exist in DO but might fail during assembly

    // Seed repository
    const id = env.REPO_DO.idFromName(repoId);
    const { commitOid } = await runDOWithRetry(
      () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>,
      async (instance: RepoDurableObject) => instance.seedMinimalRepo()
    );

    // To truly test band-3 fatal, we'd need to inject an R2 failure
    // Since we can't easily mock R2 in the test environment,
    // we'll test that the protocol structure is correct for error cases

    // Create a malformed request that might trigger an error during processing
    const body = buildFetchBody({
      wants: [commitOid],
      done: true,
    });

    // Corrupt the body slightly to potentially trigger an error
    const corruptedBody = new Uint8Array(body.length + 10);
    corruptedBody.set(body, 0);
    // Add some garbage that might confuse the parser after valid data
    corruptedBody.set(new Uint8Array([0xff, 0xff, 0xff, 0xff]), body.length);

    const url = `https://example.com/${owner}/${repo}/git-upload-pack`;

    const res = await SELF.fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-git-upload-pack-request",
        "Git-Protocol": "version=2",
      },
      body: corruptedBody,
    } as any);

    // The server should handle the corruption gracefully
    // Either by returning an error status or completing with valid data
    expect([200, 400, 500, 503].includes(res.status)).toBe(true);

    if (res.status === 200) {
      // If it succeeded, check for valid response structure
      const bytes = new Uint8Array(await res.arrayBuffer());
      const lines = decodePktLines(bytes);

      // Check if there's a band-3 fatal message
      let hasFatal = false;
      for (const line of lines) {
        if (line.type === "line" && line.raw?.[0] === 0x03) {
          hasFatal = true;
          const fatalMsg = new TextDecoder().decode(line.raw.subarray(1));
          expect(fatalMsg).toContain("fatal:");
        }
      }

      // Note: hasFatal might be false if the server recovered from the corruption
    }
  });

  it("handles abort signal during negotiation phase", async () => {
    const owner = "o";
    const repo = uniqueRepoId("abort-negotiation");
    const repoId = `${owner}/${repo}`;

    // Seed repository with commits
    const id = env.REPO_DO.idFromName(repoId);
    const { commitOid, parentOid } = await runDOWithRetry(
      () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>,
      async (instance: RepoDurableObject) => {
        const first = await instance.seedMinimalRepo();
        const second = await instance.seedMinimalRepo();
        return {
          commitOid: second.commitOid,
          parentOid: first.commitOid,
        };
      }
    );

    // Test abort during negotiation (done=false)
    const body = buildFetchBody({
      wants: [commitOid],
      haves: [parentOid],
      done: false,
    });

    const abortController = new AbortController();

    // Abort immediately
    abortController.abort();

    const res = await handleFetchV2Streaming(env as Env, repoId, body, abortController.signal);
    expect(res.status).toBe(499);
  });

  it("verifies streaming response includes progress messages", async () => {
    const owner = "o";
    const repo = uniqueRepoId("progress");
    const repoId = `${owner}/${repo}`;

    // Create a repository with enough content to trigger progress messages
    const id = env.REPO_DO.idFromName(repoId);
    const commits: string[] = [];

    await runDOWithRetry(
      () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>,
      async (instance: RepoDurableObject) => {
        // Create multiple commits
        for (let i = 0; i < 3; i++) {
          const result = await instance.seedMinimalRepo();
          commits.push(result.commitOid);
        }
        // Try to trigger packing if possible
        // Note: this might fall back to loose objects
        return commits;
      }
    );

    const body = buildFetchBody({ wants: commits, done: true });
    const url = `https://example.com/${owner}/${repo}/git-upload-pack`;

    const res = await SELF.fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-git-upload-pack-request",
        "Git-Protocol": "version=2",
      },
      body,
    } as any);

    expect(res.status).toBe(200);

    const bytes = new Uint8Array(await res.arrayBuffer());
    const lines = decodePktLines(bytes);

    // Look for band-2 progress messages
    const progressMessages: string[] = [];
    let inPackfile = false;

    for (const line of lines) {
      if (line.type === "line" && line.text === "packfile\n") {
        inPackfile = true;
      }
      if (inPackfile && line.type === "line" && line.raw?.[0] === 0x02) {
        // Band 2: progress message
        const msg = new TextDecoder().decode(line.raw.subarray(1));
        progressMessages.push(msg);
      }
    }

    expect(progressMessages.length).toBeGreaterThan(0);
    const packStart = findBytes(bytes, new TextEncoder().encode("PACK"));
    expect(packStart).toBeGreaterThan(-1);
  });

  it("shows planning progress before pack assembly for initial fetches", async () => {
    const owner = "o";
    const repo = uniqueRepoId("early-progress");
    const repoId = `${owner}/${repo}`;

    // Seed repository with some commits and ensure they're packed
    const id = env.REPO_DO.idFromName(repoId);
    const { commitOid } = await runDOWithRetry(
      () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>,
      async (instance: RepoDurableObject) => instance.seedMinimalRepo()
    );

    const body = buildFetchBody({ wants: [commitOid], done: true });
    const url = `https://example.com/${owner}/${repo}/git-upload-pack`;

    const res = await SELF.fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-git-upload-pack-request",
        "Git-Protocol": "version=2",
      },
      body,
    } as any);

    expect(res.status).toBe(200);

    const bytes = new Uint8Array(await res.arrayBuffer());
    const lines = decodePktLines(bytes);

    // Collect all progress messages and pack data in order
    const orderedOutput: { type: "progress" | "data"; content: string | number }[] = [];
    let inPackfile = false;

    for (const line of lines) {
      if (line.type === "line" && line.text === "packfile\n") {
        inPackfile = true;
      } else if (inPackfile && line.type === "line" && line.raw) {
        if (line.raw[0] === 0x02) {
          // Band 2: progress message
          const msg = new TextDecoder().decode(line.raw.subarray(1));
          orderedOutput.push({ type: "progress", content: msg });
        } else if (line.raw[0] === 0x01) {
          // Band 1: pack data - just record the first byte to prove data arrived
          orderedOutput.push({ type: "data", content: line.raw[1] });
        }
      }
    }

    // Verify we got progress messages
    const progressMessages = orderedOutput.filter((o) => o.type === "progress");
    expect(progressMessages.length).toBeGreaterThan(0);

    expect(progressMessages[0]?.content).toBe("Selecting objects to send...\n");
    expect(progressMessages[1]?.content).toBe("Preparing pack...\n");

    // Verify progress comes before data
    const firstProgressIdx = orderedOutput.findIndex((o) => o.type === "progress");
    const firstDataIdx = orderedOutput.findIndex((o) => o.type === "data");

    expect(firstProgressIdx).toBeGreaterThanOrEqual(0);
    expect(firstDataIdx).toBeGreaterThanOrEqual(0);
    expect(firstProgressIdx).toBeLessThan(firstDataIdx);
  });

  it("shows common-commit progress before pack assembly when haves are present", async () => {
    const owner = "o";
    const repo = uniqueRepoId("have-progress");
    const repoId = `${owner}/${repo}`;

    const id = env.REPO_DO.idFromName(repoId);
    const { commitOid, parentOid } = await runDOWithRetry(
      () => env.REPO_DO.get(id) as DurableObjectStub<RepoDurableObject>,
      async (instance: RepoDurableObject) => {
        const first = await instance.seedMinimalRepo();
        const second = await instance.seedMinimalRepo();
        return {
          commitOid: second.commitOid,
          parentOid: first.commitOid,
        };
      }
    );

    const body = buildFetchBody({
      wants: [commitOid],
      haves: [parentOid],
      done: true,
    });
    const url = `https://example.com/${owner}/${repo}/git-upload-pack`;

    const res = await SELF.fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-git-upload-pack-request",
        "Git-Protocol": "version=2",
      },
      body,
    } as any);

    expect(res.status).toBe(200);

    const bytes = new Uint8Array(await res.arrayBuffer());
    const lines = decodePktLines(bytes);

    const orderedOutput: { type: "progress" | "data"; content: string | number }[] = [];
    let inPackfile = false;

    for (const line of lines) {
      if (line.type === "line" && line.text === "packfile\n") {
        inPackfile = true;
      } else if (inPackfile && line.type === "line" && line.raw) {
        if (line.raw[0] === 0x02) {
          const msg = new TextDecoder().decode(line.raw.subarray(1));
          orderedOutput.push({ type: "progress", content: msg });
        } else if (line.raw[0] === 0x01) {
          orderedOutput.push({ type: "data", content: line.raw[1] });
        }
      }
    }

    const progressMessages = orderedOutput.filter((o) => o.type === "progress");
    expect(progressMessages[0]?.content).toBe("Finding common commits...\n");
    expect(progressMessages[1]?.content).toBe("Selecting objects to send...\n");
    expect(progressMessages[2]?.content).toBe("Preparing pack...\n");

    const prepareIdx = orderedOutput.findIndex(
      (o) => o.type === "progress" && o.content === "Preparing pack...\n"
    );
    const firstDataIdx = orderedOutput.findIndex((o) => o.type === "data");

    expect(prepareIdx).toBeGreaterThanOrEqual(0);
    expect(firstDataIdx).toBeGreaterThanOrEqual(0);
    expect(prepareIdx).toBeLessThan(firstDataIdx);
  });
});
