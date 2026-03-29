import { it, expect } from "vitest";
import { SELF } from "cloudflare:test";
import { decodePktLines, pktLine, flushPkt, concatChunks } from "@/git";
import { asBufferSource, deflate } from "@/common/index.ts";

function encodeObjHeader(type: number, size: number): Uint8Array {
  let first = (type << 4) | (size & 0x0f);
  size >>= 4;
  const bytes: number[] = [];
  if (size > 0) first |= 0x80;
  bytes.push(first);
  while (size > 0) {
    let b = size & 0x7f;
    size >>= 7;
    if (size > 0) b |= 0x80;
    bytes.push(b);
  }
  return new Uint8Array(bytes);
}

async function encodeGitObjectAndDeflate(
  type: "blob" | "tree" | "commit" | "tag",
  payload: Uint8Array
) {
  const header = new TextEncoder().encode(`${type} ${payload.byteLength}\0`);
  const raw = new Uint8Array(header.byteLength + payload.byteLength);
  raw.set(header, 0);
  raw.set(payload, header.byteLength);
  const hash = await crypto.subtle.digest("SHA-1", raw);
  const oid = Array.from(new Uint8Array(hash))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
  const zdata = await deflate(payload);
  return { oid, payload, zdata };
}

async function buildPack(
  objects: { type: "commit" | "tree" | "blob" | "tag"; payload: Uint8Array }[]
): Promise<Uint8Array> {
  const hdr = new Uint8Array(12);
  hdr.set(new TextEncoder().encode("PACK"), 0);
  const dv = new DataView(hdr.buffer);
  dv.setUint32(4, 2);
  dv.setUint32(8, objects.length);
  const parts: Uint8Array[] = [hdr];
  for (const o of objects) {
    const typeCode = o.type === "commit" ? 1 : o.type === "tree" ? 2 : o.type === "blob" ? 3 : 4;
    parts.push(encodeObjHeader(typeCode, o.payload.byteLength));
    parts.push(await deflate(o.payload));
  }
  const body = concatChunks(parts);
  const sha = new Uint8Array(await crypto.subtle.digest("SHA-1", asBufferSource(body)));
  const out = new Uint8Array(body.byteLength + 20);
  out.set(body, 0);
  out.set(sha, body.byteLength);
  return out;
}

function zero40() {
  return "0".repeat(40);
}

it("receive-pack: create new ref with one commit pack and report-status ok", async () => {
  const owner = "o";
  const repo = "r-push-create";
  const url = `https://example.com/${owner}/${repo}/git-receive-pack`;

  // Build an empty tree and a commit pointing to it
  const treePayload = new Uint8Array(0);
  const { oid: treeOid } = await encodeGitObjectAndDeflate("tree", treePayload);
  const author = `You <you@example.com> 0 +0000`;
  const committer = author;
  const msg = "push create\n";
  const commitPayload = new TextEncoder().encode(
    `tree ${treeOid}\n` + `author ${author}\n` + `committer ${committer}\n\n${msg}`
  );
  const { oid: commitOid } = await encodeGitObjectAndDeflate("commit", commitPayload);

  const pack = await buildPack([
    { type: "tree", payload: treePayload },
    { type: "commit", payload: commitPayload },
  ]);

  // Commands section (pkt-lines) followed by flush and then raw pack bytes
  const cmd = `${zero40()} ${commitOid} refs/heads/feat\0 report-status ofs-delta agent=test\n`;
  const body = concatChunks([pktLine(cmd), flushPkt(), pack]);

  const res = await SELF.fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/x-git-receive-pack-request" },
    body,
  } as any);
  expect(res.status).toBe(200);
  const bytes = new Uint8Array(await res.arrayBuffer());
  const items = decodePktLines(bytes);
  const lines = items.filter((i) => i.type === "line").map((i: any) => (i.text as string).trim());
  expect(lines.some((l) => l.startsWith("unpack ok"))).toBe(true);
  expect(lines).toContain("ok refs/heads/feat");

  // Verify ref was created to point to our commit
  const refsRes = await SELF.fetch(`https://example.com/${owner}/${repo}/admin/refs`);
  expect(refsRes.status).toBe(200);
  const refs = await refsRes.json<any>();
  const feat = refs.find((r: any) => r.name === "refs/heads/feat");
  expect(feat?.oid).toBe(commitOid);
});

it("receive-pack: stale old-oid rejects update and ref remains unchanged", async () => {
  const owner = "o";
  const repo = "r-push-stale";
  const url = `https://example.com/${owner}/${repo}/git-receive-pack`;

  // Create a branch 'main' with one commit
  const treePayload = new Uint8Array(0);
  const author = `You <you@example.com> 0 +0000`;
  const committer = author;
  const msg = "first\n";
  const treeHeader = new TextEncoder().encode(`tree ${treePayload.byteLength}\0`);
  const treeRaw = new Uint8Array(treeHeader.length + treePayload.length);
  treeRaw.set(treeHeader, 0);
  treeRaw.set(treePayload, treeHeader.length);
  const treeOid = Array.from(new Uint8Array(await crypto.subtle.digest("SHA-1", treeRaw)))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
  const commitPayload = new TextEncoder().encode(
    `tree ${treeOid}\n` + `author ${author}\n` + `committer ${committer}\n\n${msg}`
  );
  const pack = await buildPack([
    { type: "tree", payload: treePayload },
    { type: "commit", payload: commitPayload },
  ]);
  const commitOid = await (async () => {
    const head = new TextEncoder().encode(`commit ${commitPayload.byteLength}\0`);
    const raw = new Uint8Array(head.length + commitPayload.length);
    raw.set(head, 0);
    raw.set(commitPayload, head.length);
    const hash = await crypto.subtle.digest("SHA-1", raw);
    return Array.from(new Uint8Array(hash))
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
  })();
  const body1 = concatChunks([
    pktLine(`${zero40()} ${commitOid} refs/heads/main\0 report-status\n`),
    flushPkt(),
    pack,
  ]);
  const res1 = await SELF.fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/x-git-receive-pack-request" },
    body: body1,
  } as any);
  expect(res1.status).toBe(200);

  // Now attempt to update 'main' but provide a stale old-oid (zeros)
  const body2 = concatChunks([
    pktLine(`${zero40()} ${commitOid} refs/heads/main\0 report-status\n`),
    flushPkt(),
    await buildPack([]),
  ]);
  const res2 = await SELF.fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/x-git-receive-pack-request" },
    body: body2,
  } as any);
  expect(res2.status).toBe(200);
  const lines = decodePktLines(new Uint8Array(await res2.arrayBuffer()))
    .filter((i) => i.type === "line")
    .map((i: any) => i.text.trim());
  expect(lines.some((l) => l.startsWith("ng refs/heads/main"))).toBe(true);

  // Ref remains pointing to original commit
  const refsRes = await SELF.fetch(`https://example.com/${owner}/${repo}/admin/refs`);
  const refs = await refsRes.json<any>();
  const main = refs.find((r: any) => r.name === "refs/heads/main");
  expect(main?.oid).toBe(commitOid);
});

it("receive-pack: delete ref succeeds with zero new oid and correct old", async () => {
  const owner = "o";
  const repo = "r-push-delete";
  const url = `https://example.com/${owner}/${repo}/git-receive-pack`;

  // Create branch 'tmp' first
  const treePayload = new Uint8Array(0);
  const author = `You <you@example.com> 0 +0000`;
  const committer = author;
  const msg = "to-delete\n";
  const treeHeader = new TextEncoder().encode(`tree ${treePayload.byteLength}\0`);
  const treeRaw = new Uint8Array(treeHeader.length + treePayload.length);
  treeRaw.set(treeHeader, 0);
  treeRaw.set(treePayload, treeHeader.length);
  const treeOid = Array.from(new Uint8Array(await crypto.subtle.digest("SHA-1", treeRaw)))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
  const commitPayload = new TextEncoder().encode(
    `tree ${treeOid}\n` + `author ${author}\n` + `committer ${committer}\n\n${msg}`
  );
  const pack = await buildPack([
    { type: "tree", payload: treePayload },
    { type: "commit", payload: commitPayload },
  ]);
  const commitOid = await (async () => {
    const head = new TextEncoder().encode(`commit ${commitPayload.byteLength}\0`);
    const raw = new Uint8Array(head.length + commitPayload.length);
    raw.set(head, 0);
    raw.set(commitPayload, head.length);
    const hash = await crypto.subtle.digest("SHA-1", raw);
    return Array.from(new Uint8Array(hash))
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
  })();
  const createBody = concatChunks([
    pktLine(`${zero40()} ${commitOid} refs/heads/tmp\0 report-status\n`),
    flushPkt(),
    pack,
  ]);
  const createRes = await SELF.fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/x-git-receive-pack-request" },
    body: createBody,
  } as any);
  expect(createRes.status).toBe(200);

  // Delete: new = zeros, old = current commit oid
  const delBody = concatChunks([
    pktLine(`${commitOid} ${zero40()} refs/heads/tmp\0 report-status\n`),
    flushPkt(),
    await buildPack([]),
  ]);
  const delRes = await SELF.fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/x-git-receive-pack-request" },
    body: delBody,
  } as any);
  expect(delRes.status).toBe(200);
  const lines = decodePktLines(new Uint8Array(await delRes.arrayBuffer()))
    .filter((i) => i.type === "line")
    .map((i: any) => i.text.trim());
  expect(lines).toContain("ok refs/heads/tmp");

  // Ref removed
  const refsRes = await SELF.fetch(`https://example.com/${owner}/${repo}/admin/refs`);
  const refs = await refsRes.json<any>();
  expect(refs.find((r: any) => r.name === "refs/heads/tmp")).toBeUndefined();
});

it("receive-pack: atomic multi-ref updates - one invalid causes none to apply", async () => {
  const owner = "o";
  const repo = "r-push-atomic";
  const url = `https://example.com/${owner}/${repo}/git-receive-pack`;

  // Build a simple commit pack (empty tree + commit)
  const treePayload = new Uint8Array(0);
  const author = `You <you@example.com> 0 +0000`;
  const committer = author;
  const msg = "atomic\n";
  const commitPayload = new TextEncoder().encode(
    `tree ${await (async () => {
      const { oid } = await (async () => {
        const header = new TextEncoder().encode(`tree 0\0`);
        return { oid: "0".repeat(40) };
      })();
      return oid;
    })()}\n` +
      `author ${author}\n` +
      `committer ${committer}\n\n${msg}`
  );
  // Actually compute real oids
  const { oid: treeOid } = await (async () => {
    const header = new TextEncoder().encode(`tree ${treePayload.byteLength}\0`);
    const raw = new Uint8Array(header.byteLength + treePayload.byteLength);
    raw.set(header, 0);
    raw.set(treePayload, header.byteLength);
    const hash = await crypto.subtle.digest("SHA-1", raw);
    return {
      oid: Array.from(new Uint8Array(hash))
        .map((b) => b.toString(16).padStart(2, "0"))
        .join(""),
    };
  })();
  const commitPayload2 = new TextEncoder().encode(
    `tree ${treeOid}\n` + `author ${author}\n` + `committer ${committer}\n\n${msg}`
  );
  const pack = await (async () => {
    const hdr = new Uint8Array(12);
    hdr.set(new TextEncoder().encode("PACK"), 0);
    const dv = new DataView(hdr.buffer);
    dv.setUint32(4, 2);
    dv.setUint32(8, 2);
    const parts: Uint8Array[] = [hdr];
    parts.push(encodeObjHeader(2, treePayload.byteLength));
    parts.push(await deflate(treePayload));
    parts.push(encodeObjHeader(1, commitPayload2.byteLength));
    parts.push(await deflate(commitPayload2));
    const body = concatChunks(parts);
    const sha = new Uint8Array(await crypto.subtle.digest("SHA-1", asBufferSource(body)));
    const out = new Uint8Array(body.byteLength + 20);
    out.set(body, 0);
    out.set(sha, body.byteLength);
    return out;
  })();
  // Recompute commit oid for assertions
  const commitOid = await (async () => {
    const header = new TextEncoder().encode(`commit ${commitPayload2.byteLength}\0`);
    const raw = new Uint8Array(header.byteLength + commitPayload2.byteLength);
    raw.set(header, 0);
    raw.set(commitPayload2, header.byteLength);
    const hash = await crypto.subtle.digest("SHA-1", raw);
    return Array.from(new Uint8Array(hash))
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
  })();

  // Two commands: one valid create (zero old), one invalid create (non-zero old) -> atomic should prevent applying any
  const cmd1 = `${zero40()} ${commitOid} refs/heads/ok\0 report-status ofs-delta agent=test atomic\n`;
  const cmd2 = `${commitOid} ${commitOid} refs/heads/bad\n`;
  const body = concatChunks([pktLine(cmd1), pktLine(cmd2), flushPkt(), pack]);

  const res = await SELF.fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/x-git-receive-pack-request" },
    body,
  } as any);
  expect(res.status).toBe(200);
  const bytes = new Uint8Array(await res.arrayBuffer());
  const items = decodePktLines(bytes);
  const lines = items.filter((i) => i.type === "line").map((i: any) => (i.text as string).trim());
  expect(lines.some((l) => l.startsWith("unpack ok"))).toBe(true);
  expect(lines).toContain("ok refs/heads/ok");
  expect(lines.some((l) => l.startsWith("ng refs/heads/bad"))).toBe(true);

  // Atomic application: none of the refs should exist
  const refsRes = await SELF.fetch(`https://example.com/${owner}/${repo}/admin/refs`);
  expect(refsRes.status).toBe(200);
  const refs = await refsRes.json<any>();
  expect(refs.find((r: any) => r.name === "refs/heads/ok")).toBeUndefined();
  expect(refs.find((r: any) => r.name === "refs/heads/bad")).toBeUndefined();
});
