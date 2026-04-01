import { pktLine, flushPkt, concatChunks, decodePktLines } from "./pktline.ts";
import { asBodyInit } from "@/common/webtypes.ts";
import { getRepoStub } from "@/common/stub.ts";

/**
 * Generates a Git capability advertisement response.
 * Lists all refs and capabilities for the requested service.
 * @param env - Worker environment
 * @param service - Git service (git-upload-pack or git-receive-pack)
 * @param repoId - Repository identifier
 * @returns HTTP response with capability advertisement
 */
export async function capabilityAdvertisement(
  env: Env,
  service: "git-upload-pack" | "git-receive-pack",
  repoId?: string
) {
  if (service === "git-upload-pack") {
    const chunks: Uint8Array[] = [];
    chunks.push(pktLine("version 2\n"));
    chunks.push(pktLine(`agent=git-on-cloudflare/0.1\n`));
    chunks.push(pktLine("ls-refs\n"));
    // Advertise fetch and supported features
    chunks.push(pktLine("fetch\n"));
    // We stream pack data over sideband; advertise side-band-64k for client awareness
    chunks.push(pktLine("side-band-64k\n"));
    chunks.push(pktLine("ofs-delta\n"));
    chunks.push(pktLine(`object-format=sha1\n`));
    chunks.push(flushPkt());
    return new Response(asBodyInit(concatChunks(chunks)), {
      status: 200,
      headers: {
        "Content-Type": "application/x-git-upload-pack-advertisement",
        "Cache-Control": "no-cache",
      },
    });
  }

  // git-receive-pack uses v0-style advertisement of refs with capabilities on first line.
  // Query the repo DO for current refs so clients include the correct old OID,
  // enabling non-fast-forward (force) pushes when desired.
  let refs: { name: string; oid: string }[] = [];
  let supportsStreamingReceiveSideband = false;
  if (repoId) {
    try {
      const stub = getRepoStub(env, repoId);
      const [data, storageMode] = await Promise.all([
        stub.listRefs(),
        stub.getRepoStorageMode().catch(() => "legacy"),
      ]);
      if (Array.isArray(data)) refs = data as { name: string; oid: string }[];
      supportsStreamingReceiveSideband = storageMode === "streaming";
    } catch {}
  }
  const caps = [
    "report-status",
    "delete-refs",
    supportsStreamingReceiveSideband ? "side-band-64k" : undefined,
    supportsStreamingReceiveSideband ? "quiet" : undefined,
    "atomic",
    "ofs-delta",
    `agent=git-on-cloudflare/0.1`,
  ]
    .filter((cap): cap is string => Boolean(cap))
    .join(" ");

  const lines: Uint8Array[] = [];
  // Smart HTTP service prelude then flush
  lines.push(pktLine(`# service=git-receive-pack\n`));
  lines.push(flushPkt());
  // Only advertise real refs (no HEAD entry). Put caps on the first ref line.
  if (refs.length > 0) {
    for (let i = 0; i < refs.length; i++) {
      const r = refs[i];
      if (i === 0) lines.push(pktLine(`${r.oid} ${r.name}\0${caps}\n`));
      else lines.push(pktLine(`${r.oid} ${r.name}\n`));
    }
  } else {
    // Empty repo: advertise capabilities using the pseudo-ref 'capabilities^{}'
    lines.push(pktLine(`0000000000000000000000000000000000000000 capabilities^{}\0${caps}\n`));
  }
  lines.push(flushPkt());
  return new Response(asBodyInit(concatChunks(lines)), {
    status: 200,
    headers: {
      "Content-Type": "application/x-git-receive-pack-advertisement",
      "Cache-Control": "no-cache",
    },
  });
}

/**
 * Parses a Git protocol v2 command from the request body.
 * Extracts the command from pkt-line formatted data.
 * @param body - Raw request body
 * @returns Object containing the parsed command, if present
 */
export function parseV2Command(body: Uint8Array): { command: string; args: string[] } {
  const items = decodePktLines(body);
  let command = "";
  const args: string[] = [];
  if (items.length > 0) {
    let beforeDelim = true;
    for (const it of items) {
      if (it.type === "delim") {
        beforeDelim = false;
        continue;
      }
      if (it.type !== "line") continue;
      const text = it.text.replace(/\r?\n$/, "");
      if (beforeDelim) {
        if (text.startsWith("command=")) {
          command = text.slice("command=".length);
        }
      } else {
        args.push(text);
      }
    }
  }
  if (!command) {
    const text = new TextDecoder().decode(body);
    const m = text.match(/command=([a-z-]+)/);
    command = m ? m[1] : "";
  }
  return { command, args };
}
