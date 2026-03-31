import type { Logger } from "@/common/logger.ts";
import type { ReceiveCommand } from "@/git/operations/validation.ts";

import { asBodyInit } from "@/common/index.ts";
import { concatChunks, flushPkt, pktLine } from "@/git/core/pktline.ts";
import { isResolveAbortedError } from "@/git/pack/indexer/index.ts";

export function throwIfReceiveAborted(request: Request, log: Logger, stage: string): void {
  if (!request.signal.aborted) return;
  log.debug("receive:aborted", { stage });
  const error = new Error(`receive: aborted during ${stage}`);
  error.name = "AbortError";
  throw error;
}

export function isReceiveAbort(request: Request, error: unknown): boolean {
  if (request.signal.aborted) return true;
  if (isResolveAbortedError(error)) return true;
  return error instanceof Error && error.name === "AbortError";
}

export function buildReceiveReportStatus(args: {
  unpackOk: boolean;
  unpackMessage?: string;
  commands: ReceiveCommand[];
  statuses: Array<{ ref: string; ok: boolean; msg?: string }>;
}): Uint8Array {
  const chunks: Uint8Array[] = [];
  chunks.push(
    pktLine(args.unpackOk ? "unpack ok\n" : `unpack error ${args.unpackMessage || "failed"}\n`)
  );
  for (let index = 0; index < args.commands.length; index++) {
    const command = args.commands[index];
    const status = args.statuses[index];
    if (status?.ok) {
      chunks.push(pktLine(`ok ${command.ref}\n`));
      continue;
    }
    chunks.push(pktLine(`ng ${command.ref} ${status?.msg || "rejected"}\n`));
  }
  chunks.push(flushPkt());
  return concatChunks(chunks);
}

export function invalidRefReport(commands: ReceiveCommand[], reason: string): Response {
  const statuses = commands.map((command) => ({
    ref: command.ref,
    ok: false,
    msg: "invalid",
  }));
  return new Response(
    asBodyInit(
      buildReceiveReportStatus({
        unpackOk: false,
        unpackMessage: reason,
        commands,
        statuses,
      })
    ),
    {
      status: 200,
      headers: {
        "Content-Type": "application/x-git-receive-pack-result",
        "Cache-Control": "no-cache",
        "X-Repo-Changed": "0",
        "X-Repo-Empty": "0",
      },
    }
  );
}

export function parseReceiveCommands(lines: string[]): ReceiveCommand[] {
  const commands: ReceiveCommand[] = [];
  for (let index = 0; index < lines.length; index++) {
    let line = lines[index] || "";
    if (index === 0) {
      const nulIndex = line.indexOf("\0");
      if (nulIndex >= 0) {
        line = line.slice(0, nulIndex);
      }
    }
    const parts = line.trim().split(/\s+/);
    if (parts.length < 3) continue;
    commands.push({
      oldOid: parts[0] || "",
      newOid: parts[1] || "",
      ref: parts.slice(2).join(" "),
    });
  }
  return commands;
}
