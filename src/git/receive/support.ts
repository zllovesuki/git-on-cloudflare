import type { Logger } from "@/common/logger.ts";
import type { ReceiveCommand, ReceiveStatus } from "@/git/operations/validation.ts";

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
  statuses: ReceiveStatus[];
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

export function buildReceiveUnpackFailureReport(
  commands: ReceiveCommand[],
  unpackMessage: string,
  statusMessage: string = "unpack-failed"
): Uint8Array {
  const statuses: ReceiveStatus[] = commands.map((command) => ({
    ref: command.ref,
    ok: false,
    msg: statusMessage,
  }));
  return buildReceiveReportStatus({
    unpackOk: false,
    unpackMessage,
    commands,
    statuses,
  });
}
