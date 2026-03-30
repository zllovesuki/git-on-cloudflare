import type { Logger } from "@/common/logger.ts";

export class ResolveAbortedError extends Error {
  readonly stage: string;

  constructor(stage: string) {
    super(`resolve: aborted during ${stage}`);
    this.name = "ResolveAbortedError";
    this.stage = stage;
  }
}

export function isResolveAbortedError(error: unknown): error is ResolveAbortedError {
  return (
    error instanceof ResolveAbortedError ||
    (error instanceof Error && error.name === "ResolveAbortedError")
  );
}

export function throwIfAborted(
  signal: AbortSignal | undefined,
  log: Logger | undefined,
  stage: string
): void {
  if (!signal?.aborted) return;
  log?.debug("resolve:aborted", { stage });
  throw new ResolveAbortedError(stage);
}
