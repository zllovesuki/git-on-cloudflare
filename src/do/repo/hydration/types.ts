import type { RepoStateSchema, HydrationWork, TypedStorage } from "../repoState.ts";
import type { Logger } from "@/common/logger.ts";

export type HydrationCtx = {
  state: DurableObjectState;
  env: Env;
  prefix: string;
  store: TypedStorage<RepoStateSchema>;
  cfg: {
    unpackMaxMs: number;
    unpackDelayMs: number;
    unpackBackoffMs: number;
    chunk: number;
    keepPacks: number;
    windowMax: number;
  };
  log: Logger;
};

export type HydrationPlan = {
  snapshot: { lastPackKey: string | null; packListCount: number };
  window: { packKeys: string[] };
  counts: {
    deltaBases: number;
    looseOnly: number;
    totalCandidates: number;
    alreadyCovered: number;
    toPack: number;
  };
  segments: { estimated: number; maxObjectsPerSegment: number; maxBytesPerSegment: number };
  budgets: { timePerSliceMs: number; softSubrequestLimit: number };
  stats: { examinedPacks: number; examinedObjects: number; examinedLoose: number };
  warnings: string[];
  partial: boolean;
};

export type StageHandlerResult = {
  continue: boolean;
  persist?: boolean;
};

export type StageHandler = (ctx: HydrationCtx, work: HydrationWork) => Promise<StageHandlerResult>;
