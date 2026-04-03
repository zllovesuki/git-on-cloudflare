import type { CacheContext } from "@/cache/index.ts";
import type { IdxView } from "@/git/object-store/types.ts";

export type OrderedPackSnapshotEntry = {
  packKey: string;
  packBytes: number;
  idx: IdxView;
};

export type OrderedPackSnapshot = {
  packs: OrderedPackSnapshotEntry[];
};

export type ServeUploadPackPlan = {
  type: "Serve";
  repoId: string;
  snapshot: OrderedPackSnapshot;
  neededOids: string[];
  ackOids: string[];
  signal?: AbortSignal;
  cacheCtx?: CacheContext;
};

export type UploadPackPlan =
  | ServeUploadPackPlan
  | {
      type: "RepositoryNotReady";
    };
