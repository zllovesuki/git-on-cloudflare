// Stable public entrypoint for rewrite-planner callers and tests.

export { buildSelection, compactDeadSlots, type BuildSelectionResult } from "./selection.ts";
export { buildOutputOrder, canPassthroughSinglePack, computeHeaderLengths } from "./layout.ts";
