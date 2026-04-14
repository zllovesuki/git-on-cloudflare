// Stable public entrypoint for rewrite-planner callers and tests.

export { buildSelection, type BuildSelectionResult } from "./selection.ts";
export { compactDeadSlots } from "./selectionCompact.ts";
export { buildOutputOrder, canPassthroughSinglePack, computeHeaderLengths } from "./layout.ts";
