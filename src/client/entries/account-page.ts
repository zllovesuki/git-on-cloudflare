import { initClaimNamespaceIsland } from "@/client/islands/claimNamespace";
import { initRepositoriesIsland } from "@/client/islands/repositories";
import { initTokensIsland } from "@/client/islands/tokens";
import { onReady } from "../on-ready";

onReady(() => {
  initClaimNamespaceIsland();
  initRepositoriesIsland();
  initTokensIsland();
});
