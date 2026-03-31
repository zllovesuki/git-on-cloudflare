import { useState } from "react";
import { safeReadJson } from "@/client/json.ts";
import { isJsonObject, type JsonValue } from "@/web";
import type { RepoStorageMode, RepoStorageModeMutationResult } from "./types";

export function useRepoAdminActions(owner: string, repo: string) {
  const [compactionResult, setCompactionResult] = useState<JsonValue | null>(null);
  const [storageModeResult, setStorageModeResult] = useState<RepoStorageModeMutationResult | null>(
    null
  );
  const [backfillResult, setBackfillResult] = useState<JsonValue | null>(null);
  const [oidResult, setOidResult] = useState<JsonValue | null>(null);
  const [stateDump, setStateDump] = useState<JsonValue | null>(null);
  const [pending, setPending] = useState<Record<string, boolean>>({});

  function readFlag(value: JsonValue | null, key: string): boolean {
    return isJsonObject(value) && value[key] === true;
  }

  function readErrorMessage(value: JsonValue | null): string {
    return isJsonObject(value) && typeof value.error === "string" ? value.error : "Unknown error";
  }

  async function runAction<T>(key: string, action: () => Promise<T>) {
    setPending((current) => ({ ...current, [key]: true }));
    try {
      return await action();
    } finally {
      setPending((current) => ({ ...current, [key]: false }));
    }
  }

  async function setStorageMode(mode: RepoStorageMode) {
    await runAction(`storage-mode:${mode}`, async () => {
      const response = await fetch(`/${owner}/${repo}/admin/storage-mode`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ mode }),
      });
      const data = (await safeReadJson(response)) as RepoStorageModeMutationResult | null;
      setStorageModeResult(data);
      if (response.ok) {
        window.setTimeout(() => window.location.reload(), 1200);
      }
    });
  }

  async function requestLegacyCompatBackfill() {
    await runAction("storage-mode:backfill", async () => {
      const response = await fetch(`/${owner}/${repo}/admin/storage-mode/backfill`, {
        method: "POST",
      });
      const data = await safeReadJson(response);
      setBackfillResult(data);
      if (response.ok) {
        window.setTimeout(() => window.location.reload(), 1500);
      }
    });
  }

  async function startCompaction(dryRun: boolean) {
    await runAction(dryRun ? "compaction-dry-run" : "compaction-start", async () => {
      const response = await fetch(`/${owner}/${repo}/admin/compact`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ dryRun }),
      });
      const data = await safeReadJson(response);
      setCompactionResult(data);
      if (response.ok && !dryRun) {
        window.setTimeout(() => window.location.reload(), 2000);
      }
    });
  }

  async function clearCompaction() {
    if (!window.confirm("Clear the recorded compaction request for this repository?")) {
      return;
    }

    await runAction("compaction-clear", async () => {
      const response = await fetch(`/${owner}/${repo}/admin/compact`, { method: "DELETE" });
      const data = await safeReadJson(response);
      if (readFlag(data, "ok")) {
        window.alert("Compaction request cleared successfully");
        window.location.reload();
        return;
      }

      window.alert(`Error: ${readErrorMessage(data)}`);
    });
  }

  async function removePack(packName: string) {
    const warning =
      `Are you sure you want to remove pack: ${packName}?\n\n` +
      `This deletes the pack file, its index, and associated metadata.\n\n` +
      `Only superseded packs should be removed during normal operation. ` +
      `Active packs may still be referenced until compaction replaces them.`;
    if (!window.confirm(warning)) {
      return;
    }

    await runAction(`remove-pack:${packName}`, async () => {
      const response = await fetch(`/${owner}/${repo}/admin/pack/${encodeURIComponent(packName)}`, {
        method: "DELETE",
      });
      const data = await safeReadJson(response);
      if (readFlag(data, "ok")) {
        window.alert(
          `Pack removed successfully:\n- Pack file: ${readFlag(data, "deletedPack") ? "deleted" : "not found"}\n- Index file: ${readFlag(data, "deletedIndex") ? "deleted" : "not found"}\n- Metadata: ${readFlag(data, "deletedMetadata") ? "cleaned" : "unchanged"}`
        );
        window.location.reload();
        return;
      }

      window.alert(`Error removing pack: ${readErrorMessage(data)}`);
    });
  }

  async function checkOid(debugOid: string) {
    if (!debugOid || !/^[a-f0-9]{40}$/i.test(debugOid.trim())) {
      window.alert("Please enter a valid 40-character SHA-1 hash");
      return;
    }

    await runAction("check-oid", async () => {
      const response = await fetch(`/${owner}/${repo}/admin/debug-oid/${debugOid.trim()}`);
      setOidResult(await safeReadJson(response));
    });
  }

  async function dumpState() {
    await runAction("dump-state", async () => {
      const response = await fetch(`/${owner}/${repo}/admin/debug-state`);
      setStateDump(await safeReadJson(response));
    });
  }

  async function purgeRepo() {
    const confirmation = window.prompt(
      `This action will PERMANENTLY DELETE all repository data.\n\nTo confirm, type exactly: purge-${owner}/${repo}`
    );
    if (confirmation !== `purge-${owner}/${repo}`) {
      if (confirmation !== null) {
        window.alert("Confirmation text did not match. Action cancelled.");
      }
      return;
    }
    if (!window.confirm("Final confirmation: Delete this repository forever?")) {
      return;
    }

    await runAction("purge-repo", async () => {
      const response = await fetch(`/${owner}/${repo}/admin/purge`, {
        method: "DELETE",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ confirm: confirmation }),
      });
      const data = await safeReadJson(response);
      if (readFlag(data, "ok")) {
        window.alert("Repository has been permanently deleted");
        window.location.href = `/${owner}`;
        return;
      }

      window.alert(`Error: ${readErrorMessage(data)}`);
    });
  }

  return {
    compactionResult,
    storageModeResult,
    backfillResult,
    oidResult,
    stateDump,
    pending,
    setStorageMode,
    requestLegacyCompatBackfill,
    startCompaction,
    clearCompaction,
    removePack,
    checkOid,
    dumpState,
    purgeRepo,
  };
}
