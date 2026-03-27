import { useState } from "react";
import { Database } from "lucide-react";
import { Button } from "@/ui/components/ui/button";
import { Card } from "@/ui/components/ui/card";
import { JsonResult } from "./JsonResult";

export type DebugToolsCardProps = {
  oidResult: unknown;
  stateDump: unknown;
  pending: Record<string, boolean>;
  checkOid: (oid: string) => Promise<void>;
  dumpState: () => Promise<void>;
};

export function DebugToolsCard({
  oidResult,
  stateDump,
  pending,
  checkOid,
  dumpState,
}: DebugToolsCardProps) {
  const [debugOid, setDebugOid] = useState("");

  return (
    <Card>
      <h2 className="mb-4 text-xl font-semibold">Debug Tools</h2>
      <div className="space-y-4">
        <div>
          <label htmlFor="debug-oid" className="mb-2 block text-sm font-medium">
            Check Object ID Existence
          </label>
          <div className="flex gap-2">
            <input
              type="text"
              id="debug-oid"
              placeholder="Enter 40-character SHA-1 hash"
              className="flex-1 rounded-xl border border-zinc-300 bg-white px-3 py-2 font-mono text-sm focus:ring-2 focus:ring-accent-500 dark:border-zinc-700 dark:bg-zinc-800"
              pattern="[a-f0-9]{40}"
              value={debugOid}
              onChange={(event) => setDebugOid(event.target.value)}
            />
            <Button
              type="button"
              onClick={() => void checkOid(debugOid)}
              disabled={pending["check-oid"]}
            >
              {pending["check-oid"] ? "Checking..." : "Check OID"}
            </Button>
          </div>
          {oidResult ? <JsonResult data={oidResult} /> : null}
        </div>

        <div>
          <label className="mb-2 block text-sm font-medium">Repository State Dump</label>
          <Button type="button" onClick={() => void dumpState()} disabled={pending["dump-state"]}>
            <Database className="mr-2 inline h-4 w-4 align-[-2px]" aria-hidden="true" />
            <span className="label">
              {pending["dump-state"] ? "Processing..." : "View DO State"}
            </span>
          </Button>
          {stateDump ? <JsonResult data={stateDump} /> : null}
        </div>
      </div>
    </Card>
  );
}
