import { useState } from "react";
import { Check, Clipboard } from "lucide-react";

export function JsonResult({ data }: { data: unknown }) {
  const [copied, setCopied] = useState(false);
  const serialized = JSON.stringify(data, null, 2);

  async function copyJson() {
    try {
      await navigator.clipboard.writeText(serialized);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1200);
    } catch (error) {
      window.alert(`Failed to copy JSON: ${String(error)}`);
    }
  }

  return (
    <div className="relative mt-2">
      <pre className="overflow-x-auto rounded-xl bg-zinc-100 p-4 pr-12 text-sm dark:bg-zinc-800">
        {serialized}
      </pre>
      <button
        type="button"
        onClick={() => void copyJson()}
        className="absolute right-2 top-2 rounded border border-zinc-300 bg-white/80 p-1.5 text-zinc-600 transition-colors hover:bg-zinc-100 hover:text-zinc-900 dark:border-zinc-700 dark:bg-zinc-900/60 dark:text-zinc-300 dark:hover:bg-zinc-800 dark:hover:text-zinc-100"
        title={copied ? "Copied" : "Copy JSON"}
        aria-label="Copy JSON"
      >
        {copied ? (
          <Check className="block h-4 w-4 text-green-600 dark:text-green-400" aria-hidden="true" />
        ) : (
          <Clipboard className="block h-4 w-4" aria-hidden="true" />
        )}
      </button>
    </div>
  );
}
