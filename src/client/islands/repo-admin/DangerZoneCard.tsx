import { Trash2, TriangleAlert } from "lucide-react";
import { Button } from "@/client/components/ui/button";

export type DangerZoneCardProps = {
  defaultBranch: string;
  packList: string[];
  pending: Record<string, boolean>;
  purgeRepo: () => Promise<void>;
};

export function DangerZoneCard({
  defaultBranch,
  packList,
  pending,
  purgeRepo,
}: DangerZoneCardProps) {
  return (
    <details className="rounded-2xl border-2 border-red-500 bg-white dark:bg-zinc-900/50 p-5 sm:p-6 dark:border-red-600">
      <summary className="cursor-pointer font-bold text-red-600 dark:text-red-500">
        <TriangleAlert className="mr-2 inline h-4 w-4 align-[-2px]" aria-hidden="true" />
        Danger Zone - Irreversible Actions
      </summary>
      <div className="mt-6 space-y-4">
        <div className="rounded-xl border border-red-500/20 bg-red-500/10 p-4 text-sm text-red-400">
          <strong>Warning:</strong> These actions cannot be undone. All repository data will be
          permanently deleted.
        </div>
        <p className="text-sm text-zinc-600 dark:text-zinc-400">
          This will delete all objects, packs, references, and metadata associated with this
          repository. The repository will be removed from the owner registry.
        </p>
        <Button
          variant="danger"
          type="button"
          onClick={() => void purgeRepo()}
          disabled={pending["purge-repo"]}
        >
          <Trash2 className="mr-2 inline h-4 w-4 align-[-2px]" aria-hidden="true" />
          <span className="label">
            {pending["purge-repo"] ? "Deleting..." : "Permanently Delete Repository"}
          </span>
        </Button>
        <p className="text-zinc-500 dark:text-zinc-400 text-xs">
          Default branch: <code>{defaultBranch}</code>
        </p>
        {packList.length ? (
          <p className="text-zinc-500 dark:text-zinc-400 text-xs">
            Visible pack keys: {packList.length}
          </p>
        ) : null}
      </div>
    </details>
  );
}
