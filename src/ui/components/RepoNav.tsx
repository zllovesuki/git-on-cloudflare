import { Settings } from "lucide-react";
import { IslandHost } from "@/ui/server/IslandHost";
import { RefPickerIsland } from "@/ui/islands/ref-picker";
import { PageHeader } from "@/ui/components/ui/page-header";

function decodeRef(refEnc: string): string {
  try {
    return decodeURIComponent(refEnc);
  } catch {
    return refEnc;
  }
}

type RepoNavProps = {
  owner: string;
  repo: string;
  refEnc?: string;
  currentTab?: "browse" | "commits" | "admin";
  showRefDropdown?: boolean;
};

const tabBase =
  "inline-flex items-center gap-1.5 rounded-lg px-3 py-1.5 text-sm text-zinc-500 dark:text-zinc-400 hover:bg-zinc-100 dark:hover:bg-zinc-800/40 hover:text-zinc-900 dark:hover:text-zinc-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent-500/50";

const tabActive =
  "inline-flex items-center gap-1.5 rounded-lg px-3 py-1.5 text-sm font-medium bg-accent-100/50 dark:bg-accent-900/20 text-accent-700 dark:text-accent-300 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent-500/50";

export function RepoNav({ owner, repo, refEnc, currentTab, showRefDropdown = true }: RepoNavProps) {
  const decodedRef = refEnc ? decodeRef(refEnc) : "";

  return (
    <PageHeader>
      <div className="font-semibold text-lg">
        <a href={`/${owner}`} className="hover:text-accent-600 dark:hover:text-accent-400">
          {owner}
        </a>
        <span className="mx-1 text-zinc-500 dark:text-zinc-400">/</span>
        <a href={`/${owner}/${repo}`} className="hover:text-accent-600 dark:hover:text-accent-400">
          {repo}
        </a>
      </div>
      {refEnc ? (
        <div className="flex items-center gap-3">
          <nav className="flex items-center gap-3" aria-label="Repository navigation">
            {showRefDropdown ? (
              <IslandHost
                name="ref-picker"
                props={{ owner, repo, currentRef: decodedRef }}
                className="relative"
              >
                <RefPickerIsland owner={owner} repo={repo} currentRef={decodedRef} />
              </IslandHost>
            ) : null}
            <div className="inline-flex items-center gap-1 rounded-xl border border-zinc-200 dark:border-zinc-700 bg-white dark:bg-zinc-900 p-1">
              <a
                href={`/${owner}/${repo}/tree?ref=${refEnc}`}
                className={currentTab === "browse" ? tabActive : tabBase}
              >
                Browse
              </a>
              <a
                href={`/${owner}/${repo}/commits?ref=${refEnc}`}
                className={currentTab === "commits" ? tabActive : tabBase}
              >
                Commits
              </a>
              <a
                href={`/${owner}/${repo}/admin`}
                className={currentTab === "admin" ? tabActive : tabBase}
              >
                <Settings className="h-4 w-4" aria-hidden="true" />
                <span>Admin</span>
              </a>
            </div>
          </nav>
        </div>
      ) : (
        <div></div>
      )}
    </PageHeader>
  );
}
