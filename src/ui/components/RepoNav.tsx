import { Settings } from "lucide-react";
import { IslandHost } from "@/ui/server/IslandHost";
import { RefPickerIsland } from "@/ui/islands/ref-picker";

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

export function RepoNav({ owner, repo, refEnc, currentTab, showRefDropdown = true }: RepoNavProps) {
  const decodedRef = refEnc ? decodeRef(refEnc) : "";

  return (
    <header className="page-header">
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
            <div className="subnav">
              <a
                href={`/${owner}/${repo}/tree?ref=${refEnc}`}
                className="tab"
                aria-current={currentTab === "browse" ? "page" : undefined}
              >
                Browse
              </a>
              <a
                href={`/${owner}/${repo}/commits?ref=${refEnc}`}
                className="tab"
                aria-current={currentTab === "commits" ? "page" : undefined}
              >
                Commits
              </a>
              <a
                href={`/${owner}/${repo}/admin`}
                className="tab"
                aria-current={currentTab === "admin" ? "page" : undefined}
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
    </header>
  );
}
