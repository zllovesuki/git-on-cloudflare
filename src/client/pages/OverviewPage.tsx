import { BookOpen, GitBranch, Tag } from "lucide-react";
import { MarkdownContent } from "@/client/components/MarkdownContent";
import { type Progress, ProgressBanner } from "@/client/components/ProgressBanner";
import { RepoNav } from "@/client/components/RepoNav";
import { Card } from "@/client/components/ui/card";
import { EmptyState } from "@/client/components/EmptyState";

type RefLink = {
  name: string;
  displayName: string;
};

export type OverviewPageProps = {
  owner: string;
  repo: string;
  refShort: string;
  refEnc: string;
  branches: RefLink[];
  tags: RefLink[];
  readmeMd?: string;
  progress?: Progress;
};

export function OverviewPage({
  owner,
  repo,
  refShort,
  refEnc,
  branches,
  tags,
  readmeMd,
  progress,
}: OverviewPageProps) {
  return (
    <>
      <RepoNav owner={owner} repo={repo} refEnc={refEnc} showRefDropdown={false} />
      <ProgressBanner progress={progress} />
      <div className="mt-6 grid gap-6 md:grid-cols-[1fr_2fr]">
        {/* Left column: Refs */}
        <div className="space-y-6">
          <Card>
            <h3 className="flex items-center gap-2">
              <GitBranch className="inline h-4 w-4 text-accent-500" aria-hidden="true" />
              Branches
            </h3>
            <p className="mb-2 text-sm text-zinc-500 dark:text-zinc-400">
              Default: <code>{refShort}</code>
            </p>
            <div className="[&>div]:border-b [&>div]:border-zinc-200 dark:[&>div]:border-zinc-800 [&>div]:py-2 [&>div:last-child]:border-b-0">
              {branches.length ? (
                branches.map((branch) => (
                  <div key={branch.name}>
                    <a href={`/${owner}/${repo}/tree?ref=${branch.name}`}>{branch.displayName}</a>
                  </div>
                ))
              ) : (
                <EmptyState
                  icon={
                    <GitBranch
                      className="h-5 w-5 text-zinc-500 dark:text-zinc-400"
                      aria-hidden="true"
                    />
                  }
                  title="No branches yet"
                />
              )}
            </div>
          </Card>
          <Card>
            <h3 className="flex items-center gap-2">
              <Tag className="inline h-4 w-4 text-accent-500" aria-hidden="true" />
              Tags
            </h3>
            <div className="[&>div]:border-b [&>div]:border-zinc-200 dark:[&>div]:border-zinc-800 [&>div]:py-2 [&>div:last-child]:border-b-0">
              {tags.length ? (
                tags.map((tag) => (
                  <div key={tag.name}>
                    <a href={`/${owner}/${repo}/tree?ref=${tag.name}`}>{tag.displayName}</a>
                  </div>
                ))
              ) : (
                <EmptyState
                  icon={
                    <Tag className="h-5 w-5 text-zinc-500 dark:text-zinc-400" aria-hidden="true" />
                  }
                  title="No tags yet"
                />
              )}
            </div>
          </Card>
        </div>
        {/* Right column: README */}
        <div>
          {readmeMd ? (
            <Card>
              <MarkdownContent
                markdown={readmeMd}
                context={{ owner, repo, ref: refShort, baseDir: "" }}
              />
            </Card>
          ) : (
            <Card>
              <EmptyState
                icon={
                  <BookOpen
                    className="h-5 w-5 text-zinc-500 dark:text-zinc-400"
                    aria-hidden="true"
                  />
                }
                title="No README found"
                detail="Add a README.md to the root of your repository."
              />
            </Card>
          )}
        </div>
      </div>
    </>
  );
}
