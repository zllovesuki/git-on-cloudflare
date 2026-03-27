import { CodeViewer } from "@/ui/components/CodeViewer";
import { MarkdownContent } from "@/ui/components/MarkdownContent";
import { RepoNav } from "@/ui/components/RepoNav";
import { BlobActionsIsland } from "@/ui/islands/blob-actions";
import { IslandHost } from "@/ui/server/IslandHost";

export type BlobPageProps = {
  owner: string;
  repo: string;
  refEnc: string;
  fileName: string;
  codeLang?: string | null;
  lineCount?: number;
  sizeStr?: string;
  viewRawHref: string;
  rawHref: string;
  tooLarge?: boolean;
  isImage?: boolean;
  isPdf?: boolean;
  mediaSrc?: string;
  isMarkdown?: boolean;
  markdownRaw?: string;
  mdOwner?: string;
  mdRepo?: string;
  mdRef?: string;
  mdBase?: string;
  isBinary?: boolean;
  codeText?: string;
};

export function BlobPage(props: BlobPageProps) {
  const {
    owner,
    repo,
    refEnc,
    fileName,
    codeLang,
    lineCount,
    sizeStr,
    viewRawHref,
    rawHref,
    tooLarge,
    isImage,
    isPdf,
    mediaSrc,
    isMarkdown,
    markdownRaw,
    mdOwner,
    mdRepo,
    mdRef,
    mdBase,
    isBinary,
    codeText,
  } = props;

  const showCopy = !isMarkdown && !isImage && !isPdf && !isBinary && !tooLarge;

  return (
    <div className="animate-slide-up">
      <RepoNav owner={owner} repo={repo} refEnc={refEnc} currentTab="browse" />
      <span className="mb-1 inline-block text-xs font-semibold uppercase tracking-wider text-accent-500 dark:text-accent-400">
        File
      </span>
      <h2>Blob: {fileName}</h2>
      <div className="blob-container mt-4 rounded-2xl border border-zinc-200 bg-zinc-50 dark:border-zinc-800/60 dark:bg-zinc-900/50">
        <div className="flex items-center justify-between border-b border-zinc-200 px-3 py-2 dark:border-zinc-800/60">
          <div className="flex items-center gap-2 text-xs text-zinc-600 dark:text-zinc-400">
            {codeLang ? (
              <span className="inline-flex items-center rounded-lg border border-zinc-300 bg-zinc-50 px-2 py-0.5 dark:border-zinc-700 dark:bg-zinc-800/50">
                {codeLang}
              </span>
            ) : null}
            {isMarkdown ? (
              <span className="inline-flex items-center rounded-lg border border-zinc-300 bg-zinc-50 px-2 py-0.5 dark:border-zinc-700 dark:bg-zinc-800/50">
                Markdown
              </span>
            ) : null}
            {isMarkdown || codeLang ? (
              lineCount ? (
                <span className="text-zinc-500 dark:text-zinc-400">
                  {lineCount} line{lineCount === 1 ? "" : "s"}
                </span>
              ) : null
            ) : sizeStr ? (
              <span className="text-zinc-500 dark:text-zinc-400">{sizeStr}</span>
            ) : null}
          </div>
          <IslandHost
            name="blob-actions"
            props={{ viewRawHref, rawHref, showCopy, isImage, isPdf }}
            className="flex items-center gap-2"
          >
            <BlobActionsIsland
              viewRawHref={viewRawHref}
              rawHref={rawHref}
              showCopy={showCopy}
              isImage={isImage}
              isPdf={isPdf}
            />
          </IslandHost>
        </div>
        <div className={`${isMarkdown ? "p-5" : isBinary || tooLarge ? "p-3" : "p-1"}`}>
          {tooLarge ? (
            <div className="text-zinc-500 dark:text-zinc-400">File too large to preview.</div>
          ) : null}
          {!tooLarge && isImage && mediaSrc ? (
            <div className="flex items-center justify-center">
              <img src={mediaSrc} alt={fileName} className="max-w-full rounded-md" loading="lazy" />
            </div>
          ) : null}
          {!tooLarge && isPdf && mediaSrc ? (
            <div className="w-full" style={{ height: "75vh" }}>
              <iframe
                src={mediaSrc}
                className="h-full w-full rounded-md"
                title={fileName}
                loading="lazy"
              ></iframe>
            </div>
          ) : null}
          {!tooLarge && isMarkdown && markdownRaw && mdOwner && mdRepo && mdRef ? (
            <MarkdownContent
              markdown={markdownRaw}
              context={{ owner: mdOwner, repo: mdRepo, ref: mdRef, baseDir: mdBase || "" }}
            />
          ) : null}
          {!tooLarge && isBinary && !isImage && !isPdf ? (
            <div className="text-zinc-500 dark:text-zinc-400">Binary file.</div>
          ) : null}
          {!tooLarge && !isBinary && !isMarkdown && codeText !== undefined ? (
            <CodeViewer code={codeText} language={codeLang || null} />
          ) : null}
        </div>
      </div>
    </div>
  );
}
