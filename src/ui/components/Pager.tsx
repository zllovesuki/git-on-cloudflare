import { Button } from "@/ui/components/ui/button";

type PagerLink = {
  text: string;
  href: string;
};

type PagerModel = {
  perPageLinks: PagerLink[];
  newerHref?: string;
  olderHref?: string;
};

type PagerProps = {
  pager?: PagerModel | null;
};

export function Pager({ pager }: PagerProps) {
  if (!pager) {
    return null;
  }

  return (
    <div className="my-6 flex items-center justify-between rounded-xl bg-zinc-100 dark:bg-zinc-800/30 p-4">
      <div className="flex items-center gap-3 text-sm">
        <span className="py-1 text-zinc-600 dark:text-zinc-400">Per page:</span>
        {pager.perPageLinks.map((link) => (
          <a
            key={link.href}
            href={link.href}
            className="rounded-sm px-2 py-1 transition-colors hover:bg-zinc-200 dark:hover:bg-zinc-700"
          >
            {link.text}
          </a>
        ))}
      </div>
      <div className="flex gap-3">
        {pager.newerHref ? (
          <Button variant="secondary" size="sm" href={pager.newerHref}>
            ← Newer
          </Button>
        ) : null}
        {pager.olderHref ? (
          <Button size="sm" href={pager.olderHref}>
            Older →
          </Button>
        ) : null}
      </div>
    </div>
  );
}
