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
    <div className="pager">
      <div className="perpage">
        Per page:
        {pager.perPageLinks.map((link) => (
          <a key={link.href} href={link.href}>
            {link.text}
          </a>
        ))}
      </div>
      <div className="nav">
        {pager.newerHref ? (
          <a className="btn secondary sm" href={pager.newerHref}>
            ← Newer
          </a>
        ) : null}
        {pager.olderHref ? (
          <a className="btn sm" href={pager.olderHref}>
            Older →
          </a>
        ) : null}
      </div>
    </div>
  );
}
