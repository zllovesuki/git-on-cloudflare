type BreadcrumbItem = {
  name: string;
  href?: string | null;
};

type BreadcrumbsProps = {
  items?: BreadcrumbItem[];
  parentHref?: string | null;
};

export function Breadcrumbs({ items, parentHref }: BreadcrumbsProps) {
  if (!items?.length) {
    return null;
  }

  return (
    <nav className="breadcrumb" aria-label="Breadcrumbs">
      {parentHref ? (
        <>
          <a
            href={parentHref}
            className="text-zinc-500 hover:text-zinc-700 dark:text-zinc-400 dark:hover:text-zinc-200"
          >
            ..
          </a>
          <span className="text-zinc-400 dark:text-zinc-600">/</span>
        </>
      ) : null}
      {items.map((item, index) => (
        <span key={`${item.name}-${index}`}>
          {item.href ? <a href={item.href}>{item.name}</a> : <strong>{item.name}</strong>}
          {index < items.length - 1 ? " / " : null}
        </span>
      ))}
    </nav>
  );
}
