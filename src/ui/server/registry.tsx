import type { ReactElement } from "react";

import { AdminPage, type AdminPageProps } from "@/ui/pages/AdminPage";
import { AuthPage } from "@/ui/pages/AuthPage";
import { BlobPage, type BlobPageProps } from "@/ui/pages/BlobPage";
import { CommitPage, type CommitPageProps } from "@/ui/pages/CommitPage";
import { CommitsPage, type CommitsPageProps } from "@/ui/pages/CommitsPage";
import { ErrorPage, type ErrorPageProps } from "@/ui/pages/ErrorPage";
import { HomePage } from "@/ui/pages/HomePage";
import { NotFoundPage } from "@/ui/pages/NotFoundPage";
import { OverviewPage, type OverviewPageProps } from "@/ui/pages/OverviewPage";
import { OwnerPage, type OwnerPageProps } from "@/ui/pages/OwnerPage";
import { TreePage, type TreePageProps } from "@/ui/pages/TreePage";

type ViewDefinition = {
  kind: "document" | "fragment";
  title?: string;
  render: (data: Record<string, unknown>) => ReactElement;
};

function renderWithProps<Props extends object>(
  renderPage: (props: Props) => ReactElement
): (data: Record<string, unknown>) => ReactElement {
  return (data) => renderPage(data as Props);
}

const views: Record<string, ViewDefinition> = {
  home: {
    kind: "document",
    title: "git-on-cloudflare",
    render: () => <HomePage />,
  },
  "404": {
    kind: "document",
    title: "404 · git-on-cloudflare",
    render: () => <NotFoundPage />,
  },
  error: {
    kind: "document",
    title: "Error · git-on-cloudflare",
    render: renderWithProps((props: ErrorPageProps) => <ErrorPage {...props} />),
  },
  owner: {
    kind: "document",
    render: renderWithProps((props: OwnerPageProps) => <OwnerPage {...props} />),
  },
  overview: {
    kind: "document",
    render: renderWithProps((props: OverviewPageProps) => <OverviewPage {...props} />),
  },
  tree: {
    kind: "document",
    render: renderWithProps((props: TreePageProps) => <TreePage {...props} />),
  },
  blob: {
    kind: "document",
    render: renderWithProps((props: BlobPageProps) => <BlobPage {...props} />),
  },
  commit: {
    kind: "document",
    render: renderWithProps((props: CommitPageProps) => <CommitPage {...props} />),
  },
  commits: {
    kind: "document",
    render: renderWithProps((props: CommitsPageProps) => <CommitsPage {...props} />),
  },
  auth: {
    kind: "document",
    title: "Auth · git-on-cloudflare",
    render: () => <AuthPage />,
  },
  admin: {
    kind: "document",
    render: renderWithProps((props: AdminPageProps) => <AdminPage {...props} />),
  },
};

export function getViewDefinition(name: string): ViewDefinition | undefined {
  return views[name];
}
