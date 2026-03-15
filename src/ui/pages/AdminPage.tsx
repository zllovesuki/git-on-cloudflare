import { type Progress, ProgressBanner } from "@/ui/components/ProgressBanner";
import { RepoNav } from "@/ui/components/RepoNav";
import { type RepoAdminProps, RepoAdminIsland } from "@/ui/islands/repo-admin";
import { IslandHost } from "@/ui/server/IslandHost";

export type AdminPageProps = RepoAdminProps & {
  progress?: Progress;
};

export function AdminPage({ progress, ...props }: AdminPageProps) {
  return (
    <>
      <RepoNav
        owner={props.owner}
        repo={props.repo}
        refEnc={props.refEnc}
        currentTab="admin"
        showRefDropdown={false}
      />
      <ProgressBanner progress={progress} />
      <IslandHost name="repo-admin" props={props}>
        <RepoAdminIsland {...props} />
      </IslandHost>
    </>
  );
}
