import { IslandHost } from "@/ui/server/IslandHost";
import { AuthAdminIsland } from "@/ui/islands/auth-admin";

export function AuthPage() {
  return (
    <IslandHost name="auth-admin" props={{}}>
      <AuthAdminIsland />
    </IslandHost>
  );
}
