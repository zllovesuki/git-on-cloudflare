import type { AppRouter } from "./hono";

import { registerAuthNamespaceRoutes } from "./authNamespaces";
import { registerAuthOidcRoutes } from "./authOidc";
import { registerAuthRepositoryRoutes } from "./authRepositories";
import { registerAuthTokenRoutes } from "./authTokens";

export function registerAuthRoutes(router: AppRouter) {
  registerAuthOidcRoutes(router);
  registerAuthTokenRoutes(router);
  registerAuthRepositoryRoutes(router);
  registerAuthNamespaceRoutes(router);
}
