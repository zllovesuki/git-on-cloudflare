export function repositoryNotReadyResponse(): Response {
  return new Response(
    "Repository not ready for fetch. Objects are being packed, please retry in a few moments.\n",
    {
      status: 503,
      headers: {
        "Retry-After": "5",
        "Content-Type": "text/plain; charset=utf-8",
        "X-Git-Error": "repository-not-ready",
      },
    }
  );
}
