import type { ReactNode } from "react";

import { Header } from "@/client/components/header";
import { Footer } from "@/client/components/footer";

type AppLayoutProps = {
  children: ReactNode;
  currentView?: string;
};

export function AppLayout({ children, currentView }: AppLayoutProps) {
  return (
    <>
      <a
        href="#main-content"
        className="sr-only focus:not-sr-only focus:fixed focus:top-4 focus:left-4 focus:z-[200] focus:rounded-lg focus:bg-zinc-900 focus:px-4 focus:py-2 focus:text-accent-400 focus:ring-2 focus:ring-accent-500/50"
      >
        Skip to content
      </a>
      <div className="relative z-10 min-h-screen flex flex-col">
        <Header currentView={currentView} />
        <main
          id="main-content"
          className="mx-auto w-full max-w-7xl flex-1 px-4 py-6 sm:px-6 min-w-0"
        >
          <div className="animate-slide-up">{children}</div>
        </main>
        <Footer />
      </div>
    </>
  );
}
