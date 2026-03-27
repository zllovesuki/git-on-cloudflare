import type { ReactNode } from "react";

import { Header } from "@/ui/components/header";
import { Footer } from "@/ui/components/footer";

type AppLayoutProps = {
  children: ReactNode;
  currentView?: string;
};

export function AppLayout({ children, currentView }: AppLayoutProps) {
  return (
    <>
      <div className="relative z-10 min-h-screen flex flex-col">
        <Header currentView={currentView} />
        <main className="mx-auto w-full max-w-7xl flex-1 px-4 py-6 sm:px-6">{children}</main>
        <Footer />
      </div>
    </>
  );
}
