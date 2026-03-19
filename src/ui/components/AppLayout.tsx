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
      <div className="ambient-glow" aria-hidden="true"></div>
      <div className="relative z-10 min-h-screen flex flex-col">
        <Header currentView={currentView} />
        <main className="container flex-1 py-6 animate-fade-in">{children}</main>
        <Footer />
      </div>
    </>
  );
}
