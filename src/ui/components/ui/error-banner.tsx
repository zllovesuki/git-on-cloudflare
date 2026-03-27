import type { ReactNode } from "react";

type ErrorBannerProps = {
  children: ReactNode;
  className?: string;
};

export function ErrorBanner({ children, className = "" }: ErrorBannerProps) {
  return (
    <div
      className={`rounded-xl border border-red-500/20 bg-red-500/10 p-4 text-sm text-red-700 dark:text-red-400 ${className}`.trim()}
    >
      {children}
    </div>
  );
}
