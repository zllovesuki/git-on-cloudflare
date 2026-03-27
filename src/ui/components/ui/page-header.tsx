import type { ReactNode } from "react";

type PageHeaderProps = {
  children: ReactNode;
  className?: string;
};

export function PageHeader({ children, className = "" }: PageHeaderProps) {
  return (
    <div
      className={`mb-6 flex items-center justify-between max-sm:flex-col max-sm:items-stretch max-sm:gap-3 ${className}`.trim()}
    >
      {children}
    </div>
  );
}
