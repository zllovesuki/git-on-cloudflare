import type { ComponentProps, ReactNode } from "react";

type CardVariant = "default" | "accent";

type CardProps = {
  variant?: CardVariant;
  interactive?: boolean;
  children: ReactNode;
  className?: string;
} & Omit<ComponentProps<"div">, "children" | "className">;

const base = "rounded-2xl p-5 sm:p-6";

const variantStyles: Record<CardVariant, string> = {
  default: "border border-zinc-200 dark:border-zinc-800/60 bg-white dark:bg-zinc-900/50",
  accent:
    "border border-accent-500/20 bg-gradient-to-br from-white to-zinc-50 dark:from-zinc-900/80 dark:to-zinc-900/40",
};

const interactiveStyles = "hover:-translate-y-0.5 transition-transform cursor-pointer";

export function Card({
  variant = "default",
  interactive = false,
  className = "",
  children,
  ...rest
}: CardProps) {
  const classes =
    `${base} ${variantStyles[variant]} ${interactive ? interactiveStyles : ""} ${className}`.trim();

  return (
    <div className={classes} {...rest}>
      {children}
    </div>
  );
}
