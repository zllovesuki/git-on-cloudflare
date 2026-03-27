import type { ComponentProps } from "react";

type InputProps = {
  label?: string;
  helperText?: string;
  error?: string;
} & Omit<ComponentProps<"input">, "className">;

const inputClasses =
  "w-full rounded-xl border border-zinc-300 dark:border-zinc-700/60 bg-white dark:bg-zinc-800/80 px-4 py-2.5 text-zinc-900 dark:text-zinc-100 placeholder:text-zinc-500 focus:border-accent-500/50 focus:ring-1 focus:ring-accent-500/30 focus:outline-none focus-visible:ring-2 focus-visible:ring-accent-500/50 focus-visible:ring-offset-2 focus-visible:ring-offset-white dark:focus-visible:ring-offset-zinc-950";

const errorInputClasses =
  "w-full rounded-xl border border-red-500/40 bg-white dark:bg-zinc-800/80 px-4 py-2.5 text-zinc-900 dark:text-zinc-100 placeholder:text-zinc-500 focus:border-red-500/50 focus:ring-1 focus:ring-red-500/30 focus:outline-none focus-visible:ring-2 focus-visible:ring-red-500/50 focus-visible:ring-offset-2 focus-visible:ring-offset-white dark:focus-visible:ring-offset-zinc-950";

export function Input({ label, helperText, error, id, ...rest }: InputProps) {
  const inputId = id || (label ? label.toLowerCase().replace(/\s+/g, "-") : undefined);
  const errorId = error && inputId ? `${inputId}-error` : undefined;

  return (
    <div>
      {label ? (
        <label
          htmlFor={inputId}
          className="mb-1.5 block text-sm font-medium text-zinc-700 dark:text-zinc-300"
        >
          {label}
        </label>
      ) : null}
      <input
        id={inputId}
        className={error ? errorInputClasses : inputClasses}
        aria-describedby={errorId}
        aria-invalid={error ? true : undefined}
        {...rest}
      />
      {error ? (
        <p id={errorId} className="mt-1.5 text-sm text-red-600 dark:text-red-400">
          {error}
        </p>
      ) : helperText ? (
        <p className="mt-1.5 text-sm text-zinc-500">{helperText}</p>
      ) : null}
    </div>
  );
}
