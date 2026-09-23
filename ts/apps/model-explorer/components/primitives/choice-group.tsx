"use client";

import { cn } from "@antfly/design-system";
import type { ReactNode } from "react";

/** A selection control, with no tabpanel relationship implied. */
export function ChoiceGroup<Value extends string>({
  label,
  value,
  options,
  onValueChange,
  className,
  buttonClassName,
}: {
  label: string;
  value: Value;
  options: readonly { value: Value; label: ReactNode; disabled?: boolean }[];
  onValueChange: (value: Value) => void;
  className?: string;
  buttonClassName?: string;
}) {
  return (
    <div
      role="group"
      aria-label={label}
      className={cn(
        "inline-flex max-w-full flex-wrap items-center rounded-lg bg-muted p-1 text-muted-foreground",
        className
      )}
    >
      {options.map((option) => (
        <button
          key={option.value}
          type="button"
          aria-pressed={value === option.value}
          disabled={option.disabled}
          onClick={() => onValueChange(option.value)}
          className={cn(
            "inline-flex h-6 items-center justify-center whitespace-nowrap rounded-md px-2 text-xs font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 focus-visible:ring-offset-background disabled:cursor-not-allowed disabled:opacity-50",
            value === option.value
              ? "bg-background text-foreground shadow-sm"
              : "hover:text-foreground",
            buttonClassName
          )}
        >
          {option.label}
        </button>
      ))}
    </div>
  );
}
