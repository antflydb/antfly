import type * as React from "react";

import { cn } from "@/lib/utils";

function Input({ className, type, ...props }: React.ComponentProps<"input">) {
  return (
    <input
      type={type}
      data-slot="input"
      className={cn(
        // mono "instrument readout" voice
        "font-mono text-[13px] text-foreground placeholder:text-muted-foreground",
        // structure: square, deliberate 1.5px border, no shadow
        "h-9 w-full min-w-0 rounded-none border-[1.5px] border-input bg-transparent",
        "px-3 py-1.5",
        // selection / file input
        "selection:bg-primary selection:text-primary-foreground",
        "file:text-foreground file:inline-flex file:h-7 file:border-0 file:bg-transparent file:text-sm file:font-medium",
        // focus: border + soft amber/ink ring (per --ring token)
        "outline-none transition-[border-color,box-shadow]",
        "focus-visible:border-ring focus-visible:ring-2 focus-visible:ring-ring/30",
        // invalid + disabled
        "aria-invalid:border-destructive aria-invalid:ring-destructive/20 dark:aria-invalid:ring-destructive/40",
        "disabled:pointer-events-none disabled:cursor-not-allowed disabled:opacity-50",
        className
      )}
      {...props}
    />
  );
}

export { Input };
