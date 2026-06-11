import type * as React from "react";

import { cn } from "@/lib/utils";

function Textarea({ className, ...props }: React.ComponentProps<"textarea">) {
  return (
    <textarea
      data-slot="textarea"
      className={cn(
        // body voice — what the user types is a phrase, not an instrument
        // readout. Code-like fields can opt back in with font-mono.
        "font-sans text-sm text-foreground placeholder:text-muted-foreground",
        // structure: square, token-width border, no shadow
        "field-sizing-content flex min-h-20 w-full rounded-none border-(length:--border-width) border-input bg-transparent",
        "px-3 py-2",
        // focus
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

export { Textarea };
