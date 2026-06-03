"use client";

import * as SwitchPrimitive from "@radix-ui/react-switch";
import type * as React from "react";

import { cn } from "@/lib/utils";

function Switch({ className, ...props }: React.ComponentProps<typeof SwitchPrimitive.Root>) {
  return (
    <SwitchPrimitive.Root
      data-slot="switch"
      className={cn(
        // SQUARE track — both dimensions in fixed pixels so density rescaling
        // (compact / comfortable) doesn't distort the thumb fit. Flex centers
        // the thumb vertically; px-[1px] gives the 1px inset on each side.
        "inline-flex items-center shrink-0 w-[38px] h-[20px] px-[1px]",
        "rounded-none border-[1.5px] border-input bg-transparent",
        // checked: amber fill + amber-500 border
        "data-[state=checked]:bg-primary data-[state=checked]:border-amber-500",
        // focus
        "outline-none transition-colors",
        "focus-visible:border-ring focus-visible:ring-2 focus-visible:ring-ring/30",
        "disabled:cursor-not-allowed disabled:opacity-50",
        className
      )}
      {...props}
    >
      <SwitchPrimitive.Thumb
        data-slot="switch-thumb"
        className={cn(
          // square knob, fixed 14×14, horizontal translate only
          "pointer-events-none block size-[14px] rounded-none",
          // off: muted ink. on: ink-on-amber.
          "bg-muted-foreground data-[state=checked]:bg-primary-foreground",
          "transition-transform data-[state=checked]:translate-x-[19px]"
        )}
      />
    </SwitchPrimitive.Root>
  );
}

export { Switch };
