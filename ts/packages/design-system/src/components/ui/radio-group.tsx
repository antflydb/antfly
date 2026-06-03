"use client";

import * as RadioGroupPrimitive from "@radix-ui/react-radio-group";
import type * as React from "react";

import { cn } from "@/lib/utils";

function RadioGroup({
  className,
  ...props
}: React.ComponentProps<typeof RadioGroupPrimitive.Root>) {
  return (
    <RadioGroupPrimitive.Root
      data-slot="radio-group"
      className={cn("grid gap-3", className)}
      {...props}
    />
  );
}

function RadioGroupItem({
  className,
  ...props
}: React.ComponentProps<typeof RadioGroupPrimitive.Item>) {
  return (
    <RadioGroupPrimitive.Item
      data-slot="radio-group-item"
      className={cn(
        // round — radio convention. 1.5px border, no shadow.
        "aspect-square size-4 shrink-0 rounded-full border-[1.5px] border-input bg-transparent",
        "text-primary",
        // focus
        "outline-none transition-[color,box-shadow]",
        "focus-visible:border-ring focus-visible:ring-2 focus-visible:ring-ring/30",
        // invalid + disabled
        "aria-invalid:border-destructive aria-invalid:ring-destructive/20 dark:aria-invalid:ring-destructive/40",
        "disabled:cursor-not-allowed disabled:opacity-50",
        className
      )}
      {...props}
    >
      <RadioGroupPrimitive.Indicator
        data-slot="radio-group-indicator"
        className="relative flex items-center justify-center"
      >
        <span
          aria-hidden
          className="bg-primary absolute top-1/2 left-1/2 size-[6px] rounded-full -translate-x-1/2 -translate-y-1/2"
        />
      </RadioGroupPrimitive.Indicator>
    </RadioGroupPrimitive.Item>
  );
}

export { RadioGroup, RadioGroupItem };
