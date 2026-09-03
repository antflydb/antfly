import { Slot } from "@radix-ui/react-slot";
import { cva, type VariantProps } from "class-variance-authority";
import type * as React from "react";

import { cn } from "@/lib/utils";

const badgeVariants = cva(
  [
    // mono "instrument label" voice — uppercase, tracked, 11px, token-width square border
    "inline-flex items-center justify-center font-mono uppercase tracking-[0.08em] text-[11px] font-medium",
    "px-2 py-[3px] border-(length:--border-width) rounded-none w-fit whitespace-nowrap shrink-0",
    "[&>svg]:size-3 [&>svg]:pointer-events-none gap-1",
    "transition-colors overflow-hidden",
    "focus-visible:outline-none focus-visible:border-ring focus-visible:ring-2 focus-visible:ring-ring/30",
    "aria-invalid:border-destructive aria-invalid:ring-destructive/20 dark:aria-invalid:ring-destructive/40",
  ].join(" "),
  {
    variants: {
      variant: {
        // quiet outline — the workhorse default: hairline border, muted ink
        default: "bg-transparent border-border text-muted-foreground [a&]:hover:text-foreground",
        // solid amber fill — deliberate brand moments only
        brand: "bg-primary text-primary-foreground border-transparent [a&]:hover:bg-amber-300",
        // semantic destructive — red ink, quiet chassis
        destructive: "bg-transparent border-border text-destructive [a&]:hover:bg-destructive/10",
      },
    },
    defaultVariants: {
      variant: "default",
    },
  }
);

function Badge({
  className,
  variant,
  asChild = false,
  ...props
}: React.ComponentProps<"span"> & VariantProps<typeof badgeVariants> & { asChild?: boolean }) {
  const Comp = asChild ? Slot : "span";

  return (
    <Comp data-slot="badge" className={cn(badgeVariants({ variant }), className)} {...props} />
  );
}

export { Badge, badgeVariants };
