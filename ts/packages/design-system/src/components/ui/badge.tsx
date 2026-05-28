import { Slot } from "@radix-ui/react-slot";
import { cva, type VariantProps } from "class-variance-authority";
import type * as React from "react";

import { cn } from "@/lib/utils";

const badgeVariants = cva(
  [
    // mono "instrument label" voice — uppercase, tracked, 11px, 1.5px square border
    "inline-flex items-center justify-center font-mono uppercase tracking-[0.05em] text-[11px] font-medium",
    "px-2 py-[3px] border-[1.5px] rounded-none w-fit whitespace-nowrap shrink-0",
    "[&>svg]:size-3 [&>svg]:pointer-events-none gap-1",
    "transition-colors overflow-hidden",
    "focus-visible:outline-none focus-visible:border-ring focus-visible:ring-2 focus-visible:ring-ring/30",
    "aria-invalid:border-destructive aria-invalid:ring-destructive/20 dark:aria-invalid:ring-destructive/40",
  ].join(" "),
  {
    variants: {
      variant: {
        // solid amber fill — the loud variant (shadcn convention for default)
        default:
          "bg-primary text-primary-foreground border-amber-500 [a&]:hover:bg-amber-300",
        // subtle muted fill — soft pill, no visible border
        secondary:
          "bg-secondary text-secondary-foreground border-transparent [a&]:hover:bg-secondary/80",
        // semantic destructive — outline only
        destructive:
          "bg-transparent border-destructive text-destructive [a&]:hover:bg-destructive/10",
        // prototype `.badge` — the workhorse: strong-line border, muted ink
        outline:
          "bg-transparent border-border-strong text-muted-foreground [a&]:hover:border-foreground [a&]:hover:text-foreground",
        // amber outline — accent without filling
        amber:
          "bg-transparent border-amber-500 text-amber-600 dark:text-amber-400 [a&]:hover:bg-amber-500/10",
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
