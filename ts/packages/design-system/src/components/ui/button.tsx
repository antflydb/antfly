import { cva, type VariantProps } from "class-variance-authority";
import { Slot } from "radix-ui";
import * as React from "react";

import { cn } from "@/lib/utils";

/**
 * Buttons in the Antfly design language:
 *   - Mono "instrument" voice (Roboto Mono, 13px, weight 500, 0.03em tracking)
 *   - Square corners (rounded-none)
 *   - 1.5px borders on outlined variants — the visual language is borders, not shadows
 *   - Amber primary fill with ink text; amber-300 hover
 *   - Snappy/linear motion (no spring easing)
 */
const buttonVariants = cva(
  cn(
    // mono "instrument readout" voice
    "font-mono text-[13px] font-medium",
    // structure
    "inline-flex shrink-0 items-center justify-center gap-2 rounded-none whitespace-nowrap",
    // motion (linear, no spring)
    "transition-colors",
    // focus + invalid + disabled
    "outline-none focus-visible:ring-2 focus-visible:ring-ring/30 focus-visible:ring-offset-1 focus-visible:ring-offset-background",
    "aria-invalid:border-destructive aria-invalid:ring-destructive/20 dark:aria-invalid:ring-destructive/40",
    "disabled:pointer-events-none disabled:opacity-50",
    "[&_svg]:pointer-events-none [&_svg]:shrink-0 [&_svg:not([class*='size-'])]:size-4"
  ),
  {
    variants: {
      variant: {
        // Primary — amber fill, ink text, darker amber border, lighter amber hover
        default:
          "bg-primary text-primary-foreground border-[1.5px] border-amber-500 hover:bg-amber-300",
        // Destructive — outlined red that fills on hover
        destructive:
          "bg-transparent text-destructive border-[1.5px] border-destructive hover:bg-destructive hover:text-white",
        // Outline / Secondary — bordered, ink text, border darkens on hover
        outline:
          "bg-transparent text-foreground border-[1.5px] border-input hover:border-foreground hover:bg-secondary",
        secondary:
          "bg-transparent text-foreground border-[1.5px] border-input hover:border-foreground hover:bg-secondary",
        // Ghost — no border, muted text, lights up on hover
        ghost:
          "bg-transparent text-muted-foreground border-[1.5px] border-transparent hover:text-foreground hover:bg-secondary",
        // Link — text only, underline on hover (no border)
        link: "text-primary underline-offset-4 hover:underline border-[1.5px] border-transparent",
      },
      size: {
        default: "h-9 px-4 has-[>svg]:px-3",
        xs: "h-6 gap-1 px-2 text-[11px] has-[>svg]:px-1.5 [&_svg:not([class*='size-'])]:size-3",
        sm: "h-8 gap-1.5 px-3 has-[>svg]:px-2.5",
        lg: "h-10 px-6 has-[>svg]:px-4",
        icon: "size-9",
        "icon-xs": "size-6 [&_svg:not([class*='size-'])]:size-3",
        "icon-sm": "size-8",
        "icon-lg": "size-10",
      },
    },
    defaultVariants: {
      variant: "default",
      size: "default",
    },
  }
);

function Button({
  className,
  variant = "default",
  size = "default",
  asChild = false,
  ...props
}: React.ComponentProps<"button"> &
  VariantProps<typeof buttonVariants> & {
    asChild?: boolean;
  }) {
  const Comp = asChild ? Slot.Root : "button";

  return (
    <Comp
      data-slot="button"
      data-variant={variant}
      data-size={size}
      className={cn(buttonVariants({ variant, size, className }))}
      {...props}
    />
  );
}

export { Button, buttonVariants };
