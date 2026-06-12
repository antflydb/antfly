import { cva, type VariantProps } from "class-variance-authority";
import { Slot } from "radix-ui";
import * as React from "react";

import { cn } from "@/lib/utils";

/**
 * Inline text link — the prose/navigation counterpart to {@link Button} (which is
 * the mono "instrument" voice for chrome). Use this for links inside body copy,
 * footers, nav rows, and breadcrumbs so hover/focus treatment is consistent.
 *
 *   <Link href="/docs">Read the docs</Link>
 *   <Link asChild variant="nav"><NextLink href="/pricing">Pricing</NextLink></Link>
 *
 * Pass `asChild` to wrap a framework link (e.g. Next.js `<Link>`) while keeping
 * the design-system styling. Square focus ring; the visual language is borders +
 * amber accent, never gradients.
 */
const linkVariants = cva(
  cn(
    "underline-offset-4 transition-colors rounded-none",
    "outline-none focus-visible:ring-2 focus-visible:ring-ring/30 focus-visible:ring-offset-1 focus-visible:ring-offset-background"
  ),
  {
    variants: {
      variant: {
        // Body links — ink text, amber + underline on hover
        default: "text-foreground hover:text-primary hover:underline",
        // Quieter links (footers, metadata) — muted until hovered
        muted: "text-muted-foreground hover:text-foreground",
        // Nav items — muted, lights up to ink (no underline)
        nav: "text-muted-foreground hover:text-foreground",
        // Accent links — amber text, underline on hover (use sparingly)
        accent: "text-primary hover:underline",
      },
    },
    defaultVariants: {
      variant: "default",
    },
  }
);

interface LinkProps
  extends React.AnchorHTMLAttributes<HTMLAnchorElement>,
    VariantProps<typeof linkVariants> {
  /** Render the child element instead of an `<a>` (Radix `Slot`) — e.g. Next.js `<Link>`. */
  asChild?: boolean;
}

export const Link = React.forwardRef<HTMLAnchorElement, LinkProps>(
  ({ variant, className, asChild = false, ...props }, ref) => {
    const Comp = asChild ? Slot.Root : "a";
    return (
      <Comp
        ref={ref}
        data-slot="link"
        className={cn(linkVariants({ variant }), className)}
        {...props}
      />
    );
  }
);
Link.displayName = "Link";

export { linkVariants };
