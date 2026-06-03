import { cva, type VariantProps } from "class-variance-authority";
import { Slot } from "radix-ui";
import * as React from "react";

import { cn } from "@/lib/utils";

/**
 * Body voice: Inter (the default sans stack), used for paragraphs, lede text,
 * and descriptions. Pairs with {@link Heading} (display) and {@link MonoLabel}
 * (technical) to make the three-register hierarchy explicit instead of scattering
 * `text-lg text-muted-foreground` across every page.
 *
 *   <Text size="lg" tone="muted">Private instances, built-in inference…</Text>
 *
 * Renders a `<p>` by default; pass `asChild` to render a `<span>`/`<div>`/etc.
 */
const textVariants = cva("", {
  variants: {
    size: {
      xs: "text-xs",
      sm: "text-sm",
      base: "text-base",
      lg: "text-lg",
      xl: "text-xl",
    },
    tone: {
      default: "text-foreground",
      muted: "text-muted-foreground",
    },
    leading: {
      normal: "",
      relaxed: "leading-relaxed",
    },
  },
  defaultVariants: {
    size: "base",
    tone: "default",
    leading: "normal",
  },
});

interface TextProps
  extends React.HTMLAttributes<HTMLParagraphElement>,
    VariantProps<typeof textVariants> {
  /** Render the child element instead of a `<p>` (Radix `Slot`). */
  asChild?: boolean;
}

export const Text = React.forwardRef<HTMLParagraphElement, TextProps>(
  ({ size, tone, leading, className, asChild = false, ...props }, ref) => {
    const Comp = asChild ? Slot.Root : "p";
    return (
      <Comp
        ref={ref}
        data-slot="text"
        className={cn(textVariants({ size, tone, leading }), className)}
        {...props}
      />
    );
  }
);
Text.displayName = "Text";

export { textVariants };
