import { cva, type VariantProps } from "class-variance-authority";
import { Slot } from "radix-ui";
import * as React from "react";

import { cn } from "@/lib/utils";

/**
 * The display voice of the design system: Aeonik (font-display), bold, tightly
 * tracked. Use {@link Heading} for every page/section title so consumers stop
 * hand-rolling `font-display text-5xl font-bold tracking-tight` permutations.
 *
 * `level` (1–6) chooses the semantic element AND the default visual size; pass
 * `size` to decouple the two (e.g. a visually large `<h2>`). Wrap an accent word
 * in {@link Highlight} for the amber-fill marker.
 *
 *   <Heading level={1}>Pricing that <Highlight>rewards growth</Highlight></Heading>
 *   <Heading level={2} size="lg">Pick your path</Heading>
 *
 * The `display` size matches the {@link Hero} headline scale (5xl → 8xl).
 */
const headingVariants = cva("font-display font-bold tracking-tight text-foreground", {
  variants: {
    size: {
      display: "text-5xl md:text-7xl lg:text-8xl",
      xl: "text-4xl md:text-5xl",
      lg: "text-3xl md:text-4xl",
      md: "text-2xl",
      sm: "text-lg",
      xs: "text-base",
    },
  },
  defaultVariants: {
    size: "lg",
  },
});

type HeadingLevel = 1 | 2 | 3 | 4 | 5 | 6;

const LEVEL_SIZE: Record<
  HeadingLevel,
  NonNullable<VariantProps<typeof headingVariants>["size"]>
> = {
  1: "xl",
  2: "lg",
  3: "md",
  4: "sm",
  5: "xs",
  6: "xs",
};

interface HeadingProps
  extends React.HTMLAttributes<HTMLHeadingElement>,
    Omit<VariantProps<typeof headingVariants>, "size"> {
  /** Semantic heading element to render (`h1`–`h6`). @default 2 */
  level?: HeadingLevel;
  /**
   * Visual size, independent of `level`. Defaults to the size that matches
   * `level`. Use `display` for hero-scale titles.
   */
  size?: VariantProps<typeof headingVariants>["size"];
  /** Render the child element instead of an `h*` tag (Radix `Slot`). */
  asChild?: boolean;
}

export const Heading = React.forwardRef<HTMLHeadingElement, HeadingProps>(
  ({ level = 2, size, className, asChild = false, ...props }, ref) => {
    const Comp = asChild ? Slot.Root : (`h${level}` as const);
    return (
      <Comp
        ref={ref}
        data-slot="heading"
        className={cn(headingVariants({ size: size ?? LEVEL_SIZE[level] }), className)}
        {...props}
      />
    );
  }
);
Heading.displayName = "Heading";

export { headingVariants };
