import * as React from "react";
import { cn } from "@/lib/utils";

/**
 * The technical voice of the design system: small-caps Roboto Mono used for
 * section eyebrows, table labels, and code annotations. Pair with an Aeonik
 * headline in Hero/CTA to create the two-voice typographic hierarchy.
 *
 * Renders as a `<span>`. Apply `.block` / `.mb-6` on the className to own
 * spacing — the utility itself is display-agnostic.
 */
export const MonoLabel = React.forwardRef<HTMLSpanElement, React.HTMLAttributes<HTMLSpanElement>>(
  ({ className, ...props }, ref) => (
    <span ref={ref} className={cn("mono-label", className)} {...props} />
  )
);
MonoLabel.displayName = "MonoLabel";
