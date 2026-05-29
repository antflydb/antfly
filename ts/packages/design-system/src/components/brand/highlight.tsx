import * as React from "react";
import { cn } from "@/lib/utils";

/**
 * Inline amber-fill marker for accenting a word inside a headline.
 *
 *   <h1>Search the <Highlight>swarm</Highlight></h1>
 *
 * This is the canonical way to deploy the brand color inside type: amber as a
 * FILL with ink-on-amber text. Amber-as-text on paper fails contrast and is
 * not how the design language uses the hue.
 *
 * Renders as a `<span>`; safe inside any heading or block-level text element.
 */
export const Highlight = React.forwardRef<HTMLSpanElement, React.HTMLAttributes<HTMLSpanElement>>(
  ({ className, ...props }, ref) => (
    <span ref={ref} className={cn("highlight", className)} {...props} />
  )
);
Highlight.displayName = "Highlight";
