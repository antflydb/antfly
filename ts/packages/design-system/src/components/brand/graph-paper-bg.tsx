import * as React from "react";
import { cn } from "@/lib/utils";

/**
 * Subtle hexagonal honeycomb texture behind content. Used to anchor the hero
 * section visually without adding any gradient or heavy ornament. Respects
 * dark mode automatically via the `.grid-paper` class's dark override.
 */
export const GraphPaperBg = React.forwardRef<HTMLDivElement, React.HTMLAttributes<HTMLDivElement>>(
  ({ className, ...props }, ref) => (
    <div ref={ref} className={cn("grid-paper", className)} {...props} />
  )
);
GraphPaperBg.displayName = "GraphPaperBg";
