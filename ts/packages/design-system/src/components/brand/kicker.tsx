import * as React from "react";
import { cn } from "@/lib/utils";

/**
 * The pixel-font emphasis voice — Silkscreen, amber, restrained.
 *
 * Use sparingly: brand-moment overlines (above an Aeonik headline in Hero/CTA),
 * loud tags (404, version pills in marketing), splash captions. NOT a working-UI
 * label — for that, see {@link MonoLabel}, the technical/data voice.
 *
 * Renders as a `<span>`; pair with `block`/`mb-*` utilities for spacing.
 */
export const Kicker = React.forwardRef<HTMLSpanElement, React.HTMLAttributes<HTMLSpanElement>>(
  ({ className, ...props }, ref) => (
    <span ref={ref} className={cn("kicker", className)} {...props} />
  )
);
Kicker.displayName = "Kicker";
