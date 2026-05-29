import * as React from "react";
import { cn } from "@/lib/utils";

interface WordmarkProps extends React.HTMLAttributes<HTMLSpanElement> {
  /** Weight applied to the wordmark. Defaults to `bold` — which is correct
   * for every Antfly-family brand ("Antfly", "SearchAF", "Antfly Inference").
   * Only override if a non-Antfly consumer needs a lighter identity.
   */
  weight?: "regular" | "bold";
}

/**
 * Wordmark primitive. Aeonik (font-display) text rendered as the brand's
 * type-only identity. Always bold by default.
 *
 * Size inherits from the parent — compose with Tailwind text utilities:
 *
 * ```tsx
 * <Wordmark className="text-lg">SearchAF</Wordmark>
 * <Wordmark className="text-2xl">Antfly</Wordmark>
 * ```
 *
 * The component is intentionally simple: it exists so every brand lockup
 * reaches for the same Aeonik-bold treatment and consumers aren't tempted
 * to invent their own variants.
 */
export const Wordmark = React.forwardRef<HTMLSpanElement, WordmarkProps>(
  ({ weight = "bold", className, children, ...props }, ref) => {
    return (
      <span
        ref={ref}
        className={cn(
          "font-display tracking-tight",
          weight === "bold" ? "font-bold" : "font-normal",
          className
        )}
        {...props}
      >
        {children}
      </span>
    );
  }
);
Wordmark.displayName = "Wordmark";
