import * as React from "react";
import { cn } from "@/lib/utils";

interface LockupProps extends React.HTMLAttributes<HTMLSpanElement> {
  /**
   * Standardized gap between Logo and Wordmark. Default `gap-2.5` matches the
   * marketing-site convention. Override with `className="gap-*"`.
   */
}

/**
 * Lockup primitive — a flex container coordinating Logo + Wordmark with the
 * standard 10px gap between mark and text.
 *
 * Intended usage:
 *
 * ```tsx
 * <Lockup>
 *   <Logo src="/af-logo.svg" srcDark="/af-logo-dark.svg" alt="Antfly" />
 *   <Wordmark className="text-lg">Antfly</Wordmark>
 * </Lockup>
 * ```
 *
 * Compose with `<Link>` / `<a>` when the lockup is a home-link:
 *
 * ```tsx
 * <Link href="/" className="contents">
 *   <Lockup>…</Lockup>
 * </Link>
 * ```
 */
export const Lockup = React.forwardRef<HTMLSpanElement, LockupProps>(
  ({ className, children, ...props }, ref) => {
    return (
      <span ref={ref} className={cn("inline-flex items-center gap-2.5", className)} {...props}>
        {children}
      </span>
    );
  }
);
Lockup.displayName = "Lockup";
