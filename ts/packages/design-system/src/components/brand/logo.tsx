import * as React from "react";
import { cn } from "@/lib/utils";

type LogoSize = "sm" | "md" | "lg" | "xl";

const sizeClasses: Record<LogoSize, string> = {
  sm: "h-6 w-6", // 24px — inline, badges
  md: "h-8 w-8", // 32px — standard header/footer
  lg: "h-12 w-12", // 48px — hero lockup
  xl: "h-16 w-16", // 64px — large marketing moments
};

interface LogoProps extends Omit<React.HTMLAttributes<HTMLElement>, "children"> {
  /** Default (light-mode) asset. */
  src?: string;
  /**
   * Optional dark-mode asset. When provided, the component renders both
   * images and CSS toggles visibility via `.dark` — the correct approach for
   * full-color marks or any logo where `brightness-0 invert` would distort
   * color. When omitted, `invertInDark` is used as a fallback for
   * monochrome logos.
   */
  srcDark?: string;
  /** Accessible description. Required — every logo needs a name. */
  alt: string;
  /** Size variant. 24 / 32 / 48 / 64px. Default `md`. */
  size?: LogoSize;
  /**
   * Monochrome-mark fallback: when no `srcDark` is provided and the asset is
   * dark-on-light, flip it with `brightness-0 invert` in dark mode. Default
   * `true`. Ignored when `srcDark` is set.
   */
  invertInDark?: boolean;
  /** Inline SVG content, alternative to `src`. Useful for bundled React SVG components. */
  children?: React.ReactNode;
}

/**
 * Logo primitive. Standardizes sizing and dark-mode theming.
 *
 * Two dark-mode strategies:
 *
 * 1. **Paired assets (preferred for full-color marks)** — pass both `src`
 *    and `srcDark`. The component renders both and CSS shows the right one
 *    via the `.dark` class:
 *
 *    ```tsx
 *    <Logo src="/af-logo.svg" srcDark="/af-logo-dark.svg" alt="Antfly" />
 *    ```
 *
 * 2. **Monochrome invert (fallback)** — pass only `src` and let
 *    `invertInDark` (default `true`) apply `brightness-0 invert` in dark
 *    mode. Only works cleanly for pure dark-on-transparent marks.
 *
 * The library does not ship logo SVG assets — consumers provide their own
 * via `src` / `srcDark` (URL) or `children` (inline SVG). This keeps brand
 * updates decoupled from library releases.
 */
export const Logo = React.forwardRef<HTMLElement, LogoProps>(
  ({ src, srcDark, alt, size = "md", invertInDark = true, className, children, ...props }, ref) => {
    const sizing = sizeClasses[size];

    if (src && srcDark) {
      return (
        <span
          ref={ref as React.Ref<HTMLSpanElement>}
          role="img"
          aria-label={alt}
          className={cn("relative inline-block shrink-0", sizing, className)}
          {...props}
        >
          <img
            src={src}
            alt=""
            aria-hidden
            className="absolute inset-0 h-full w-full opacity-100 dark:opacity-0"
          />
          <img
            src={srcDark}
            alt=""
            aria-hidden
            className="absolute inset-0 h-full w-full opacity-0 dark:opacity-100"
          />
        </span>
      );
    }

    const classes = cn(
      "inline-block shrink-0",
      sizing,
      invertInDark && "dark:brightness-0 dark:invert",
      className
    );

    if (src) {
      return (
        <img
          ref={ref as React.Ref<HTMLImageElement>}
          src={src}
          alt={alt}
          className={classes}
          {...(props as React.ImgHTMLAttributes<HTMLImageElement>)}
        />
      );
    }

    return (
      <span
        ref={ref as React.Ref<HTMLSpanElement>}
        role="img"
        aria-label={alt}
        className={classes}
        {...props}
      >
        {children}
      </span>
    );
  }
);
Logo.displayName = "Logo";
