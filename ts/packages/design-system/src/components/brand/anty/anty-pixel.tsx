import * as React from "react";
import { cn } from "@/lib/utils";

export type AntyPixelVariant = "square" | "diagonal";
export type AntyPixelSize = "sm" | "md" | "lg" | "xl";

/**
 * Sizes are multiples of 24px so both the 12×12 and 24×24 grids land on
 * whole-pixel cells (sharp edges, no anti-aliasing artifacts).
 */
const sizePx: Record<AntyPixelSize, number> = {
  sm: 48,
  md: 96,
  lg: 144,
  xl: 192,
};

export interface AntyPixelProps extends Omit<React.HTMLAttributes<HTMLSpanElement>, "children"> {
  /**
   * Notch geometry.
   * - `"square"` — 12×12, blocky square-cut notches at top-right and bottom-left.
   * - `"diagonal"` — 24×24, stair-stepped diagonal slot (slope −1) at top-right and
   *   bottom-left, with parallel staircases on both adjacent body edges to match
   *   the canonical mark's tail direction.
   * @default "diagonal"
   */
  variant?: AntyPixelVariant;
  /** Sprite size. Defaults to `md` (96px). */
  size?: AntyPixelSize;
  /** Render an amber glow halo behind the sprite. @default true */
  glow?: boolean;
  /** Eye blink animation. @default true */
  blink?: boolean;
  /** Accessible label. @default "Anty" */
  alt?: string;
}

/**
 * Pixel-sprite Anty — the "loud register" of the brand mascot, used in hero
 * moments, empty states, onboarding, 404s, and loading splashes. For the smooth
 * vector mascot used in the working UI, see {@link Anty}.
 *
 * Themes via `currentColor`; the parent's `color` controls the sprite's ink.
 * The glow uses the `--anty-glow-inner` token (amber).
 */
export const AntyPixel = React.forwardRef<HTMLSpanElement, AntyPixelProps>(
  (
    {
      variant = "diagonal",
      size = "md",
      glow = true,
      blink = true,
      alt = "Anty",
      className,
      style,
      ...props
    },
    ref
  ) => {
    const px = sizePx[size];
    const viewBox = variant === "square" ? "0 0 12 12" : "0 0 24 24";

    return (
      <span
        ref={ref}
        role="img"
        aria-label={alt}
        className={cn("relative inline-grid place-items-center", className)}
        style={{ width: px, height: px, ...style }}
        {...props}
      >
        {glow && (
          <span
            aria-hidden="true"
            className="anty-pixel-glow pointer-events-none absolute rounded-full"
          />
        )}
        <svg
          xmlns="http://www.w3.org/2000/svg"
          viewBox={viewBox}
          shapeRendering="crispEdges"
          width={px}
          height={px}
          aria-hidden="true"
          style={{ position: "relative", imageRendering: "pixelated" }}
        >
          {variant === "square" ? (
            <PixelSquareFrame blink={blink} />
          ) : (
            <PixelDiagonalFrame blink={blink} />
          )}
        </svg>
      </span>
    );
  }
);
AntyPixel.displayName = "AntyPixel";

/* ---------- 12×12 · square-cut notches at TR and BL ---------- */
function PixelSquareFrame({ blink }: { blink: boolean }) {
  return (
    <>
      <g fill="currentColor">
        <rect x="1" y="1" width="8" height="2" />
        <rect x="1" y="1" width="2" height="8" />
        <rect x="9" y="3" width="2" height="8" />
        <rect x="3" y="9" width="8" height="2" />
      </g>
      <g fill="currentColor" className={blink ? "anty-pixel-eye" : undefined}>
        <rect x="4" y="5" width="1" height="2" />
        <rect x="7" y="5" width="1" height="2" />
      </g>
    </>
  );
}

/* ---------- 24×24 · stair-step diagonal slot, slope −1, parallel edges ---------- */
function PixelDiagonalFrame({ blink }: { blink: boolean }) {
  return (
    <>
      <g fill="currentColor">
        {/* TR notch: top edge staircase */}
        <rect x="2" y="2" width="19" height="1" />
        <rect x="2" y="3" width="18" height="1" />
        <rect x="2" y="4" width="17" height="1" />
        <rect x="2" y="5" width="16" height="1" />
        {/* TR notch: right edge widening to full bar (parallel staircase) */}
        <rect x="21" y="6" width="1" height="1" />
        <rect x="20" y="7" width="2" height="1" />
        <rect x="19" y="8" width="3" height="1" />
        <rect x="18" y="9" width="4" height="1" />
        {/* left bar full, y=6..13 */}
        <rect x="2" y="6" width="4" height="8" />
        {/* right bar full, y=10..17 */}
        <rect x="18" y="10" width="4" height="8" />
        {/* BL notch: left edge narrowing from full bar (parallel staircase) */}
        <rect x="2" y="14" width="4" height="1" />
        <rect x="2" y="15" width="3" height="1" />
        <rect x="2" y="16" width="2" height="1" />
        <rect x="2" y="17" width="1" height="1" />
        {/* BL notch: bottom edge staircase */}
        <rect x="6" y="18" width="16" height="1" />
        <rect x="5" y="19" width="17" height="1" />
        <rect x="4" y="20" width="18" height="1" />
        <rect x="3" y="21" width="19" height="1" />
      </g>
      <g fill="currentColor" className={blink ? "anty-pixel-eye" : undefined}>
        <rect x="8" y="10" width="2" height="4" />
        <rect x="14" y="10" width="2" height="4" />
      </g>
    </>
  );
}
