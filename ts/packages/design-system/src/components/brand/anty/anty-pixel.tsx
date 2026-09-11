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
   * Pixel resolution of the mark.
   * - `"diagonal"` — 24×24, the high-fidelity Anty silhouette: rounded top-left and
   *   bottom-right corners with thin slope−1 ("/") slits at the top-right and
   *   bottom-left, splitting the ring into two interlocking brackets.
   * - `"square"` — 12×12, a simplified low-res mark: a square ring with a single
   *   diagonal corner notch at the top-left and a mirrored one at the bottom-right
   *   (the slits don't read cleanly at this resolution).
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

/* ----- 12×12 · simplified for the low-res grid: a square ring with a single
 * diagonal corner notch at the top-left and a mirrored one at the bottom-right.
 * (The diagonal variant's TR/BL slits don't read cleanly at this resolution.) ----- */
function PixelSquareFrame({ blink }: { blink: boolean }) {
  return (
    <>
      <g fill="currentColor">
        {/* top + bottom edges, notched at the TL and BR corners */}
        <rect x="3" y="1" width="8" height="1" />
        <rect x="2" y="2" width="9" height="1" />
        {/* left + right bars */}
        <rect x="1" y="3" width="2" height="6" />
        <rect x="9" y="3" width="2" height="6" />
        <rect x="1" y="9" width="9" height="1" />
        <rect x="1" y="10" width="8" height="1" />
      </g>
      <g fill="currentColor" className={blink ? "anty-pixel-eye" : undefined}>
        <rect x="4" y="5" width="1" height="2" />
        <rect x="7" y="5" width="1" height="2" />
      </g>
    </>
  );
}

/* ----- 24×24 · two interlocking brackets: rounded TL/BR corners, /-slits at TR/BL -----
 *
 * Faithful to the canonical Anty mark: a ring split into a top-left bracket
 * (top + left bars, joined by a big rounded top-left corner) and a bottom-right
 * bracket (bottom + right bars, rounded bottom-right corner). The two brackets
 * are divided by thin slope−1 ("/") slits running inward from the top-right and
 * bottom-left corners. Rects are emitted as per-row horizontal runs.
 */
function PixelDiagonalFrame({ blink }: { blink: boolean }) {
  return (
    <>
      <g fill="currentColor">
        {/* top-left bracket — top bar + left bar, rounded top-left corner.
            Top/left bars stop 2px short of the TR/BL slits (2px-wide slits). */}
        <rect x="6" y="2" width="14" height="1" />
        <rect x="4" y="3" width="15" height="1" />
        <rect x="3" y="4" width="15" height="1" />
        <rect x="3" y="5" width="14" height="1" />
        <rect x="2" y="6" width="5" height="1" />
        <rect x="2" y="7" width="4" height="1" />
        <rect x="2" y="8" width="4" height="1" />
        <rect x="2" y="9" width="4" height="1" />
        <rect x="2" y="10" width="4" height="1" />
        <rect x="2" y="11" width="4" height="1" />
        <rect x="2" y="12" width="4" height="1" />
        <rect x="2" y="13" width="4" height="1" />
        <rect x="2" y="14" width="4" height="1" />
        <rect x="2" y="15" width="4" height="1" />
        <rect x="2" y="16" width="4" height="1" />
        <rect x="2" y="17" width="3" height="1" />
        <rect x="2" y="18" width="2" height="1" />
        <rect x="2" y="19" width="1" height="1" />
        {/* bottom-right bracket — exact 180° point-reflection of the top-left
            bracket above (each rect mirrored through the center (11.5, 11.5)),
            so the rounded BR corner reflects the rounded TL corner. */}
        <rect x="21" y="4" width="1" height="1" />
        <rect x="20" y="5" width="2" height="1" />
        <rect x="19" y="6" width="3" height="1" />
        <rect x="18" y="7" width="4" height="1" />
        <rect x="18" y="8" width="4" height="1" />
        <rect x="18" y="9" width="4" height="1" />
        <rect x="18" y="10" width="4" height="1" />
        <rect x="18" y="11" width="4" height="1" />
        <rect x="18" y="12" width="4" height="1" />
        <rect x="18" y="13" width="4" height="1" />
        <rect x="18" y="14" width="4" height="1" />
        <rect x="18" y="15" width="4" height="1" />
        <rect x="18" y="16" width="4" height="1" />
        <rect x="17" y="17" width="5" height="1" />
        <rect x="7" y="18" width="14" height="1" />
        <rect x="6" y="19" width="15" height="1" />
        <rect x="5" y="20" width="15" height="1" />
        <rect x="4" y="21" width="14" height="1" />
      </g>
      {/* Eyes: triangles pointing inward (apex toward center), echoing the
          logo's converging arrowheads — see Anty's OFF_LEFT/OFF_RIGHT shapes. */}
      <g fill="currentColor" className={blink ? "anty-pixel-eye" : undefined}>
        {/* left eye — base at x8, apex right at x10 */}
        <rect x="8" y="9" width="1" height="1" />
        <rect x="8" y="10" width="2" height="1" />
        <rect x="8" y="11" width="3" height="1" />
        <rect x="8" y="12" width="2" height="1" />
        <rect x="8" y="13" width="1" height="1" />
        {/* right eye — base at x15, apex left at x13 (mirror of left) */}
        <rect x="15" y="9" width="1" height="1" />
        <rect x="14" y="10" width="2" height="1" />
        <rect x="13" y="11" width="3" height="1" />
        <rect x="14" y="12" width="2" height="1" />
        <rect x="15" y="13" width="1" height="1" />
      </g>
    </>
  );
}
