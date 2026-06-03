import type * as React from "react";
import { cn } from "@/lib/utils";
import { GraphPaperBg } from "./graph-paper-bg";
import { Kicker } from "./kicker";
import { MonoLabel } from "./mono-label";
import { TypeOn } from "./type-on";

interface HeroProps extends Omit<React.HTMLAttributes<HTMLElement>, "title"> {
  /**
   * Pixel-font overline for loud brand moments — rendered with {@link Kicker}.
   * Use for marketing/hero/onboarding voice. Preferred over `eyebrow` here.
   */
  kicker?: React.ReactNode;
  /**
   * Small monospace eyebrow (technical voice) — rendered with {@link MonoLabel}.
   * Kept for back-compat; prefer `kicker` for new hero treatments.
   */
  eyebrow?: React.ReactNode;
  /**
   * Headline. Wrap an accent word with {@link Highlight} for the amber-fill
   * marker, e.g. `<>Search the <Highlight>swarm</Highlight></>`. Typeset in
   * Aeonik bold at display sizes.
   */
  title: React.ReactNode;
  /**
   * Optional `steps()` typewriter line beneath the headline. Pass a string and
   * it renders with {@link TypeOn}.
   */
  tagline?: string;
  description?: React.ReactNode;
  /** CTAs — typically one primary and one outline button. */
  actions?: React.ReactNode;
  /**
   * Right-side adornment (e.g. `<AntyPixel size="xl" />`). When provided,
   * the hero lays out as a two-column grid on `md+`.
   */
  aside?: React.ReactNode;
  /** Wrap the hero in a subtle hexagonal graph-paper background. @default true */
  graphPaper?: boolean;
  /** Center the content horizontally. @default "start" */
  align?: "start" | "center";
}

/**
 * The Antfly hero treatment:
 *   - Pixel-font {@link Kicker} (loud) or mono {@link MonoLabel} (technical) overline
 *   - Aeonik headline (5xl → 8xl) with optional amber {@link Highlight}
 *   - Optional {@link TypeOn} typewriter tagline
 *   - Restrained description in muted-foreground
 *   - Flat square buttons (the visual language is borders + amber accent, not gradients)
 *   - Optional hexagonal graph-paper background
 *   - Optional `aside` slot (right column) for an `<AntyPixel>` or other brand asset
 *
 * Deliberately sparse — the whitespace and the amber accent do the work.
 */
export function Hero({
  kicker,
  eyebrow,
  title,
  tagline,
  description,
  actions,
  aside,
  graphPaper = true,
  align = "start",
  className,
  ...props
}: HeroProps) {
  const content = (
    <>
      {kicker ? <Kicker className="mb-3 block">{kicker}</Kicker> : null}
      {eyebrow ? <MonoLabel className="mb-6 block">{eyebrow}</MonoLabel> : null}
      <h1
        className={cn(
          "font-display font-bold tracking-tight text-5xl md:text-7xl lg:text-8xl max-w-4xl",
          align === "center" && "mx-auto"
        )}
      >
        {title}
      </h1>
      {tagline ? <TypeOn className="mt-6 block" text={tagline} /> : null}
      {description ? (
        <p
          className={cn(
            "mt-8 text-lg md:text-xl text-muted-foreground max-w-2xl leading-relaxed",
            align === "center" && "mx-auto"
          )}
        >
          {description}
        </p>
      ) : null}
      {actions ? (
        <div className={cn("mt-10 flex flex-wrap gap-4", align === "center" && "justify-center")}>
          {actions}
        </div>
      ) : null}
    </>
  );

  const inner = (
    <section
      className={cn("container py-24 md:py-32", align === "center" && "text-center", className)}
      {...props}
    >
      {aside ? (
        <div className="grid items-center gap-12 md:grid-cols-[1fr_auto] md:gap-16">
          <div>{content}</div>
          <div className="justify-self-center md:justify-self-end">{aside}</div>
        </div>
      ) : (
        content
      )}
    </section>
  );

  if (graphPaper) return <GraphPaperBg>{inner}</GraphPaperBg>;
  return inner;
}
