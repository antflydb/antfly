import type * as React from "react";
import { cn } from "@/lib/utils";
import { GraphPaperBg } from "./graph-paper-bg";
import { MonoLabel } from "./mono-label";

interface HeroProps extends Omit<React.HTMLAttributes<HTMLElement>, "title"> {
  /** Small monospace eyebrow above the headline — e.g. "The AI-native database". */
  eyebrow?: React.ReactNode;
  /**
   * Headline. Pass ReactNode to accent a word with `<span className="text-primary">`.
   * Typeset in Aeonik bold at display sizes (up to 8xl).
   */
  title: React.ReactNode;
  description?: React.ReactNode;
  /** CTAs — typically one primary and one outline button. */
  actions?: React.ReactNode;
  /** Wrap the hero in a subtle hexagonal graph-paper background. Default: true. */
  graphPaper?: boolean;
  /** Center the content horizontally. Default left-aligned (PR #184 antfly.io style). */
  align?: "start" | "center";
}

/**
 * The Antfly hero treatment from PR #184:
 *   - Small MonoLabel eyebrow
 *   - Aeonik headline (5xl → 8xl responsive) with tight tracking
 *   - Restrained description in muted-foreground
 *   - Plain rounded-md buttons (no gradients, no pills)
 *   - Optional hexagonal graph-paper background (default on)
 *
 * Deliberately sparse — the whitespace does the work. No animated flourishes,
 * no gradient text, no glassmorphism. Pair with `<span className="text-primary">`
 * inside `title` to accent a single word/phrase.
 */
export function Hero({
  eyebrow,
  title,
  description,
  actions,
  graphPaper = true,
  align = "start",
  className,
  ...props
}: HeroProps) {
  const inner = (
    <section
      className={cn("container py-24 md:py-32", align === "center" && "text-center", className)}
      {...props}
    >
      {eyebrow ? <MonoLabel className="mb-6 block">{eyebrow}</MonoLabel> : null}
      <h1
        className={cn(
          "font-display font-bold tracking-tight text-5xl md:text-7xl lg:text-8xl max-w-4xl",
          align === "center" && "mx-auto"
        )}
      >
        {title}
      </h1>
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
    </section>
  );

  if (graphPaper) return <GraphPaperBg>{inner}</GraphPaperBg>;
  return inner;
}
