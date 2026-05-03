import type * as React from "react";
import { cn } from "@/lib/utils";
import { MonoLabel } from "./mono-label";

interface CTAProps extends Omit<React.HTMLAttributes<HTMLElement>, "title"> {
  /** Small monospace eyebrow above the headline. */
  eyebrow?: React.ReactNode;
  title: React.ReactNode;
  description?: React.ReactNode;
  /** CTAs — typically one primary button. */
  actions?: React.ReactNode;
  /** Horizontal alignment. Default `center` matches the PR #184 Cloud/Waitlist CTAs. */
  align?: "start" | "center";
  /** Add a `border-t border-border` divider above the section. Default false. */
  dividerTop?: boolean;
}

/**
 * Centered (by default) CTA section matching PR #184. Container padding,
 * MonoLabel eyebrow, Aeonik headline, description, and one or two plain
 * buttons. No snake-border, no gradient — the hierarchy is typographic.
 *
 * Use `dividerTop` to reproduce the searchaf waitlist's top-border rule.
 */
export function CTA({
  eyebrow,
  title,
  description,
  actions,
  align = "center",
  dividerTop = false,
  className,
  ...props
}: CTAProps) {
  return (
    <section
      className={cn(
        "container py-20 md:py-28",
        align === "center" && "text-center",
        dividerTop && "border-t border-border",
        className
      )}
      {...props}
    >
      {eyebrow ? <MonoLabel className="mb-4 block">{eyebrow}</MonoLabel> : null}
      <h2 className="text-3xl md:text-4xl font-bold tracking-tight">{title}</h2>
      {description ? (
        <p
          className={cn(
            "mt-4 text-lg text-muted-foreground",
            align === "center" ? "max-w-xl mx-auto" : "max-w-2xl"
          )}
        >
          {description}
        </p>
      ) : null}
      {actions ? (
        <div className={cn("mt-8 flex flex-wrap gap-4", align === "center" && "justify-center")}>
          {actions}
        </div>
      ) : null}
    </section>
  );
}
