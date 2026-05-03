import type * as React from "react";
import { cn } from "@/lib/utils";

export interface Feature {
  icon?: React.ReactNode;
  title: React.ReactNode;
  description: React.ReactNode;
}

interface FeatureGridProps extends React.HTMLAttributes<HTMLDivElement> {
  features: Feature[];
  /** Column count for `bordered` variant. Ignored for `icon-row`. Default 3. */
  columns?: 2 | 3 | 4;
  /**
   * Visual treatment:
   * - `bordered` (default, antfly.io style): bordered cards in a grid, purple accent on hover
   * - `icon-row` (searchaf style): icon + title + description laid out as a flex row
   */
  variant?: "bordered" | "icon-row";
}

const columnClasses = {
  2: "md:grid-cols-2",
  3: "md:grid-cols-2 lg:grid-cols-3",
  4: "md:grid-cols-2 lg:grid-cols-4",
} as const;

/**
 * Feature grid in the PR #184 aesthetic — restrained, monochromatic, no
 * glassmorphism or tinted icon chips.
 *
 *   `bordered`   Simple bordered cards, hover raises the border to primary/30.
 *                The antfly.io "Capabilities" grid.
 *
 *   `icon-row`   Flex rows with a 12×12 primary/10 icon square on the left and
 *                title + description on the right. The searchaf.com "What it
 *                does" section.
 */
export function FeatureGrid({
  features,
  columns = 3,
  variant = "bordered",
  className,
  ...props
}: FeatureGridProps) {
  if (variant === "icon-row") {
    return (
      <div className={cn("grid grid-cols-1 md:grid-cols-2 gap-10 md:gap-12", className)} {...props}>
        {features.map((feature) => (
          <IconRow key={String(feature.title)} {...feature} />
        ))}
      </div>
    );
  }

  return (
    <div className={cn("grid grid-cols-1 gap-6", columnClasses[columns], className)} {...props}>
      {features.map((feature) => (
        <BorderedCard key={String(feature.title)} {...feature} />
      ))}
    </div>
  );
}

function BorderedCard({ title, description }: Feature) {
  return (
    <div className="rounded-lg border border-border p-6 transition-colors hover:border-primary/30">
      <h3 className="mb-2 text-lg font-bold">{title}</h3>
      <p className="text-sm text-muted-foreground leading-relaxed">{description}</p>
    </div>
  );
}

function IconRow({ icon, title, description }: Feature) {
  return (
    <div className="flex gap-5">
      {icon ? (
        <div className="flex h-12 w-12 flex-shrink-0 items-center justify-center rounded-lg bg-primary/10 text-primary">
          {icon}
        </div>
      ) : null}
      <div>
        <h3 className="mb-2 font-display text-xl font-bold">{title}</h3>
        <p className="text-muted-foreground leading-relaxed">{description}</p>
      </div>
    </div>
  );
}
