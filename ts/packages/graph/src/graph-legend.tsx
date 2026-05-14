"use client";

import { cn } from "@antfly/design-system";
import type { GraphLegendProps } from "./types";

export function GraphLegend({ typeLabels, typeColors, className }: GraphLegendProps) {
  if (typeColors.size === 0) return null;

  return (
    <div
      className={cn(
        "flex flex-wrap items-center gap-x-4 gap-y-1.5 rounded-md border border-border/50 bg-background/70 px-3 py-2 text-xs backdrop-blur-sm",
        className
      )}
    >
      {Array.from(typeColors.entries()).map(([type, color]) => (
        <div key={type} className="flex items-center gap-1.5">
          <div className="size-2.5 shrink-0 rounded-full" style={{ backgroundColor: color }} />
          <span className="capitalize text-foreground">{typeLabels?.[type] ?? type}</span>
        </div>
      ))}
    </div>
  );
}
