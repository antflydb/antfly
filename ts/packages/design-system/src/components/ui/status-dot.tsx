import * as React from "react";
import { cn } from "@/lib/utils";

export type StatusDotKind = "ok" | "warn" | "error" | "info" | "neutral";

const kindClass: Record<StatusDotKind, string> = {
  ok: "bg-success",
  warn: "bg-warning",
  error: "bg-destructive",
  info: "bg-info",
  neutral: "bg-muted-foreground",
};

export interface StatusDotProps extends React.HTMLAttributes<HTMLSpanElement> {
  /** Semantic status. @default "neutral" */
  kind?: StatusDotKind;
}

/**
 * Small (7px) round status indicator — the lightweight alternative to a Badge
 * for inline row-level state in data tables and lists. Pairs naturally with
 * mono identifiers:
 *
 *   <TableCell><StatusDot kind="ok" /> healthy</TableCell>
 *
 * Reads as part of the row, not as a callout. For loud state callouts, use
 * Badge or an Alert instead.
 */
export const StatusDot = React.forwardRef<HTMLSpanElement, StatusDotProps>(
  ({ kind = "neutral", className, ...props }, ref) => (
    <span
      ref={ref}
      data-slot="status-dot"
      data-kind={kind}
      aria-hidden="true"
      className={cn(
        "inline-block size-[7px] mr-2 align-middle shrink-0",
        kindClass[kind],
        className
      )}
      {...props}
    />
  )
);
StatusDot.displayName = "StatusDot";
