import { cva, type VariantProps } from "class-variance-authority";
import type * as React from "react";

import { cn } from "@/lib/utils";

const alertVariants = cva(
  [
    // quiet panel: tinted-neutral surface, no outer chassis — a 2px semantic
    // left edge plus icon/title color carry the state.
    "relative w-full bg-background-secondary text-foreground rounded-none border-0 border-l-2 px-4 py-3.5",
    // grid: icon column collapses to 0 when no svg present
    "grid has-[>svg]:grid-cols-[18px_1fr] grid-cols-[0_1fr] has-[>svg]:gap-x-[11px] gap-y-0 items-start",
    "[&>svg]:size-[18px] [&>svg]:translate-y-[1px]",
  ].join(" "),
  {
    variants: {
      variant: {
        // one signal: the icon (and left edge) carry the state; titles stay ink
        default: "border-l-foreground [&>svg]:text-foreground",
        destructive: "border-l-destructive [&>svg]:text-destructive",
        success: "border-l-success [&>svg]:text-success",
        warning: "border-l-warning [&>svg]:text-warning",
        info: "border-l-info [&>svg]:text-info",
      },
    },
    defaultVariants: {
      variant: "default",
    },
  }
);

function Alert({
  className,
  variant,
  ...props
}: React.ComponentProps<"div"> & VariantProps<typeof alertVariants>) {
  return (
    <div
      data-slot="alert"
      role="alert"
      className={cn(alertVariants({ variant }), className)}
      {...props}
    />
  );
}

function AlertTitle({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="alert-title"
      className={cn(
        // mono uppercase callout — label register, medium weight
        "col-start-2 font-mono uppercase tracking-[0.08em] text-[11px] font-medium leading-none mb-[2px] text-foreground",
        className
      )}
      {...props}
    />
  );
}

function AlertDescription({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="alert-description"
      className={cn(
        "text-foreground col-start-2 grid justify-items-start gap-1 text-[13px] leading-relaxed [&_p]:leading-relaxed",
        className
      )}
      {...props}
    />
  );
}

export { Alert, AlertDescription, AlertTitle };
