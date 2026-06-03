import { cva, type VariantProps } from "class-variance-authority";
import type * as React from "react";

import { cn } from "@/lib/utils";

const alertVariants = cva(
  [
    // chassis: square, 1.5px bordered, flat. Left border bumped to 4px,
    // recolored to the semantic state — the only color signal in the component.
    "relative w-full bg-card text-card-foreground rounded-none border-[1.5px] border-border-strong border-l-4 px-[14px] py-[12px]",
    // grid: icon column collapses to 0 when no svg present
    "grid has-[>svg]:grid-cols-[18px_1fr] grid-cols-[0_1fr] has-[>svg]:gap-x-[11px] gap-y-0 items-start",
    "[&>svg]:size-[18px] [&>svg]:translate-y-[1px]",
  ].join(" "),
  {
    variants: {
      variant: {
        default: "border-l-foreground [&>svg]:text-foreground *:data-[slot=alert-title]:text-foreground",
        destructive:
          "border-l-destructive [&>svg]:text-destructive *:data-[slot=alert-title]:text-destructive",
        success:
          "border-l-success [&>svg]:text-success *:data-[slot=alert-title]:text-success",
        warning:
          "border-l-warning [&>svg]:text-warning *:data-[slot=alert-title]:text-warning",
        info: "border-l-info [&>svg]:text-info *:data-[slot=alert-title]:text-info",
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
        // mono uppercase callout — the loud register inside the chassis
        "col-start-2 font-mono uppercase tracking-[0.06em] text-[12px] font-bold leading-none mb-[2px]",
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
        "text-foreground col-start-2 grid justify-items-start gap-1 text-[13px] [&_p]:leading-relaxed",
        className
      )}
      {...props}
    />
  );
}

export { Alert, AlertDescription, AlertTitle };
