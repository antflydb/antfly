import { cva, type VariantProps } from "class-variance-authority";
import type * as React from "react";

import { cn } from "@/lib/utils";

const alertVariants = cva(
  "relative w-full rounded-[var(--radius)] px-4 py-3 text-sm grid has-[>svg]:grid-cols-[calc(var(--spacing)*4)_1fr] grid-cols-[0_1fr] has-[>svg]:gap-x-3 gap-y-0.5 items-start [&>svg]:size-4 [&>svg]:translate-y-0.5 [&>svg]:text-current",
  {
    variants: {
      variant: {
        default:
          "bg-primary/10 text-searchaf-11 [&>svg]:text-searchaf-10 *:data-[slot=alert-description]:text-searchaf-10",
        destructive:
          "bg-danger/10 text-danger-700 [&>svg]:text-danger-600 *:data-[slot=alert-description]:text-danger-600",
        info: "bg-info/10 text-info-700 [&>svg]:text-info-600 *:data-[slot=alert-description]:text-info-600",
        success:
          "bg-success/10 text-success-700 [&>svg]:text-success-600 *:data-[slot=alert-description]:text-success-600",
        warning:
          "bg-warning/10 text-warning-700 [&>svg]:text-warning-600 *:data-[slot=alert-description]:text-warning-600",
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
      className={cn("col-start-2 line-clamp-1 min-h-4 font-medium tracking-tight", className)}
      {...props}
    />
  );
}

function AlertDescription({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="alert-description"
      className={cn(
        "text-muted-foreground col-start-2 grid justify-items-start gap-1 text-sm [&_p]:leading-relaxed",
        className
      )}
      {...props}
    />
  );
}

export { Alert, AlertDescription, AlertTitle };
