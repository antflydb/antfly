"use client";

import type { HTMLAttributes } from "react";
import { cn } from "@/lib/utils";

export type ButtonGroupProps = HTMLAttributes<HTMLDivElement> & {
  orientation?: "horizontal" | "vertical";
};

/**
 * Adjacent buttons with merged borders. Children overlap by the chassis
 * border width (1px) so shared edges read as a single line.
 */
export const ButtonGroup = ({
  className,
  orientation = "horizontal",
  ...props
}: ButtonGroupProps) => (
  <div
    role="group"
    className={cn(
      "inline-flex items-center",
      orientation === "horizontal" && "[&>*:not(:first-child)]:-ml-px",
      orientation === "vertical" && "flex-col [&>*:not(:first-child)]:-mt-px",
      className
    )}
    {...props}
  />
);

export type ButtonGroupTextProps = HTMLAttributes<HTMLSpanElement>;

export const ButtonGroupText = ({ className, ...props }: ButtonGroupTextProps) => (
  <span
    className={cn(
      "inline-flex items-center justify-center px-2 text-xs font-medium text-muted-foreground",
      className
    )}
    {...props}
  />
);
