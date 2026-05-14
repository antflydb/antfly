"use client";

import type { HTMLAttributes } from "react";
import { cn } from "@/lib/utils";

export type ButtonGroupProps = HTMLAttributes<HTMLDivElement> & {
  orientation?: "horizontal" | "vertical";
};

export const ButtonGroup = ({
  className,
  orientation = "horizontal",
  ...props
}: ButtonGroupProps) => (
  <div
    role="group"
    className={cn(
      "inline-flex items-center",
      orientation === "vertical" && "flex-col",
      "[&>*:not(:first-child)]:rounded-l-none [&>*:not(:last-child)]:rounded-r-none [&>*:not(:first-child)]:-ml-px",
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
