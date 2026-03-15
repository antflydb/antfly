"use client";

import type { ComponentProps, HTMLAttributes, TextareaHTMLAttributes } from "react";
import { forwardRef } from "react";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

export type InputGroupProps = HTMLAttributes<HTMLDivElement>;

export const InputGroup = ({ className, ...props }: InputGroupProps) => (
  <div
    className={cn(
      "flex w-full flex-col rounded-lg border bg-background shadow-xs focus-within:ring-2 focus-within:ring-ring/50",
      className
    )}
    {...props}
  />
);

export type InputGroupTextareaProps = TextareaHTMLAttributes<HTMLTextAreaElement>;

export const InputGroupTextarea = forwardRef<HTMLTextAreaElement, InputGroupTextareaProps>(
  ({ className, ...props }, ref) => (
    <textarea
      ref={ref}
      className={cn(
        "w-full resize-none border-0 bg-transparent px-3 py-2 text-sm placeholder:text-muted-foreground focus:outline-none disabled:cursor-not-allowed disabled:opacity-50",
        className
      )}
      {...props}
    />
  )
);
InputGroupTextarea.displayName = "InputGroupTextarea";

export type InputGroupAddonProps = HTMLAttributes<HTMLDivElement> & {
  align?: "block-start" | "block-end" | "inline-start" | "inline-end";
};

export const InputGroupAddon = ({
  className,
  align = "block-end",
  ...props
}: InputGroupAddonProps) => (
  <div
    className={cn(
      "flex items-center px-2 py-1",
      align === "block-start" && "border-b",
      align === "block-end" && "border-t",
      className
    )}
    {...props}
  />
);

export type InputGroupButtonProps = ComponentProps<typeof Button>;

export const InputGroupButton = ({ className, ...props }: InputGroupButtonProps) => (
  <Button className={cn(className)} {...props} />
);
