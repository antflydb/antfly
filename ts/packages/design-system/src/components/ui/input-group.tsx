"use client";

import type { ComponentProps, HTMLAttributes, TextareaHTMLAttributes } from "react";
import { forwardRef } from "react";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

export type InputGroupProps = HTMLAttributes<HTMLDivElement>;

/**
 * A bordered shell that composes a text field with addon rows (prefix/suffix
 * actions). Carries the control chassis itself — children render borderless.
 */
export const InputGroup = ({ className, ...props }: InputGroupProps) => (
  <div
    className={cn(
      "flex w-full flex-col rounded-none border-(length:--border-width) border-input bg-transparent",
      "transition-[border-color,box-shadow] focus-within:border-ring focus-within:ring-2 focus-within:ring-ring/30",
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
        // body voice — matches Input; the prompt the user types is a phrase
        "w-full resize-none border-0 bg-transparent px-3 py-2 font-sans text-sm text-foreground",
        "placeholder:text-muted-foreground focus:outline-none disabled:cursor-not-allowed disabled:opacity-50",
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
      align === "block-start" && "border-b-(length:--border-width) border-border",
      align === "block-end" && "border-t-(length:--border-width) border-border",
      className
    )}
    {...props}
  />
);

export type InputGroupButtonProps = ComponentProps<typeof Button>;

export const InputGroupButton = ({ className, ...props }: InputGroupButtonProps) => (
  <Button className={cn(className)} {...props} />
);
