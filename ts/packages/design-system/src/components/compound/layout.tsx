import { cva, type VariantProps } from "class-variance-authority";
import type * as React from "react";
import { cn } from "@/lib/utils";

/**
 * Layout primitives that encode the system's whitespace rhythm, so app
 * authors compose pages from named gaps instead of hand-rolled margins.
 *
 *   - Stack:   vertical flow (gap-3 / gap-6 / gap-10)
 *   - Inline:  horizontal wrapping row, centered items
 *   - Section: a page band with generous vertical padding; `lg` is the
 *              marketing scale
 */

const stackVariants = cva("flex flex-col", {
  variants: {
    gap: {
      sm: "gap-3",
      md: "gap-6",
      lg: "gap-10",
    },
  },
  defaultVariants: {
    gap: "md",
  },
});

function Stack({
  className,
  gap,
  ...props
}: React.ComponentProps<"div"> & VariantProps<typeof stackVariants>) {
  return <div data-slot="stack" className={cn(stackVariants({ gap }), className)} {...props} />;
}

const inlineVariants = cva("flex flex-wrap items-center", {
  variants: {
    gap: {
      sm: "gap-3",
      md: "gap-6",
      lg: "gap-10",
    },
  },
  defaultVariants: {
    gap: "sm",
  },
});

function Inline({
  className,
  gap,
  ...props
}: React.ComponentProps<"div"> & VariantProps<typeof inlineVariants>) {
  return <div data-slot="inline" className={cn(inlineVariants({ gap }), className)} {...props} />;
}

const sectionVariants = cva("w-full", {
  variants: {
    size: {
      default: "py-10 md:py-12",
      lg: "py-16 md:py-24",
    },
  },
  defaultVariants: {
    size: "default",
  },
});

function Section({
  className,
  size,
  ...props
}: React.ComponentProps<"section"> & VariantProps<typeof sectionVariants>) {
  return (
    <section data-slot="section" className={cn(sectionVariants({ size }), className)} {...props} />
  );
}

export { Inline, Section, Stack, inlineVariants, sectionVariants, stackVariants };
