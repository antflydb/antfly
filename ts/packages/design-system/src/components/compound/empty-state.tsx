import type * as React from "react";
import { Anty } from "@/components/brand/anty";
import {
  Empty,
  EmptyContent,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
} from "@/components/ui/empty";
import { cn } from "@/lib/utils";

interface EmptyStateProps extends Omit<React.HTMLAttributes<HTMLDivElement>, "title"> {
  icon?: React.ReactNode;
  title: React.ReactNode;
  description?: React.ReactNode;
  action?: React.ReactNode;
}

export function EmptyState({
  icon,
  title,
  description,
  action,
  className,
  ...props
}: EmptyStateProps) {
  return (
    <Empty
      className={cn(
        "gap-0 border-(length:--border-width) border-border/70 bg-muted/30 px-6 py-16",
        className
      )}
      {...props}
    >
      <EmptyHeader className="max-w-md gap-0">
        {icon ? (
          <EmptyMedia className="mb-4 grid size-12 place-items-center rounded-md bg-background text-muted-foreground">
            {icon}
          </EmptyMedia>
        ) : null}
        <h3 data-slot="empty-title" className="font-display text-xl text-foreground">
          {title}
        </h3>
        {description ? <EmptyDescription className="mt-2">{description}</EmptyDescription> : null}
      </EmptyHeader>
      {action ? <EmptyContent className="mt-6 w-auto max-w-none">{action}</EmptyContent> : null}
    </Empty>
  );
}

type AntyEmptyStateProps = Omit<EmptyStateProps, "icon">;

/**
 * Branded empty state — Anty held statically in its pixelated form (no
 * float), a quiet pixel brand moment. Eyes use the `original` style so the
 * vector↔pixel cross-fade matches the logo texture.
 */
export function AntyEmptyState({
  title,
  description,
  action,
  className,
  ...props
}: AntyEmptyStateProps) {
  return (
    <EmptyState
      icon={
        <div className="grid size-16 place-items-center">
          <Anty
            size={56}
            expression="idle"
            pixelated
            float={false}
            blink
            showShadow={false}
            showGlow
            eyeStyle="original"
          />
        </div>
      }
      title={title}
      description={description}
      action={action}
      className={className}
      {...props}
    />
  );
}
