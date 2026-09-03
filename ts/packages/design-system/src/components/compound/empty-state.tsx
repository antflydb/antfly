import type * as React from "react";
import { Anty } from "@/components/brand/anty";
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
    <div
      className={cn(
        "flex flex-col items-center justify-center rounded-none border-(length:--border-width) border-dashed border-border/70 bg-muted/30 px-6 py-16 text-center",
        className
      )}
      {...props}
    >
      {icon ? (
        <div className="mb-4 grid size-12 place-items-center rounded-none bg-background text-muted-foreground">
          {icon}
        </div>
      ) : null}
      <h3 className="font-display text-xl text-foreground">{title}</h3>
      {description ? (
        <p className="mt-2 max-w-md text-sm text-muted-foreground">{description}</p>
      ) : null}
      {action ? <div className="mt-6">{action}</div> : null}
    </div>
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
