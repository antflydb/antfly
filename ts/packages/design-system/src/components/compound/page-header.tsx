import type * as React from "react";
import { cn } from "@/lib/utils";

interface PageHeaderProps extends Omit<React.HTMLAttributes<HTMLDivElement>, "title"> {
  title: React.ReactNode;
  description?: React.ReactNode;
  actions?: React.ReactNode;
}

export function PageHeader({ title, description, actions, className, ...props }: PageHeaderProps) {
  return (
    <div
      className={cn(
        "flex flex-col gap-4 pb-6 md:flex-row md:items-end md:justify-between",
        className
      )}
      {...props}
    >
      <div className="space-y-1.5">
        <h1 className="font-display text-3xl tracking-tight text-foreground md:text-4xl">
          {title}
        </h1>
        {description ? (
          <p className="max-w-2xl text-base text-muted-foreground">{description}</p>
        ) : null}
      </div>
      {actions ? <div className="flex flex-wrap items-center gap-2">{actions}</div> : null}
    </div>
  );
}
