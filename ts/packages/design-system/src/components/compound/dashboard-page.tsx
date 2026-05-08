import type * as React from "react";
import { cn } from "@/lib/utils";

function DashboardPage({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="dashboard-page"
      className={cn("flex min-w-0 flex-col gap-4", className)}
      {...props}
    />
  );
}

function DashboardPageHeader({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="dashboard-page-header"
      className={cn(
        "flex flex-col gap-4 pb-2 md:flex-row md:items-end md:justify-between",
        className
      )}
      {...props}
    />
  );
}

function DashboardPageTitle({ className, ...props }: React.ComponentProps<"h1">) {
  return (
    <h1
      data-slot="dashboard-page-title"
      className={cn("font-display text-2xl tracking-tight text-foreground", className)}
      {...props}
    />
  );
}

function DashboardPageDescription({ className, ...props }: React.ComponentProps<"p">) {
  return (
    <p
      data-slot="dashboard-page-description"
      className={cn("max-w-2xl text-sm text-muted-foreground md:text-base", className)}
      {...props}
    />
  );
}

function DashboardPageActions({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="dashboard-page-actions"
      className={cn("flex flex-wrap items-center gap-2", className)}
      {...props}
    />
  );
}

function DashboardToolbar({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="dashboard-toolbar"
      className={cn(
        "flex flex-col gap-3 rounded-[var(--radius)] border bg-card p-3 md:flex-row md:items-end",
        className
      )}
      {...props}
    />
  );
}

export {
  DashboardPage,
  DashboardPageActions,
  DashboardPageDescription,
  DashboardPageHeader,
  DashboardPageTitle,
  DashboardToolbar,
};
