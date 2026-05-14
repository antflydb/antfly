import type * as React from "react";
import { Card, CardContent } from "@/components/ui/card";
import { cn } from "@/lib/utils";

interface StatCardProps extends React.HTMLAttributes<HTMLDivElement> {
  label: React.ReactNode;
  value: React.ReactNode;
  delta?: React.ReactNode;
  icon?: React.ReactNode;
  tone?: "default" | "positive" | "negative";
}

const toneClasses = {
  default: "text-muted-foreground",
  positive: "text-success",
  negative: "text-destructive",
} as const;

export function StatCard({
  label,
  value,
  delta,
  icon,
  tone = "default",
  className,
  ...props
}: StatCardProps) {
  return (
    <Card className={cn("overflow-hidden", className)} {...props}>
      <CardContent className="flex items-start justify-between gap-4 p-6">
        <div className="space-y-1">
          <p className="text-sm font-medium text-muted-foreground">{label}</p>
          <p className="font-display text-3xl tracking-tight text-foreground">{value}</p>
          {delta ? <p className={cn("text-xs font-medium", toneClasses[tone])}>{delta}</p> : null}
        </div>
        {icon ? (
          <div className="grid size-10 place-items-center rounded-lg bg-accent text-accent-foreground">
            {icon}
          </div>
        ) : null}
      </CardContent>
    </Card>
  );
}
