import type React from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { cn } from "@/lib/utils";

interface QuerySectionProps {
  title?: string;
  children: React.ReactNode;
  className?: string;
  variant?: "default" | "muted";
}

/**
 * QuerySection - Modern card-based wrapper for query builder sections
 * Provides consistent styling and visual hierarchy
 */
const QuerySection: React.FC<QuerySectionProps> = ({
  title,
  children,
  className,
  variant = "default",
}) => {
  if (!title) {
    // No title - just a container with consistent styling
    return (
      <div
        className={cn(
          "border rounded-lg p-6 space-y-4",
          variant === "default" && "bg-card shadow-sm",
          variant === "muted" && "bg-muted/30",
          className
        )}
      >
        {children}
      </div>
    );
  }

  // With title - use Card components
  return (
    <Card className={cn("shadow-sm", className)}>
      <CardHeader className="pb-4">
        <CardTitle className="text-lg">{title}</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">{children}</CardContent>
    </Card>
  );
};

export default QuerySection;
