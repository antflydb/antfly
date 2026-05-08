import type * as React from "react";
import { cn } from "@/lib/utils";

function AuthShell({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      className={cn(
        "af-auth-shell flex min-h-screen items-center justify-center bg-background p-4 text-foreground",
        className
      )}
      {...props}
    />
  );
}

function StatusScreen({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      className={cn(
        "af-status-screen grid min-h-screen place-items-center bg-background p-4 text-foreground",
        className
      )}
      {...props}
    />
  );
}

function StatusCard({ className, ...props }: React.ComponentProps<"div">) {
  return <div className={cn("af-status-card w-full max-w-md", className)} {...props} />;
}

export { AuthShell, StatusCard, StatusScreen };
