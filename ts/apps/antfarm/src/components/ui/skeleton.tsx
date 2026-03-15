import { cn } from "@/lib/utils";

function Skeleton({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="skeleton"
      className={cn(
        "bg-muted-foreground/10 animate-pulse rounded-[calc(var(--radius)-2px)]",
        className
      )}
      {...props}
    />
  );
}

export { Skeleton };
