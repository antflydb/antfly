"use client";

import * as TabsPrimitive from "@radix-ui/react-tabs";
import type * as React from "react";

import { cn } from "@/lib/utils";

function Tabs({ className, ...props }: React.ComponentProps<typeof TabsPrimitive.Root>) {
  return (
    <TabsPrimitive.Root
      data-slot="tabs"
      className={cn("flex flex-col gap-4", className)}
      {...props}
    />
  );
}

function TabsList({ className, ...props }: React.ComponentProps<typeof TabsPrimitive.List>) {
  return (
    <TabsPrimitive.List
      data-slot="tabs-list"
      // the rail — chassis line that runs under the triggers
      className={cn(
        "inline-flex items-end gap-0 border-b-[1.5px] border-border-strong",
        className
      )}
      {...props}
    />
  );
}

function TabsTrigger({ className, ...props }: React.ComponentProps<typeof TabsPrimitive.Trigger>) {
  return (
    <TabsPrimitive.Trigger
      data-slot="tabs-trigger"
      className={cn(
        // mono kicker voice — matches the prototype's nav .eyebrow treatment
        "font-mono uppercase tracking-[0.1em] text-[11px] font-medium",
        "inline-flex items-center justify-center gap-2 px-3 py-[10px]",
        // overlap the rail: the trigger's bottom border sits ON the list's border
        "-mb-[1.5px] border-b-[1.5px] border-transparent",
        "text-muted-foreground transition-colors",
        "hover:text-foreground",
        "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring/30",
        "disabled:pointer-events-none disabled:opacity-50",
        // active: amber underline + foreground text
        "data-[state=active]:text-foreground data-[state=active]:border-amber-500",
        "[&_svg]:pointer-events-none [&_svg]:shrink-0 [&_svg:not([class*='size-'])]:size-4",
        "[&_svg:not([class*='text-'])]:text-muted-foreground data-[state=active]:[&_svg:not([class*='text-'])]:text-foreground",
        className
      )}
      {...props}
    />
  );
}

function TabsContent({ className, ...props }: React.ComponentProps<typeof TabsPrimitive.Content>) {
  return (
    <TabsPrimitive.Content
      data-slot="tabs-content"
      className={cn("flex-1 outline-none", className)}
      {...props}
    />
  );
}

export { Tabs, TabsContent, TabsList, TabsTrigger };
