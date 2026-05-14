"use client";

import { Button, Collapsible, CollapsibleContent, CollapsibleTrigger } from "@antfly/design-system";
import { ChevronsUpDown } from "lucide-react";
import * as React from "react";

export function CollapsibleDemo() {
  const [open, setOpen] = React.useState(false);
  return (
    <Collapsible open={open} onOpenChange={setOpen} className="max-w-sm space-y-2">
      <div className="flex items-center justify-between rounded-md border border-border px-4 py-2">
        <span className="text-sm font-medium">3 shards</span>
        <CollapsibleTrigger asChild>
          <Button variant="ghost" size="sm">
            <ChevronsUpDown className="size-4" />
            <span className="sr-only">Toggle</span>
          </Button>
        </CollapsibleTrigger>
      </div>
      <div className="rounded-md border border-border px-4 py-2 text-sm">shard-0 (leader)</div>
      <CollapsibleContent className="space-y-2">
        <div className="rounded-md border border-border px-4 py-2 text-sm">shard-1</div>
        <div className="rounded-md border border-border px-4 py-2 text-sm">shard-2</div>
      </CollapsibleContent>
    </Collapsible>
  );
}
