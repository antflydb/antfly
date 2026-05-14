"use client";

import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@antfly/design-system";
import type { GraphNode, RenderTooltipFn } from "./types";

function DefaultTooltipContent({ node }: { node: GraphNode }) {
  return (
    <div className="space-y-1">
      <div className="font-medium text-sm">{node.label}</div>
      <div className="text-xs text-muted-foreground capitalize">{node.type}</div>
      {node.metric != null && (
        <div className="text-xs text-muted-foreground">Weight: {node.metric.toLocaleString()}</div>
      )}
    </div>
  );
}

export function NodeTooltipWrapper({
  children,
  node,
  renderTooltip,
}: {
  children: React.ReactNode;
  node: GraphNode;
  renderTooltip?: RenderTooltipFn;
}) {
  return (
    <TooltipProvider delayDuration={200}>
      <Tooltip>
        <TooltipTrigger asChild>{children}</TooltipTrigger>
        <TooltipContent side="top" className="max-w-xs">
          {renderTooltip ? renderTooltip(node) : <DefaultTooltipContent node={node} />}
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}
