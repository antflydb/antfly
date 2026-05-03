"use client";

import { Handle, type Node, type NodeProps, Position } from "@xyflow/react";
import { memo } from "react";
import { NodeTooltipWrapper } from "./graph-tooltip";
import type { InternalNodeData } from "./types";

const handleStyle = {
  opacity: 0,
  pointerEvents: "none" as const,
};

const GraphNodeComponent = memo(({ data }: NodeProps<Node<InternalNodeData>>) => {
  const { graphNode, radius, colorVar, selected, renderNode, renderTooltip } = data;
  const diameter = radius * 2;

  if (renderNode) {
    const rendered = renderNode(graphNode, {
      color: colorVar,
      radius,
      label: graphNode.label,
      selected,
    });
    return (
      <NodeTooltipWrapper node={graphNode} renderTooltip={renderTooltip}>
        <div style={{ position: "relative" }}>
          {rendered}
          <Handle type="source" position={Position.Top} style={{ ...handleStyle, top: radius }} />
          <Handle
            type="target"
            position={Position.Bottom}
            style={{ ...handleStyle, top: radius }}
          />
        </div>
      </NodeTooltipWrapper>
    );
  }

  return (
    <NodeTooltipWrapper node={graphNode} renderTooltip={renderTooltip}>
      <div
        className="relative flex flex-col items-center"
        style={{ width: diameter, height: diameter + 16 }}
      >
        <div
          className="rounded-full shadow-sm transition-shadow"
          style={{
            width: diameter,
            height: diameter,
            backgroundColor: colorVar,
            outline: selected ? "2px solid var(--background)" : "none",
            outlineOffset: 1,
            boxShadow: selected
              ? `0 0 0 3px color-mix(in oklch, ${colorVar} 40%, transparent)`
              : undefined,
          }}
        />
        <div
          className="mt-0.5 text-[9px] font-medium text-center leading-tight truncate"
          style={{ maxWidth: Math.max(diameter + 20, 70), color: "var(--graph-node-label)" }}
        >
          {graphNode.label}
        </div>
        <Handle type="source" position={Position.Top} style={{ ...handleStyle, top: radius }} />
        <Handle type="target" position={Position.Bottom} style={{ ...handleStyle, top: radius }} />
      </div>
    </NodeTooltipWrapper>
  );
});

GraphNodeComponent.displayName = "GraphNode";

export { GraphNodeComponent };
