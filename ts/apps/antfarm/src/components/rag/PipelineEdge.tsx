import type React from "react";
import { cn } from "@/lib/utils";
import type { PipelineStepStatus } from "./pipeline-types";

interface PipelineEdgeProps {
  d: string;
  pathId: string;
  sourceStatus: PipelineStepStatus;
  targetStatus: PipelineStepStatus;
}

type EdgeState = "pending" | "active" | "complete" | "error";

function getEdgeState(
  sourceStatus: PipelineStepStatus,
  targetStatus: PipelineStepStatus
): EdgeState {
  if (sourceStatus === "error" || targetStatus === "error") return "error";
  if (sourceStatus === "complete" && targetStatus === "complete") return "complete";
  if (sourceStatus === "complete" && targetStatus === "running") return "active";
  return "pending";
}

export const PipelineEdge: React.FC<PipelineEdgeProps> = ({
  d,
  pathId,
  sourceStatus,
  targetStatus,
}) => {
  const edgeState = getEdgeState(sourceStatus, targetStatus);

  return (
    <g>
      <path
        id={pathId}
        d={d}
        fill="none"
        strokeWidth={1.5}
        markerEnd="url(#arrowhead)"
        className={cn(
          edgeState === "pending" && "stroke-muted-foreground/30",
          edgeState === "active" && "stroke-blue-500 pipeline-edge-active",
          edgeState === "complete" && "stroke-green-500/60",
          edgeState === "error" && "stroke-red-500/60"
        )}
        strokeDasharray={edgeState === "pending" ? "4 4" : undefined}
      />
      {edgeState === "active" && (
        <circle
          r={3}
          className="fill-blue-500 pipeline-edge-pulse-dot"
          style={{ offsetPath: `path('${d}')` }}
        />
      )}
    </g>
  );
};
