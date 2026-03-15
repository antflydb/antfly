import type React from "react";
import { useMemo } from "react";
import { PipelineEdge } from "./PipelineEdge";
import { PipelineNode } from "./PipelineNode";
import { computeLayout } from "./pipeline-layout";
import type { PipelineStepState } from "./pipeline-types";
import "./pipeline-graph.css";

interface PipelineGraphProps {
  steps: PipelineStepState[];
  selectedStepId: string | null;
  onSelectStep: (stepId: string) => void;
}

export const PipelineGraph: React.FC<PipelineGraphProps> = ({
  steps,
  selectedStepId,
  onSelectStep,
}) => {
  const layout = useMemo(() => computeLayout(steps.length), [steps.length]);

  if (steps.length === 0) return null;

  return (
    <div className="relative mx-auto" style={{ width: layout.width, height: layout.height }}>
      {/* SVG overlay for edges */}
      <svg
        className="absolute inset-0 pointer-events-none"
        width={layout.width}
        height={layout.height}
        overflow="visible"
        aria-hidden="true"
      >
        <title>Pipeline graph edges</title>
        <defs>
          <marker id="arrowhead" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto">
            <polygon points="0 0, 8 3, 0 6" className="fill-muted-foreground/40" />
          </marker>
        </defs>
        {layout.edges.map((edge) => (
          <PipelineEdge
            key={edge.pathId}
            d={edge.d}
            pathId={edge.pathId}
            sourceStatus={steps[edge.from].status}
            targetStatus={steps[edge.to].status}
          />
        ))}
      </svg>

      {/* HTML nodes */}
      {layout.nodes.map((node) => {
        const step = steps[node.index];
        const duration =
          step.startTime && step.endTime ? `${(step.endTime - step.startTime).toFixed(0)}ms` : null;

        return (
          <PipelineNode
            key={step.id}
            stepId={step.id}
            label={step.label}
            status={step.status}
            duration={duration}
            selected={selectedStepId === step.id}
            x={node.x}
            y={node.y}
            onClick={() => onSelectStep(step.id)}
          />
        );
      })}
    </div>
  );
};
