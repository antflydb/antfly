import {
  AlertCircle,
  BookOpen,
  Check,
  ChevronDown,
  HelpCircle,
  Loader2,
  Search,
  Sparkles,
  Target,
} from "lucide-react";
import type React from "react";
import { cn } from "@/lib/utils";
import { NODE_HEIGHT, NODE_WIDTH } from "./pipeline-layout";
import type { PipelineStepId, PipelineStepStatus } from "./pipeline-types";

const STEP_ICONS: Record<PipelineStepId, React.FC<{ className?: string }>> = {
  classification: Sparkles,
  search: Search,
  generation: BookOpen,
  confidence: Target,
  followup: HelpCircle,
};

interface PipelineNodeProps {
  stepId: PipelineStepId;
  label: string;
  status: PipelineStepStatus;
  duration: string | null;
  selected: boolean;
  x: number;
  y: number;
  onClick: () => void;
}

export const PipelineNode: React.FC<PipelineNodeProps> = ({
  stepId,
  label,
  status,
  duration,
  selected,
  x,
  y,
  onClick,
}) => {
  const Icon = STEP_ICONS[stepId];

  const statusIcon = () => {
    switch (status) {
      case "complete":
        return <Check className="af-status-icon-success w-3.5 h-3.5" />;
      case "running":
        return <Loader2 className="af-status-icon-info w-3.5 h-3.5 animate-spin" />;
      case "error":
        return <AlertCircle className="af-status-icon-error w-3.5 h-3.5" />;
      case "pending":
      case "skipped":
        return <div className="w-3 h-3 rounded-full border-2 border-muted-foreground/30" />;
    }
  };

  return (
    <button
      type="button"
      onClick={onClick}
      className={cn(
        "absolute flex items-center gap-2 rounded-xl border px-3 transition-all cursor-pointer select-none",
        // Status-dependent styles
        status === "pending" && "border-border/50 bg-muted/30 opacity-50",
        status === "running" &&
          "af-status-border-info af-status-surface-info pipeline-node-running",
        status === "complete" &&
          "af-status-border-success bg-background pipeline-node-complete-flash",
        status === "error" && "af-status-border-error bg-background pipeline-node-error-shake",
        status === "skipped" && "border-border/50 bg-muted/30 opacity-50",
        // Selected state
        selected && "af-status-selected",
        // Hover
        status !== "pending" && status !== "skipped" && "hover:bg-muted/50"
      )}
      style={{
        left: x,
        top: y,
        width: NODE_WIDTH,
        height: NODE_HEIGHT,
      }}
      disabled={status === "pending" || status === "skipped"}
    >
      <div className="flex items-center justify-center w-[18px] shrink-0">{statusIcon()}</div>
      <Icon className="w-3.5 h-3.5 text-muted-foreground shrink-0" />
      <span className="text-xs font-medium truncate flex-1 text-left">{label}</span>
      {duration && (
        <span className="text-[10px] text-muted-foreground tabular-nums shrink-0">{duration}</span>
      )}
      {selected && <ChevronDown className="af-status-icon-info w-3 h-3 shrink-0" />}
    </button>
  );
};
