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
        return <Check className="w-3.5 h-3.5 text-green-500" />;
      case "running":
        return <Loader2 className="w-3.5 h-3.5 text-blue-500 animate-spin" />;
      case "error":
        return <AlertCircle className="w-3.5 h-3.5 text-red-500" />;
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
        status === "running" && "border-blue-500 bg-blue-500/5 pipeline-node-running",
        status === "complete" && "border-green-500/30 bg-background pipeline-node-complete-flash",
        status === "error" && "border-red-500/30 bg-background pipeline-node-error-shake",
        status === "skipped" && "border-border/50 bg-muted/30 opacity-50",
        // Selected state
        selected && "ring-2 ring-blue-500/50 border-blue-500",
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
      {selected && <ChevronDown className="w-3 h-3 text-blue-500 shrink-0" />}
    </button>
  );
};
