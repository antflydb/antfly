import {
  AlertCircle,
  BookOpen,
  Check,
  ChevronDown,
  ChevronRight,
  HelpCircle,
  Loader2,
  Search,
  Sparkles,
  Target,
} from "lucide-react";
import type React from "react";
import { useState } from "react";
import { Badge } from "@/components/ui/badge";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { cn } from "@/lib/utils";
import type {
  ClassificationStepData,
  ConfidenceStepData,
  FollowupStepData,
  GenerationStepData,
  PipelineStepId,
  PipelineStepState,
  SearchStepData,
} from "./pipeline-types";

const STEP_ICONS: Record<PipelineStepId, React.FC<{ className?: string }>> = {
  classification: Sparkles,
  search: Search,
  generation: BookOpen,
  confidence: Target,
  followup: HelpCircle,
};

interface PipelineStepProps {
  step: PipelineStepState;
  isLast: boolean;
  onFollowupClick?: (question: string) => void;
  formatAnswer?: (text: string) => React.ReactNode;
}

export const PipelineStep: React.FC<PipelineStepProps> = ({
  step,
  isLast,
  onFollowupClick,
  formatAnswer,
}) => {
  const [expanded, setExpanded] = useState(step.status === "running");
  const Icon = STEP_ICONS[step.id];

  const duration =
    step.startTime && step.endTime ? `${(step.endTime - step.startTime).toFixed(0)}ms` : null;

  const statusIcon = () => {
    switch (step.status) {
      case "complete":
        return <Check className="w-3.5 h-3.5 text-green-500" />;
      case "running":
        return <Loader2 className="w-3.5 h-3.5 text-blue-500 animate-spin" />;
      case "error":
        return <AlertCircle className="w-3.5 h-3.5 text-red-500" />;
      case "pending":
        return <div className="w-3 h-3 rounded-full border-2 border-muted-foreground/30" />;
      case "skipped":
        return <div className="w-3 h-3 rounded-full bg-muted-foreground/20" />;
    }
  };

  const statusColor = () => {
    switch (step.status) {
      case "complete":
        return "border-green-500/30";
      case "running":
        return "border-blue-500/30 bg-blue-500/5";
      case "error":
        return "border-red-500/30";
      default:
        return "border-border/50";
    }
  };

  // Auto-expand running steps
  const isOpen = expanded || step.status === "running";

  return (
    <div className="relative">
      {/* Vertical connector line */}
      {!isLast && <div className="absolute left-[19px] top-10 bottom-0 w-px bg-border" />}

      <Collapsible open={isOpen} onOpenChange={setExpanded}>
        <CollapsibleTrigger asChild>
          <button
            type="button"
            className={cn(
              "w-full flex items-center gap-3 p-3 rounded-lg border transition-colors text-left",
              statusColor(),
              step.status !== "pending" && "hover:bg-muted/50 cursor-pointer"
            )}
            disabled={step.status === "pending"}
          >
            <div className="flex items-center justify-center w-[22px] shrink-0">{statusIcon()}</div>
            <Icon className="w-4 h-4 text-muted-foreground shrink-0" />
            <span className="text-sm font-medium flex-1">{step.label}</span>
            {duration && (
              <span className="text-xs text-muted-foreground tabular-nums">{duration}</span>
            )}
            {step.status !== "pending" &&
              (isOpen ? (
                <ChevronDown className="w-3.5 h-3.5 text-muted-foreground" />
              ) : (
                <ChevronRight className="w-3.5 h-3.5 text-muted-foreground" />
              ))}
          </button>
        </CollapsibleTrigger>
        <CollapsibleContent>
          <div className="ml-[31px] mt-2 mb-3 pl-3 border-l-2 border-border/50">
            {renderStepContent(step, onFollowupClick, formatAnswer)}
          </div>
        </CollapsibleContent>
      </Collapsible>
    </div>
  );
};

function renderStepContent(
  step: PipelineStepState,
  onFollowupClick?: (question: string) => void,
  formatAnswer?: (text: string) => React.ReactNode
): React.ReactNode {
  if (!step.data && step.status !== "running") return null;

  switch (step.id) {
    case "classification": {
      const data = step.data as ClassificationStepData | undefined;
      if (!data?.classification) return null;
      const c = data.classification;
      return (
        <div className="space-y-2 text-xs">
          <div className="grid grid-cols-2 gap-2">
            <div>
              <span className="text-muted-foreground">Strategy:</span>{" "}
              <span className="font-medium">{c.strategy}</span>
            </div>
            <div>
              <span className="text-muted-foreground">Mode:</span>{" "}
              <span className="font-medium">{c.semantic_mode}</span>
            </div>
          </div>
          {c.semantic_query && (
            <div>
              <span className="text-muted-foreground">Semantic Query:</span>{" "}
              <span className="italic">{c.semantic_query}</span>
            </div>
          )}
          {c.reasoning && (
            <div className="p-2 bg-muted/50 rounded text-muted-foreground">{c.reasoning}</div>
          )}
        </div>
      );
    }

    case "search": {
      const data = step.data as SearchStepData | undefined;
      if (!data) return null;
      return (
        <div className="space-y-2 text-xs">
          {data.filterApplied && (
            <div>
              <span className="text-muted-foreground">Filter:</span>{" "}
              <code className="bg-muted px-1 rounded">{data.filterApplied}</code>
            </div>
          )}
          <div className="flex items-center gap-2">
            <Badge variant="secondary" className="text-xs">
              {data.hits.length} documents retrieved
            </Badge>
          </div>
          {data.hits.length > 0 && (
            <div className="space-y-1 max-h-48 overflow-y-auto">
              {data.hits.map((hit, i) => (
                <div key={hit._id || i} className="p-2 rounded border bg-background text-xs">
                  <div className="flex items-center justify-between">
                    <span className="font-medium truncate">{hit._id}</span>
                    <Badge variant="secondary" className="text-[10px]">
                      {hit._score?.toFixed(3)}
                    </Badge>
                  </div>
                  {hit._source && (
                    <pre className="text-muted-foreground overflow-x-auto whitespace-pre-wrap mt-1 line-clamp-3">
                      {JSON.stringify(hit._source, null, 2).slice(0, 300)}
                    </pre>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>
      );
    }

    case "generation": {
      const data = step.data as GenerationStepData | undefined;
      if (!data) {
        if (step.status === "running") {
          return (
            <div className="text-sm text-muted-foreground italic">
              Generating...
              <span className="inline-block w-2 h-4 bg-foreground/50 animate-pulse ml-0.5" />
            </div>
          );
        }
        return null;
      }
      return (
        <div className="space-y-2">
          {data.provider && data.model && (
            <Badge variant="outline" className="text-[10px]">
              {data.provider}/{data.model}
            </Badge>
          )}
          <div className="text-sm">
            {formatAnswer ? formatAnswer(data.answer) : data.answer}
            {step.status === "running" && (
              <span className="inline-block w-2 h-4 bg-foreground/50 animate-pulse ml-0.5" />
            )}
          </div>
        </div>
      );
    }

    case "confidence": {
      const data = step.data as ConfidenceStepData | undefined;
      if (!data) return null;
      return (
        <div className="space-y-2">
          <div className="space-y-1.5">
            <div className="flex items-center justify-between text-xs">
              <span className="text-muted-foreground">Generation Confidence</span>
              <span
                className={cn(
                  "font-medium",
                  data.generation > 0.7
                    ? "text-green-600"
                    : data.generation > 0.4
                      ? "text-yellow-600"
                      : "text-red-600"
                )}
              >
                {(data.generation * 100).toFixed(0)}%
              </span>
            </div>
            <div className="w-full bg-muted rounded-full h-1.5">
              <div
                className={cn(
                  "h-1.5 rounded-full transition-all",
                  data.generation > 0.7
                    ? "bg-green-500"
                    : data.generation > 0.4
                      ? "bg-yellow-500"
                      : "bg-red-400"
                )}
                style={{ width: `${data.generation * 100}%` }}
              />
            </div>
          </div>
          <div className="space-y-1.5">
            <div className="flex items-center justify-between text-xs">
              <span className="text-muted-foreground">Context Relevance</span>
              <span
                className={cn(
                  "font-medium",
                  data.context > 0.7
                    ? "text-green-600"
                    : data.context > 0.4
                      ? "text-yellow-600"
                      : "text-red-600"
                )}
              >
                {(data.context * 100).toFixed(0)}%
              </span>
            </div>
            <div className="w-full bg-muted rounded-full h-1.5">
              <div
                className={cn(
                  "h-1.5 rounded-full transition-all",
                  data.context > 0.7
                    ? "bg-green-500"
                    : data.context > 0.4
                      ? "bg-yellow-500"
                      : "bg-red-400"
                )}
                style={{ width: `${data.context * 100}%` }}
              />
            </div>
          </div>
        </div>
      );
    }

    case "followup": {
      const data = step.data as FollowupStepData | undefined;
      if (!data || data.questions.length === 0) return null;
      return (
        <div className="space-y-1">
          {data.questions.map((q, _i) => (
            <button
              key={q}
              type="button"
              onClick={() => onFollowupClick?.(q)}
              className="w-full text-left text-sm p-2 rounded hover:bg-muted/50 transition-colors text-muted-foreground hover:text-foreground"
            >
              {q}
            </button>
          ))}
        </div>
      );
    }

    default:
      return null;
  }
}
