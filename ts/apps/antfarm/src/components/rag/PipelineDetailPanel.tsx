import type React from "react";
import { Badge } from "@/components/ui/badge";
import { Collapsible, CollapsibleContent } from "@/components/ui/collapsible";
import { cn } from "@/lib/utils";
import type {
  ClassificationStepData,
  ConfidenceStepData,
  FollowupStepData,
  GenerationStepData,
  PipelineStepState,
  SearchStepData,
} from "./pipeline-types";

interface PipelineDetailPanelProps {
  step: PipelineStepState | null;
  open: boolean;
  onFollowupClick?: (question: string) => void;
  formatAnswer?: (text: string) => React.ReactNode;
}

export const PipelineDetailPanel: React.FC<PipelineDetailPanelProps> = ({
  step,
  open,
  onFollowupClick,
  formatAnswer,
}) => {
  return (
    <Collapsible open={open && step !== null}>
      <CollapsibleContent>
        {step && (
          <div className="mt-4 p-4 rounded-lg border border-border/50 bg-muted/10">
            <div className="flex items-center gap-2 mb-3">
              <h4 className="text-sm font-medium">{step.label}</h4>
              {step.startTime && step.endTime && (
                <span className="text-xs text-muted-foreground tabular-nums">
                  {(step.endTime - step.startTime).toFixed(0)}ms
                </span>
              )}
            </div>
            {renderStepContent(step, onFollowupClick, formatAnswer)}
          </div>
        )}
      </CollapsibleContent>
    </Collapsible>
  );
};

function renderStepContent(
  step: PipelineStepState,
  onFollowupClick?: (question: string) => void,
  formatAnswer?: (text: string) => React.ReactNode
): React.ReactNode {
  if (step.status === "error" && typeof step.data === "string") {
    return <div className="text-sm text-red-500 bg-red-500/5 p-2 rounded">{step.data}</div>;
  }

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
          {data.questions.map((q) => (
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
