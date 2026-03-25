import type { AgentStep, SSEStepStarted } from "@antfly/sdk";
import {
  ChevronDown,
  ChevronRight,
  Filter,
  Globe,
  Loader2,
  Search,
  TreePine,
  Waypoints,
} from "lucide-react";
import type React from "react";
import { useState } from "react";
import { Badge } from "@/components/ui/badge";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";

interface ReasoningChainCollapsibleProps {
  chain: AgentStep[];
  activeSteps?: SSEStepStarted[];
  toolCallsMade?: number;
  reasoningText?: string;
  isStreaming?: boolean;
}

const STEP_ICONS: Record<string, React.ReactNode> = {
  semantic_search: <Search className="h-3.5 w-3.5" />,
  full_text_search: <Search className="h-3.5 w-3.5" />,
  tree_search: <TreePine className="h-3.5 w-3.5" />,
  graph_search: <Waypoints className="h-3.5 w-3.5" />,
  websearch: <Globe className="h-3.5 w-3.5" />,
  fetch: <Globe className="h-3.5 w-3.5" />,
  add_filter: <Filter className="h-3.5 w-3.5" />,
};

function getStepIcon(stepName: string) {
  return STEP_ICONS[stepName] ?? <Search className="h-3.5 w-3.5" />;
}

function getStatusBadgeVariant(
  status?: string
): "default" | "secondary" | "destructive" | "outline" {
  switch (status) {
    case "success":
      return "secondary";
    case "error":
      return "destructive";
    case "skipped":
      return "outline";
    default:
      return "default";
  }
}

function StepItem({
  step,
  defaultOpen = false,
}: {
  step: AgentStep;
  defaultOpen?: boolean;
}) {
  const [open, setOpen] = useState(defaultOpen);
  const hasDetails = step.details && Object.keys(step.details).length > 0;

  return (
    <div className="flex gap-2">
      {/* Connector line + icon */}
      <div className="flex flex-col items-center">
        <div className="flex h-6 w-6 items-center justify-center rounded-full border bg-background">
          {getStepIcon(step.name ?? "")}
        </div>
        <div className="w-px flex-1 bg-border" />
      </div>

      {/* Step content */}
      <div className="flex-1 pb-3 min-w-0">
        <div className="flex items-center gap-2 flex-wrap">
          <Badge variant="outline" className="text-xs font-mono">
            {step.name}
          </Badge>
          {step.status && (
            <Badge variant={getStatusBadgeVariant(step.status)} className="text-xs">
              {step.status}
            </Badge>
          )}
          {step.duration_ms !== undefined && (
            <span className="text-xs text-muted-foreground">{step.duration_ms}ms</span>
          )}
        </div>
        {step.action && (
          <p className="text-xs text-muted-foreground mt-1 truncate">{step.action}</p>
        )}
        {step.error_message && (
          <p className="text-xs text-destructive mt-1">{step.error_message}</p>
        )}
        {hasDetails && (
          <button
            type="button"
            onClick={() => setOpen(!open)}
            className="text-xs text-muted-foreground mt-1 hover:text-foreground flex items-center gap-1"
          >
            {open ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
            Details
          </button>
        )}
        {open && hasDetails && (
          <pre className="text-xs bg-muted p-2 rounded mt-1 overflow-auto max-h-40">
            {JSON.stringify(step.details, null, 2)}
          </pre>
        )}
      </div>
    </div>
  );
}

function ActiveStepItem({ step }: { step: SSEStepStarted }) {
  return (
    <div className="flex gap-2">
      <div className="flex flex-col items-center">
        <div className="flex h-6 w-6 items-center justify-center rounded-full border bg-background">
          <Loader2 className="h-3.5 w-3.5 animate-spin text-muted-foreground" />
        </div>
        <div className="w-px flex-1 bg-border" />
      </div>
      <div className="flex-1 pb-3 min-w-0">
        <div className="flex items-center gap-2">
          <Badge variant="outline" className="text-xs font-mono">
            {step.name}
          </Badge>
        </div>
        {step.action && (
          <p className="text-xs text-muted-foreground mt-1 truncate">{step.action}</p>
        )}
      </div>
    </div>
  );
}

export function ReasoningChainCollapsible({
  chain,
  activeSteps = [],
  toolCallsMade = 0,
  reasoningText,
  isStreaming,
}: ReasoningChainCollapsibleProps) {
  const totalSteps = chain.length + activeSteps.length;
  if (totalSteps === 0 && !reasoningText) return null;

  const label = isStreaming
    ? `Reasoning... (${chain.length} step${chain.length !== 1 ? "s" : ""})`
    : `${toolCallsMade || chain.length} tool call${(toolCallsMade || chain.length) !== 1 ? "s" : ""}`;

  return (
    <Collapsible>
      <CollapsibleTrigger asChild>
        <button
          type="button"
          className="flex items-center gap-2 text-xs text-muted-foreground hover:text-foreground py-1 group"
        >
          <ChevronRight className="h-3 w-3 transition-transform group-data-[state=open]:rotate-90" />
          {isStreaming && <Loader2 className="h-3 w-3 animate-spin" />}
          <span>{label}</span>
        </button>
      </CollapsibleTrigger>
      <CollapsibleContent>
        <div className="mt-2 ml-1 space-y-0">
          {chain.map((step) => (
            <StepItem key={step.id || `${step.name}-${step.action}`} step={step} />
          ))}
          {activeSteps.map((step) => (
            <ActiveStepItem key={step.id} step={step} />
          ))}
          {reasoningText && (
            <div className="mt-2 p-2 bg-muted/50 rounded text-xs text-muted-foreground whitespace-pre-wrap">
              {reasoningText}
            </div>
          )}
        </div>
      </CollapsibleContent>
    </Collapsible>
  );
}
