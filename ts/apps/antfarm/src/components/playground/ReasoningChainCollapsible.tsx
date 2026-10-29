import {
  HighlightedCodeBlock,
  Spinner,
  Trace,
  TraceCollapsible,
  TraceCollapsibleContent,
  TraceCollapsibleTrigger,
  TraceDescription,
  TraceIcon,
  TraceLine,
  TraceTitle,
} from "@antfly/design-system";
import type { AgentStep, SSEStepStarted } from "@antfly/sdk";
import { Filter, Globe, Search, TreePine, Waypoints } from "lucide-react";
import type React from "react";

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
  web_search: <Globe className="h-3.5 w-3.5" />,
  fetch: <Globe className="h-3.5 w-3.5" />,
  add_filter: <Filter className="h-3.5 w-3.5" />,
};

function getStepIcon(stepName: string) {
  return STEP_ICONS[stepName] ?? <Search className="h-3.5 w-3.5" />;
}

function StepLine({ step }: { step: AgentStep }) {
  return (
    <TraceLine status={step.status === "error" ? "error" : undefined}>
      <TraceIcon>{getStepIcon(step.name ?? "")}</TraceIcon>
      <TraceTitle>
        <span className="font-mono">{step.name}</span>
        {step.action ? ` ${step.action}` : null}
      </TraceTitle>
      {step.status && <TraceDescription>{step.status}</TraceDescription>}
      {step.duration_ms !== undefined && <TraceDescription>{step.duration_ms}ms</TraceDescription>}
    </TraceLine>
  );
}

function StepItem({ step, defaultOpen = false }: { step: AgentStep; defaultOpen?: boolean }) {
  const hasDetails = step.details && Object.keys(step.details).length > 0;
  const errorMessage = step.error_message ? (
    <p className="ml-6 text-xs text-destructive">{step.error_message}</p>
  ) : null;

  if (!hasDetails) {
    return (
      <>
        <StepLine step={step} />
        {errorMessage}
      </>
    );
  }

  return (
    <>
      <TraceCollapsible defaultOpen={defaultOpen}>
        <TraceCollapsibleTrigger>
          <StepLine step={step} />
        </TraceCollapsibleTrigger>
        <TraceCollapsibleContent>
          <HighlightedCodeBlock
            code={JSON.stringify(step.details, null, 2)}
            language="json"
            size="sm"
            contentClassName="max-h-40"
          />
        </TraceCollapsibleContent>
      </TraceCollapsible>
      {errorMessage}
    </>
  );
}

function ActiveStepItem({ step }: { step: SSEStepStarted }) {
  return (
    <TraceLine status="running">
      <TraceIcon>
        <Spinner className="h-3.5 w-3.5" />
      </TraceIcon>
      <TraceTitle>
        <span className="font-mono">{step.name}</span>
        {step.action ? ` ${step.action}` : null}
      </TraceTitle>
    </TraceLine>
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
    <TraceCollapsible>
      <TraceCollapsibleTrigger className="py-1">
        <TraceLine status={isStreaming ? "running" : undefined}>
          {isStreaming && (
            <TraceIcon>
              <Spinner className="h-3 w-3" />
            </TraceIcon>
          )}
          <TraceTitle>{label}</TraceTitle>
        </TraceLine>
      </TraceCollapsibleTrigger>
      <TraceCollapsibleContent>
        <Trace>
          {chain.map((step) => (
            <StepItem key={step.id || `${step.name}-${step.action}`} step={step} />
          ))}
          {activeSteps.map((step) => (
            <ActiveStepItem key={step.id} step={step} />
          ))}
          {reasoningText && (
            <div className="mt-2 p-2 bg-muted/50 rounded-md text-xs text-muted-foreground whitespace-pre-wrap">
              {reasoningText}
            </div>
          )}
        </Trace>
      </TraceCollapsibleContent>
    </TraceCollapsible>
  );
}
