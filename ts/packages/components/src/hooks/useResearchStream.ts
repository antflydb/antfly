import type {
  AgentStep,
  ResearchAgentRequest,
  ResearchAgentResult,
  ResearchFinding,
  ResearchPlan,
  ResearchReflection,
  ResearchSectionProgress,
  ResearchSubQuestionStartedProgress,
  ResearchVerification,
  SSEStepStarted,
} from "@antfly/sdk";
import { useCallback, useRef, useState } from "react";
import { streamResearch } from "../utils";

/** Streaming lifecycle status for a research run. */
export type ResearchStreamStatus = "idle" | "streaming" | "done" | "error";

/** State exposed by useResearchStream. */
export interface ResearchStreamState {
  status: ResearchStreamStatus;
  /** The research brief and sub-questions, available once the planner completes. */
  plan: ResearchPlan | null;
  /** Sub-questions as researchers are dispatched, in dispatch order. */
  subQuestionProgress: ResearchSubQuestionStartedProgress[];
  /** Compressed researcher findings as each researcher completes. */
  findings: ResearchFinding[];
  /** Gap-analysis reflections after each research round. */
  reflections: ResearchReflection[];
  /** Report section headings as the writer produces them. */
  sections: ResearchSectionProgress[];
  /** Streamed report markdown, accumulated chunk by chunk. */
  reportMarkdown: string;
  /** Citation verification summary, when the verify step runs. */
  verification: ResearchVerification | null;
  /** Execution steps currently in progress. */
  activeSteps: SSEStepStarted[];
  /** Completed execution steps. */
  steps: AgentStep[];
  /** The authoritative result once the run reaches `done`. */
  result: ResearchAgentResult | null;
  error: Error | null;
}

const initialState: ResearchStreamState = {
  status: "idle",
  plan: null,
  subQuestionProgress: [],
  findings: [],
  reflections: [],
  sections: [],
  reportMarkdown: "",
  verification: null,
  activeSteps: [],
  steps: [],
  result: null,
  error: null,
};

/**
 * Hook for streaming Research Agent responses with state management.
 *
 * Manages the research plan, per-sub-question progress, findings, reflections,
 * streamed report markdown and final cited result from the Antfly Research
 * Agent endpoint (`/agents/research`).
 *
 * @returns Object with research state and streaming controls
 *
 * @example
 * ```typescript
 * const {
 *   plan,
 *   findings,
 *   reportMarkdown,
 *   result,
 *   status,
 *   error,
 *   startStream,
 *   stopStream,
 *   reset
 * } = useResearchStream();
 *
 * startStream({
 *   url: 'http://localhost:8080/db/v1',
 *   request: {
 *     query: 'How do hybrid search and reranking interact?',
 *     queries: [{ table: 'docs' }],
 *   },
 * });
 * ```
 */
export function useResearchStream() {
  const [state, setState] = useState<ResearchStreamState>(initialState);
  const abortControllerRef = useRef<AbortController | null>(null);

  const startStream = useCallback(
    async ({
      url,
      request,
      headers = {},
    }: {
      url: string;
      request: ResearchAgentRequest;
      headers?: Record<string, string>;
    }) => {
      // Abort any existing stream
      if (abortControllerRef.current) {
        abortControllerRef.current.abort();
      }

      // Reset state
      setState({ ...initialState, status: "streaming" });

      try {
        const controller = await streamResearch(url, request, headers, {
          onStepStarted: (step) => {
            setState((prev) => ({ ...prev, activeSteps: [...prev.activeSteps, step] }));
          },
          onStepCompleted: (step) => {
            setState((prev) => ({
              ...prev,
              steps: [...prev.steps, step],
              activeSteps: prev.activeSteps.filter((s) => s.id !== step.id),
            }));
          },
          onPlan: (plan) => {
            setState((prev) => ({ ...prev, plan }));
          },
          onSubQuestionStarted: (event) => {
            setState((prev) => ({
              ...prev,
              subQuestionProgress: [...prev.subQuestionProgress, event],
            }));
          },
          onFinding: (finding) => {
            setState((prev) => ({ ...prev, findings: [...prev.findings, finding] }));
          },
          onReflection: (reflection) => {
            setState((prev) => ({ ...prev, reflections: [...prev.reflections, reflection] }));
          },
          onSection: (section) => {
            setState((prev) => ({ ...prev, sections: [...prev.sections, section] }));
          },
          onVerification: (verification) => {
            setState((prev) => ({ ...prev, verification }));
          },
          onGeneration: (chunk) => {
            setState((prev) => ({ ...prev, reportMarkdown: prev.reportMarkdown + chunk }));
          },
          onResearchAgentResult: (result) => {
            setState((prev) => ({
              ...prev,
              result,
              plan: result.plan ?? prev.plan,
              findings: result.findings ?? prev.findings,
              reflections: result.reflections ?? prev.reflections,
              verification: result.verification ?? prev.verification,
              reportMarkdown: result.report?.markdown ?? prev.reportMarkdown,
              steps: result.steps ?? prev.steps,
              activeSteps: [],
            }));
          },
          onComplete: () => {
            setState((prev) => ({ ...prev, status: "done" }));
          },
          onError: (err) => {
            const errorObj = err instanceof Error ? err : new Error(String(err));
            setState((prev) => ({ ...prev, status: "error", error: errorObj }));
          },
        });

        abortControllerRef.current = controller;
      } catch (err) {
        const errorObj = err instanceof Error ? err : new Error(String(err));
        setState((prev) => ({ ...prev, status: "error", error: errorObj }));
      }
    },
    []
  );

  /**
   * Stop the current stream
   */
  const stopStream = useCallback(() => {
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
      abortControllerRef.current = null;
    }
    setState((prev) => (prev.status === "streaming" ? { ...prev, status: "done" } : prev));
  }, []);

  /**
   * Reset all state
   */
  const reset = useCallback(() => {
    stopStream();
    setState(initialState);
  }, [stopStream]);

  return {
    ...state,
    startStream,
    stopStream,
    reset,
  };
}
