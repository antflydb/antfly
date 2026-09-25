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
  // Identifies the current run. Stop, reset and every new start advance it,
  // so a run that is still connecting can never deliver callbacks or install
  // its controller afterwards.
  const runRef = useRef(0);

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
        abortControllerRef.current = null;
      }
      runRef.current += 1;
      const runId = runRef.current;
      // Own the controller before connecting so stopStream can abort the
      // request while it is still waiting for response headers.
      const runController = new AbortController();
      abortControllerRef.current = runController;
      const isCurrent = () => runRef.current === runId;
      const update: typeof setState = (next) => {
        if (isCurrent()) setState(next);
      };

      // Reset state
      setState({ ...initialState, status: "streaming" });

      try {
        const controller = await streamResearch(
          url,
          request,
          headers,
          {
            onStepStarted: (step) => {
              update((prev) => ({ ...prev, activeSteps: [...prev.activeSteps, step] }));
            },
            onStepCompleted: (step) => {
              update((prev) => ({
                ...prev,
                steps: [...prev.steps, step],
                activeSteps: prev.activeSteps.filter((s) => s.id !== step.id),
              }));
            },
            onPlan: (plan) => {
              update((prev) => ({ ...prev, plan }));
            },
            onSubQuestionStarted: (event) => {
              update((prev) => ({
                ...prev,
                subQuestionProgress: [...prev.subQuestionProgress, event],
              }));
            },
            onFinding: (finding) => {
              update((prev) => ({ ...prev, findings: [...prev.findings, finding] }));
            },
            onReflection: (reflection) => {
              update((prev) => ({ ...prev, reflections: [...prev.reflections, reflection] }));
            },
            onSection: (section) => {
              update((prev) => ({ ...prev, sections: [...prev.sections, section] }));
            },
            onVerification: (verification) => {
              update((prev) => ({ ...prev, verification }));
            },
            onGeneration: (chunk) => {
              update((prev) => ({ ...prev, reportMarkdown: prev.reportMarkdown + chunk }));
            },
            onResearchAgentResult: (result) => {
              update((prev) => ({
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
              update((prev) => ({ ...prev, status: "done" }));
            },
            onError: (err) => {
              const errorObj = err instanceof Error ? err : new Error(String(err));
              update((prev) => ({ ...prev, status: "error", error: errorObj }));
            },
          },
          runController.signal
        );

        // Stopped or superseded while connecting: end this run now.
        if (!isCurrent()) {
          controller.abort();
          return;
        }
        // stopStream aborts runController; forward that to the controller
        // streamResearch returned, which is not always linked to the signal
        // (the non-streaming path returns a fresh one).
        runController.signal.addEventListener("abort", () => controller.abort(), { once: true });
      } catch (err) {
        const errorObj = err instanceof Error ? err : new Error(String(err));
        update((prev) => ({ ...prev, status: "error", error: errorObj }));
      }
    },
    []
  );

  /**
   * Stop the current stream
   */
  const stopStream = useCallback(() => {
    runRef.current += 1;
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
