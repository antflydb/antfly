import type { EvalResult, GenerationConfidence, QueryHit, RetrievalAgentResult } from "@antfly/sdk";
import { createContext, useContext } from "react";

// Context for sharing Answer Agent data with child components (e.g., AnswerFeedback)
export interface AnswerResultsContextValue {
  query: string;
  agentKnowledge?: string;
  classification: {
    route_type: "question" | "search";
    improved_query: string;
    semantic_query: string;
    confidence: number;
  } | null;
  hits: QueryHit[];
  reasoning: string;
  answer: string;
  followUpQuestions: string[];
  isStreaming: boolean;
  result: RetrievalAgentResult | null;
  confidence: GenerationConfidence | null;
  evalResult: EvalResult | null;
}

export const AnswerResultsContext = createContext<AnswerResultsContextValue | null>(null);

export function useAnswerResultsContext() {
  const context = useContext(AnswerResultsContext);
  if (!context) {
    throw new Error("useAnswerResultsContext must be used within an AnswerResults component");
  }
  return context;
}
