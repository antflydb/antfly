import type {
  ChatMessage,
  ChatToolsConfig,
  ClarificationRequest,
  ClassificationTransformationResult,
  FilterSpec,
  GenerationConfidence,
  GeneratorConfig,
  QueryHit,
  RetrievalAgentRequest,
  RetrievalAgentSteps,
  RetrievalReasoningStep,
} from "@antfly/sdk";
import { useCallback, useRef, useState } from "react";
import { streamAnswer } from "../utils";

/** Per-turn state for a chat conversation */
export interface ChatTurn {
  /** Unique identifier for this turn */
  id: string;
  /** The user's message for this turn */
  userMessage: string;
  /** Accumulated assistant response text */
  assistantMessage: string;
  /** Search hits retrieved for this turn */
  hits: QueryHit[];
  /** Follow-up questions suggested by the agent */
  followUpQuestions: string[];
  /** Query classification data */
  classification: ClassificationTransformationResult | null;
  /** Generation confidence assessment */
  confidence: GenerationConfidence | null;
  /** Clarification request from the agent (if awaiting user input) */
  clarification: ClarificationRequest | null;
  /** Filters applied during retrieval */
  appliedFilters: FilterSpec[];
  /** Accumulated reasoning text */
  reasoningText: string;
  /** Completed reasoning steps from the agent */
  reasoningChain: RetrievalReasoningStep[];
  /** Steps currently in progress (during streaming) */
  activeSteps: { id: string; step: string; action: string }[];
  /** Number of tool calls made by the agent */
  toolCallsMade: number;
  /** Error for this turn */
  error: string | null;
  /** Whether this turn is currently streaming */
  isStreaming: boolean;
}

/** Configuration for the chat session */
export interface ChatConfig {
  url: string;
  headers?: Record<string, string>;
  generator: GeneratorConfig;
  table: string;
  semanticIndexes?: string[];
  agentKnowledge?: string;
  systemPrompt?: string;
  maxIterations?: number;
  followUpCount?: number;
  limit?: number;
  steps?: RetrievalAgentSteps;
  tools?: ChatToolsConfig;
  fields?: string[];
  filterQuery?: Record<string, unknown>;
  exclusionQuery?: Record<string, unknown>;
}

/**
 * Hook for managing multi-turn chat conversations with the Antfly Retrieval Agent.
 *
 * Each call to `sendMessage` creates a new turn with its own streaming state.
 * Full conversation history is passed to the agent for context.
 */
export function useChatStream() {
  const [turns, setTurns] = useState<ChatTurn[]>([]);
  const [isStreaming, setIsStreaming] = useState(false);
  const abortControllerRef = useRef<AbortController | null>(null);
  const turnsRef = useRef<ChatTurn[]>([]);
  const nextTurnIdRef = useRef(0);

  // Keep ref in sync with state
  turnsRef.current = turns;

  const sendMessage = useCallback(async (text: string, config: ChatConfig) => {
    // Abort any existing stream
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
    }

    // Build messages history from completed turns
    const messages: ChatMessage[] = [];
    const currentTurns = turnsRef.current;

    for (const turn of currentTurns) {
      messages.push({ role: "user", content: turn.userMessage });
      if (turn.assistantMessage) {
        messages.push({ role: "assistant", content: turn.assistantMessage });
      }
    }

    // Create new turn
    const newTurn: ChatTurn = {
      id: `turn-${++nextTurnIdRef.current}`,
      userMessage: text,
      assistantMessage: "",
      hits: [],
      followUpQuestions: [],
      classification: null,
      confidence: null,
      clarification: null,
      appliedFilters: [],
      reasoningText: "",
      reasoningChain: [],
      activeSteps: [],
      toolCallsMade: 0,
      error: null,
      isStreaming: true,
    };

    const turnId = newTurn.id;
    setTurns((prev) => [...prev, newTurn]);
    setIsStreaming(true);

    const updateTurn = (updater: (turn: ChatTurn) => ChatTurn) => {
      setTurns((prev) => prev.map((t) => (t.id === turnId ? updater(t) : t)));
    };

    // Build retrieval agent request
    const request: RetrievalAgentRequest = {
      query: text,
      generator: config.generator,
      agent_knowledge: config.agentKnowledge,
      stream: true,
      messages,
      queries: [
        {
          table: config.table,
          semantic_search: text,
          ...(config.semanticIndexes?.length ? { indexes: config.semanticIndexes } : {}),
          ...(config.fields?.length ? { fields: config.fields } : {}),
          ...(config.filterQuery ? { filter_query: config.filterQuery } : {}),
          ...(config.exclusionQuery ? { exclusion_query: config.exclusionQuery } : {}),
          limit: config.limit ?? 10,
        },
      ],
      steps: config.steps ?? {
        generation: {
          enabled: true,
          ...(config.systemPrompt ? { system_prompt: config.systemPrompt } : {}),
        },
        classification: { enabled: true },
        followup: {
          enabled: true,
          ...(config.followUpCount ? { count: config.followUpCount } : {}),
        },
        confidence: { enabled: true },
        ...(config.tools ? { tools: config.tools } : {}),
      },
      ...(config.maxIterations ? { max_iterations: config.maxIterations } : {}),
    };

    try {
      const controller = await streamAnswer(config.url, request, config.headers || {}, {
        onClassification: (data) => {
          updateTurn((t) => ({ ...t, classification: data }));
        },
        onHit: (hit) => {
          updateTurn((t) => ({ ...t, hits: [...t.hits, hit] }));
        },
        onReasoning: (chunk) => {
          updateTurn((t) => ({ ...t, reasoningText: t.reasoningText + chunk }));
        },
        onStepStarted: (step) => {
          updateTurn((t) => ({ ...t, activeSteps: [...t.activeSteps, step] }));
        },
        onStepCompleted: (step) => {
          updateTurn((t) => ({
            ...t,
            reasoningChain: [...t.reasoningChain, step],
            activeSteps: t.activeSteps.filter((s) => s.id !== step.id),
            toolCallsMade: t.toolCallsMade + 1,
          }));
        },
        onGeneration: (chunk) => {
          updateTurn((t) => ({ ...t, assistantMessage: t.assistantMessage + chunk }));
        },
        onConfidence: (data) => {
          updateTurn((t) => ({ ...t, confidence: data }));
        },
        onFollowup: (question) => {
          updateTurn((t) => ({ ...t, followUpQuestions: [...t.followUpQuestions, question] }));
        },
        onComplete: () => {
          updateTurn((t) => ({ ...t, isStreaming: false }));
          setIsStreaming(false);
        },
        onError: (err) => {
          const message = err instanceof Error ? err.message : String(err);
          updateTurn((t) => ({ ...t, error: message, isStreaming: false }));
          setIsStreaming(false);
        },
        onRetrievalAgentResult: (result) => {
          updateTurn((t) => ({
            ...t,
            assistantMessage: result.generation || "",
            hits: result.hits || [],
            followUpQuestions: result.followup_questions || [],
            classification: result.classification || null,
            clarification: result.clarification_request || null,
            appliedFilters: result.applied_filters || [],
            reasoningChain: result.reasoning_chain || t.reasoningChain,
            toolCallsMade: result.tool_calls_made ?? t.toolCallsMade,
            activeSteps: [],
            confidence:
              result.generation_confidence !== undefined && result.context_relevance !== undefined
                ? {
                    generation_confidence: result.generation_confidence,
                    context_relevance: result.context_relevance,
                  }
                : null,
            isStreaming: false,
          }));
          setIsStreaming(false);
        },
      });

      abortControllerRef.current = controller;
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      updateTurn((t) => ({ ...t, error: message, isStreaming: false }));
      setIsStreaming(false);
    }
  }, []);

  const abort = useCallback(() => {
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
      abortControllerRef.current = null;
    }
    setTurns((prev) => prev.map((t) => (t.isStreaming ? { ...t, isStreaming: false } : t)));
    setIsStreaming(false);
  }, []);

  const reset = useCallback(() => {
    abort();
    setTurns([]);
  }, [abort]);

  return {
    turns,
    isStreaming,
    sendMessage,
    abort,
    reset,
  };
}
