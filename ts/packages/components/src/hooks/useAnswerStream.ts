import type { QueryHit, RetrievalAgentRequest } from "@antfly/sdk";
import { useCallback, useEffect, useRef, useState } from "react";
import { streamAnswer } from "../utils";

/**
 * Classification data from Answer Agent
 */
export interface QueryClassification {
  route_type: "question" | "search";
  improved_query: string;
  semantic_query: string;
  confidence: number;
}

/**
 * Hook for streaming Answer Agent responses with state management.
 *
 * Manages streaming answer text, reasoning, classification, hits, and follow-up questions
 * from the Antfly Answer Agent endpoint.
 *
 * @returns Object with answer state and streaming controls
 *
 * @example
 * ```typescript
 * const {
 *   answer,
 *   reasoning,
 *   classification,
 *   hits,
 *   followUpQuestions,
 *   isStreaming,
 *   error,
 *   startStream,
 *   stopStream,
 *   reset
 * } = useAnswerStream();
 *
 * // Start streaming
 * startStream({
 *   url: 'http://localhost:8080/db/v1',
 *   request: {
 *     query: 'how does raft work',
 *     tables: ['docs']
 *   },
 *   headers: { 'X-API-Key': 'key' }
 * });
 *
 * // Stop streaming
 * stopStream();
 *
 * // Reset state
 * reset();
 * ```
 */
export function useAnswerStream() {
  const [answer, setAnswer] = useState<string>("");
  const [reasoning, setReasoning] = useState<string>("");
  const [classification, setClassification] = useState<QueryClassification | null>(null);
  const [hits, setHits] = useState<QueryHit[]>([]);
  const [followUpQuestions, setFollowUpQuestions] = useState<string[]>([]);
  const [isStreaming, setIsStreaming] = useState<boolean>(false);
  const [error, setError] = useState<Error | null>(null);

  const abortControllerRef = useRef<AbortController | null>(null);

  useEffect(() => () => abortControllerRef.current?.abort(), []);

  /**
   * Start streaming an answer
   */
  const startStream = useCallback(
    async ({
      url,
      request,
      headers = {},
    }: {
      url: string;
      request: RetrievalAgentRequest;
      headers?: Record<string, string>;
    }) => {
      // Reset state
      setAnswer("");
      setReasoning("");
      setClassification(null);
      setHits([]);
      setFollowUpQuestions([]);
      setError(null);
      setIsStreaming(true);

      // Abort any existing stream
      if (abortControllerRef.current) {
        abortControllerRef.current.abort();
      }

      const requestController = new AbortController();
      abortControllerRef.current = requestController;

      try {
        const streamController = await streamAnswer(
          url,
          request,
          headers,
          {
            onClassification: (data) => {
              if (!requestController.signal.aborted) setClassification(data);
            },
            onHit: (hit) => {
              if (!requestController.signal.aborted) setHits((prev) => [...prev, hit]);
            },
            onReasoning: (chunk) => {
              if (!requestController.signal.aborted) setReasoning((prev) => prev + chunk);
            },
            onGeneration: (chunk) => {
              if (!requestController.signal.aborted) setAnswer((prev) => prev + chunk);
            },
            onFollowup: (question) => {
              if (!requestController.signal.aborted) {
                setFollowUpQuestions((prev) => [...prev, question]);
              }
            },
            onRetrievalAgentResult: (result) => {
              if (requestController.signal.aborted) return;
              if (result.generation !== undefined) setAnswer(result.generation);
              if (result.hits !== undefined) setHits(result.hits);
              if (result.classification !== undefined) {
                setClassification(result.classification ?? null);
              }
              if (result.followup_questions !== undefined) {
                setFollowUpQuestions(result.followup_questions);
              }
            },
            onComplete: () => {
              if (!requestController.signal.aborted) setIsStreaming(false);
            },
            onError: (err) => {
              if (requestController.signal.aborted) return;
              const errorObj = err instanceof Error ? err : new Error(String(err));
              setError(errorObj);
              setIsStreaming(false);
            },
          },
          { signal: requestController.signal }
        );
        // Keep the adapter's returned controller linked for compatibility with
        // custom streamAnswer implementations while retaining pre-header
        // cancellation through requestController.
        if (requestController.signal.aborted) streamController.abort();
        else {
          requestController.signal.addEventListener("abort", () => streamController.abort(), {
            once: true,
          });
        }
      } catch (err) {
        if (requestController.signal.aborted) return;
        const errorObj = err instanceof Error ? err : new Error(String(err));
        setError(errorObj);
        setIsStreaming(false);
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
    setIsStreaming(false);
  }, []);

  /**
   * Reset all state
   */
  const reset = useCallback(() => {
    stopStream();
    setAnswer("");
    setReasoning("");
    setClassification(null);
    setHits([]);
    setFollowUpQuestions([]);
    setError(null);
  }, [stopStream]);

  return {
    answer,
    reasoning,
    classification,
    hits,
    followUpQuestions,
    isStreaming,
    error,
    startStream,
    stopStream,
    reset,
  };
}
