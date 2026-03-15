import type { QueryHit, RetrievalAgentRequest } from "@antfly/sdk";
import { useCallback, useRef, useState } from "react";
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
 *   url: 'http://localhost:8080/api/v1',
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

      try {
        const controller = await streamAnswer(url, request, headers, {
          onClassification: (data) => {
            setClassification(data);
          },
          onHit: (hit) => {
            setHits((prev) => [...prev, hit]);
          },
          onReasoning: (chunk) => {
            setReasoning((prev) => prev + chunk);
          },
          onGeneration: (chunk) => {
            setAnswer((prev) => prev + chunk);
          },
          onFollowup: (question) => {
            setFollowUpQuestions((prev) => [...prev, question]);
          },
          onComplete: () => {
            setIsStreaming(false);
          },
          onError: (err) => {
            const errorObj = err instanceof Error ? err : new Error(String(err));
            setError(errorObj);
            setIsStreaming(false);
          },
        });

        abortControllerRef.current = controller;
      } catch (err) {
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
