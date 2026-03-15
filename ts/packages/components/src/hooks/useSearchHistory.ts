import type { QueryHit } from "@antfly/sdk";
import { useCallback, useState } from "react";

/**
 * Citation metadata from RAG/Answer Agent results
 */
export interface CitationMetadata {
  id: string;
  quote?: string;
  score?: number;
}

/**
 * Represents a single search result in history
 */
export interface SearchResult {
  query: string;
  timestamp: number;
  summary: string;
  hits: QueryHit[];
  citations?: CitationMetadata[];
}

/**
 * Structure of search history in localStorage
 */
export interface SearchHistory {
  results: SearchResult[];
}

/**
 * Hook to manage search history with localStorage persistence.
 *
 * Stores search queries, summaries, hits, and citations in browser localStorage
 * with a configurable maximum number of results.
 *
 * @param maxResults - Maximum number of search results to store (default: 10, 0 to disable)
 * @returns Object with history state and management functions
 *
 * @example
 * ```typescript
 * const { history, isReady, saveSearch, clearHistory } = useSearchHistory(10);
 *
 * // Save a search result
 * saveSearch({
 *   query: "how does raft work",
 *   timestamp: Date.now(),
 *   summary: "Raft is a consensus algorithm...",
 *   hits: [...],
 *   citations: [{ id: "doc1", score: 0.95 }]
 * });
 *
 * // Clear all history
 * clearHistory();
 * ```
 */
export function useSearchHistory(maxResults = 10) {
  // Load from localStorage on initialization
  const [history, setHistory] = useState<SearchResult[]>(() => {
    if (typeof window === "undefined") {
      return [];
    }
    try {
      const stored = localStorage.getItem("antfly-search-history");
      if (stored) {
        const parsed: SearchHistory = JSON.parse(stored);
        return parsed.results || [];
      }
    } catch (error) {
      console.warn("Failed to load search history:", error);
    }
    return [];
  });

  // History is loaded synchronously via lazy initializer, so always ready
  const isReady = true;

  // Save search result
  const saveSearch = useCallback(
    (result: SearchResult) => {
      if (maxResults === 0) return; // History disabled

      setHistory((prev) => {
        const updated = [result, ...prev].slice(0, maxResults);

        if (typeof window === "undefined") return updated;

        try {
          localStorage.setItem("antfly-search-history", JSON.stringify({ results: updated }));
        } catch (error) {
          console.warn("Failed to save search history:", error);
        }
        return updated;
      });
    },
    [maxResults]
  );

  // Clear history
  const clearHistory = useCallback(() => {
    setHistory([]);

    if (typeof window === "undefined") return;

    try {
      localStorage.removeItem("antfly-search-history");
    } catch (error) {
      console.warn("Failed to clear search history:", error);
    }
  }, []);

  return { history, isReady, saveSearch, clearHistory };
}
