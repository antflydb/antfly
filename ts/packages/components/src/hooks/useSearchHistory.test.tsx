import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import type { SearchResult } from "./useSearchHistory";
import { useSearchHistory } from "./useSearchHistory";

describe("useSearchHistory", () => {
  const STORAGE_KEY = "antfly-search-history";

  // Clear localStorage before each test
  beforeEach(() => {
    localStorage.clear();
  });

  afterEach(() => {
    localStorage.clear();
  });

  it("should initialize with empty history", () => {
    const { result } = renderHook(() => useSearchHistory(10));

    expect(result.current.history).toEqual([]);
    expect(result.current.isReady).toBe(true);
  });

  it("should load existing history from localStorage", () => {
    const existingHistory: SearchResult[] = [
      {
        query: "test query",
        timestamp: Date.now(),
        summary: "test summary",
        hits: [],
      },
    ];

    localStorage.setItem(STORAGE_KEY, JSON.stringify({ results: existingHistory }));

    const { result } = renderHook(() => useSearchHistory(10));

    expect(result.current.history).toEqual(existingHistory);
  });

  it("should save search result to history", () => {
    const { result } = renderHook(() => useSearchHistory(10));

    const searchResult: SearchResult = {
      query: "how does raft work",
      timestamp: Date.now(),
      summary: "Raft is a consensus algorithm",
      hits: [],
      citations: [{ id: "doc1", score: 0.95 }],
    };

    act(() => {
      result.current.saveSearch(searchResult);
    });

    expect(result.current.history).toHaveLength(1);
    expect(result.current.history[0]).toEqual(searchResult);

    // Verify localStorage was updated
    const stored = JSON.parse(localStorage.getItem(STORAGE_KEY) ?? "{}");
    expect(stored.results).toEqual([searchResult]);
  });

  it("should add new results to the beginning of history", () => {
    const { result } = renderHook(() => useSearchHistory(10));

    const firstResult: SearchResult = {
      query: "first",
      timestamp: 1,
      summary: "first summary",
      hits: [],
    };

    const secondResult: SearchResult = {
      query: "second",
      timestamp: 2,
      summary: "second summary",
      hits: [],
    };

    act(() => {
      result.current.saveSearch(firstResult);
    });

    act(() => {
      result.current.saveSearch(secondResult);
    });

    expect(result.current.history).toHaveLength(2);
    expect(result.current.history[0]).toEqual(secondResult);
    expect(result.current.history[1]).toEqual(firstResult);
  });

  it("should respect maxResults limit", () => {
    const { result } = renderHook(() => useSearchHistory(3));

    const results: SearchResult[] = [];
    for (let i = 0; i < 5; i++) {
      results.push({
        query: `query ${i}`,
        timestamp: i,
        summary: `summary ${i}`,
        hits: [],
      });
    }

    act(() => {
      for (const r of results) {
        result.current.saveSearch(r);
      }
    });

    // Should only keep the last 3 results
    expect(result.current.history).toHaveLength(3);
    expect(result.current.history[0].query).toBe("query 4");
    expect(result.current.history[1].query).toBe("query 3");
    expect(result.current.history[2].query).toBe("query 2");
  });

  it("should clear history", () => {
    const { result } = renderHook(() => useSearchHistory(10));

    const searchResult: SearchResult = {
      query: "test",
      timestamp: Date.now(),
      summary: "test",
      hits: [],
    };

    act(() => {
      result.current.saveSearch(searchResult);
    });

    expect(result.current.history).toHaveLength(1);

    act(() => {
      result.current.clearHistory();
    });

    expect(result.current.history).toEqual([]);
    expect(localStorage.getItem(STORAGE_KEY)).toBeNull();
  });

  it("should disable history when maxResults is 0", () => {
    const { result } = renderHook(() => useSearchHistory(0));

    const searchResult: SearchResult = {
      query: "test",
      timestamp: Date.now(),
      summary: "test",
      hits: [],
    };

    act(() => {
      result.current.saveSearch(searchResult);
    });

    // History should remain empty
    expect(result.current.history).toEqual([]);
    expect(localStorage.getItem(STORAGE_KEY)).toBeNull();
  });

  it("should handle corrupt localStorage data gracefully", () => {
    localStorage.setItem(STORAGE_KEY, "invalid json{");

    const { result } = renderHook(() => useSearchHistory(10));

    // Should initialize with empty history instead of crashing
    expect(result.current.history).toEqual([]);
    expect(result.current.isReady).toBe(true);
  });

  it("should handle localStorage quota exceeded gracefully", () => {
    const { result } = renderHook(() => useSearchHistory(10));

    // Mock localStorage.setItem to throw quota exceeded error
    const originalSetItem = Storage.prototype.setItem;
    Storage.prototype.setItem = () => {
      throw new DOMException("QuotaExceededError");
    };

    const searchResult: SearchResult = {
      query: "test",
      timestamp: Date.now(),
      summary: "test",
      hits: [],
    };

    // Should not throw error
    act(() => {
      result.current.saveSearch(searchResult);
    });

    // State should still update
    expect(result.current.history).toHaveLength(1);

    // Restore original setItem
    Storage.prototype.setItem = originalSetItem;
  });

  it("should update maxResults dynamically", () => {
    const { result, rerender } = renderHook(({ max }) => useSearchHistory(max), {
      initialProps: { max: 5 },
    });

    // Add 5 results
    act(() => {
      for (let i = 0; i < 5; i++) {
        result.current.saveSearch({
          query: `query ${i}`,
          timestamp: i,
          summary: `summary ${i}`,
          hits: [],
        });
      }
    });

    expect(result.current.history).toHaveLength(5);

    // Change maxResults to 3
    rerender({ max: 3 });

    // Add one more result
    act(() => {
      result.current.saveSearch({
        query: "query 5",
        timestamp: 5,
        summary: "summary 5",
        hits: [],
      });
    });

    // Should now only keep 3 results
    expect(result.current.history).toHaveLength(3);
  });
});
