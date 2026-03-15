import type { QueryHit, RetrievalAgentRequest } from "@antfly/sdk";
import { act, renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import * as utils from "../utils";
import { useAnswerStream } from "./useAnswerStream";

// Mock the streamAnswer utility
vi.mock("../utils", async () => {
  const actual = await vi.importActual("../utils");
  return {
    ...actual,
    streamAnswer: vi.fn(),
  };
});

describe("useAnswerStream", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("should initialize with empty state", () => {
    const { result } = renderHook(() => useAnswerStream());

    expect(result.current.answer).toBe("");
    expect(result.current.reasoning).toBe("");
    expect(result.current.classification).toBeNull();
    expect(result.current.hits).toEqual([]);
    expect(result.current.followUpQuestions).toEqual([]);
    expect(result.current.isStreaming).toBe(false);
    expect(result.current.error).toBeNull();
  });

  it("should start streaming and handle callbacks", async () => {
    const mockController = new AbortController();

    // Mock streamAnswer to call callbacks
    vi.mocked(utils.streamAnswer).mockImplementation(
      async (_url, _request, _headers, callbacks) => {
        // Simulate streaming events
        setTimeout(() => {
          callbacks.onClassification?.({
            route_type: "question",
            strategy: "simple",
            semantic_mode: "rewrite",
            improved_query: "improved",
            semantic_query: "semantic",
            confidence: 0.9,
          });

          callbacks.onHit?.({
            _id: "doc1",
            _source: { title: "Test Doc" },
            _score: 0.95,
          } as QueryHit);

          callbacks.onReasoning?.("Thinking about ");
          callbacks.onReasoning?.("the problem...");

          callbacks.onGeneration?.("Raft is ");
          callbacks.onGeneration?.("a consensus algorithm.");

          callbacks.onFollowup?.("What is Paxos?");

          callbacks.onComplete?.();
        }, 10);

        return mockController;
      }
    );

    const { result } = renderHook(() => useAnswerStream());

    const request: RetrievalAgentRequest = {
      query: "how does raft work",
      queries: [{ table: "docs", semantic_search: "how does raft work" }],
      generator: { provider: "openai", model: "gpt-4" },
    };

    await act(async () => {
      await result.current.startStream({
        url: "http://localhost:8080/api/v1",
        request,
        headers: { "X-API-Key": "test" },
      });
    });

    // Should be streaming
    expect(result.current.isStreaming).toBe(true);

    // Wait for callbacks to execute
    await waitFor(
      () => {
        expect(result.current.isStreaming).toBe(false);
      },
      { timeout: 100 }
    );

    // Check all state was updated
    expect(result.current.classification).toEqual({
      route_type: "question",
      strategy: "simple",
      semantic_mode: "rewrite",
      improved_query: "improved",
      semantic_query: "semantic",
      confidence: 0.9,
    });

    expect(result.current.hits).toHaveLength(1);
    expect(result.current.hits[0]._id).toBe("doc1");

    expect(result.current.reasoning).toBe("Thinking about the problem...");
    expect(result.current.answer).toBe("Raft is a consensus algorithm.");

    expect(result.current.followUpQuestions).toEqual(["What is Paxos?"]);
    expect(result.current.error).toBeNull();
  });

  it("should handle streaming errors", async () => {
    const error = new Error("Stream failed");

    vi.mocked(utils.streamAnswer).mockImplementation(
      async (_url, _request, _headers, callbacks) => {
        setTimeout(() => {
          callbacks.onError?.(error);
        }, 10);
        return new AbortController();
      }
    );

    const { result } = renderHook(() => useAnswerStream());

    await act(async () => {
      await result.current.startStream({
        url: "http://localhost:8080/api/v1",
        request: {
          query: "test",
          queries: [{ table: "docs" }],
          generator: { provider: "openai", model: "gpt-4" },
        },
      });
    });

    await waitFor(
      () => {
        expect(result.current.error).toEqual(error);
      },
      { timeout: 100 }
    );

    expect(result.current.isStreaming).toBe(false);
  });

  it("should handle string errors", async () => {
    const errorString = "Network error";

    vi.mocked(utils.streamAnswer).mockImplementation(
      async (_url, _request, _headers, callbacks) => {
        setTimeout(() => {
          callbacks.onError?.(errorString);
        }, 10);
        return new AbortController();
      }
    );

    const { result } = renderHook(() => useAnswerStream());

    await act(async () => {
      await result.current.startStream({
        url: "http://localhost:8080/api/v1",
        request: {
          query: "test",
          queries: [{ table: "docs" }],
          generator: { provider: "openai", model: "gpt-4" },
        },
      });
    });

    await waitFor(
      () => {
        expect(result.current.error).toBeInstanceOf(Error);
      },
      { timeout: 100 }
    );

    expect(result.current.error?.message).toBe(errorString);
  });

  it("should reset state when starting new stream", async () => {
    const mockController = new AbortController();

    vi.mocked(utils.streamAnswer).mockImplementation(
      async (_url, _request, _headers, callbacks) => {
        setTimeout(() => {
          callbacks.onGeneration?.("First answer");
          callbacks.onComplete?.();
        }, 10);
        return mockController;
      }
    );

    const { result } = renderHook(() => useAnswerStream());

    // First stream
    await act(async () => {
      await result.current.startStream({
        url: "http://localhost:8080/api/v1",
        request: {
          query: "first",
          queries: [{ table: "docs" }],
          generator: { provider: "openai", model: "gpt-4" },
        },
      });
    });

    await waitFor(() => expect(result.current.answer).toBe("First answer"), {
      timeout: 100,
    });

    // Second stream
    vi.mocked(utils.streamAnswer).mockImplementation(
      async (_url, _request, _headers, callbacks) => {
        setTimeout(() => {
          callbacks.onGeneration?.("Second answer");
          callbacks.onComplete?.();
        }, 10);
        return mockController;
      }
    );

    await act(async () => {
      await result.current.startStream({
        url: "http://localhost:8080/api/v1",
        request: {
          query: "second",
          queries: [{ table: "docs" }],
          generator: { provider: "openai", model: "gpt-4" },
        },
      });
    });

    await waitFor(() => expect(result.current.answer).toBe("Second answer"), {
      timeout: 100,
    });

    // Should have replaced, not appended
    expect(result.current.answer).toBe("Second answer");
  });

  it("should stop stream", async () => {
    const mockAbort = vi.fn();
    const mockController = {
      abort: mockAbort,
      signal: new AbortController().signal,
    } as AbortController;

    vi.mocked(utils.streamAnswer).mockResolvedValue(mockController);

    const { result } = renderHook(() => useAnswerStream());

    await act(async () => {
      await result.current.startStream({
        url: "http://localhost:8080/api/v1",
        request: {
          query: "test",
          queries: [{ table: "docs" }],
          generator: { provider: "openai", model: "gpt-4" },
        },
      });
    });

    act(() => {
      result.current.stopStream();
    });

    expect(mockAbort).toHaveBeenCalled();
    expect(result.current.isStreaming).toBe(false);
  });

  it("should reset all state", async () => {
    const mockController = new AbortController();

    vi.mocked(utils.streamAnswer).mockImplementation(
      async (_url, _request, _headers, callbacks) => {
        setTimeout(() => {
          callbacks.onGeneration?.("Test answer");
          callbacks.onReasoning?.("Test reasoning");
          callbacks.onHit?.({
            _id: "doc1",
            _source: {},
          } as QueryHit);
          callbacks.onComplete?.();
        }, 10);
        return mockController;
      }
    );

    const { result } = renderHook(() => useAnswerStream());

    await act(async () => {
      await result.current.startStream({
        url: "http://localhost:8080/api/v1",
        request: {
          query: "test",
          queries: [{ table: "docs" }],
          generator: { provider: "openai", model: "gpt-4" },
        },
      });
    });

    await waitFor(() => expect(result.current.answer).toBe("Test answer"), {
      timeout: 100,
    });

    act(() => {
      result.current.reset();
    });

    expect(result.current.answer).toBe("");
    expect(result.current.reasoning).toBe("");
    expect(result.current.classification).toBeNull();
    expect(result.current.hits).toEqual([]);
    expect(result.current.followUpQuestions).toEqual([]);
    expect(result.current.error).toBeNull();
    expect(result.current.isStreaming).toBe(false);
  });

  it("should accumulate multiple hits", async () => {
    const mockController = new AbortController();

    vi.mocked(utils.streamAnswer).mockImplementation(
      async (_url, _request, _headers, callbacks) => {
        setTimeout(() => {
          callbacks.onHit?.({ _id: "doc1", _source: {} } as QueryHit);
          callbacks.onHit?.({ _id: "doc2", _source: {} } as QueryHit);
          callbacks.onHit?.({ _id: "doc3", _source: {} } as QueryHit);
          callbacks.onComplete?.();
        }, 10);
        return mockController;
      }
    );

    const { result } = renderHook(() => useAnswerStream());

    await act(async () => {
      await result.current.startStream({
        url: "http://localhost:8080/api/v1",
        request: {
          query: "test",
          queries: [{ table: "docs" }],
          generator: { provider: "openai", model: "gpt-4" },
        },
      });
    });

    await waitFor(() => expect(result.current.hits).toHaveLength(3), {
      timeout: 100,
    });

    expect(result.current.hits[0]._id).toBe("doc1");
    expect(result.current.hits[1]._id).toBe("doc2");
    expect(result.current.hits[2]._id).toBe("doc3");
  });

  it("should accumulate follow-up questions", async () => {
    const mockController = new AbortController();

    vi.mocked(utils.streamAnswer).mockImplementation(
      async (_url, _request, _headers, callbacks) => {
        setTimeout(() => {
          callbacks.onFollowup?.("Question 1?");
          callbacks.onFollowup?.("Question 2?");
          callbacks.onFollowup?.("Question 3?");
          callbacks.onComplete?.();
        }, 10);
        return mockController;
      }
    );

    const { result } = renderHook(() => useAnswerStream());

    await act(async () => {
      await result.current.startStream({
        url: "http://localhost:8080/api/v1",
        request: {
          query: "test",
          queries: [{ table: "docs" }],
          generator: { provider: "openai", model: "gpt-4" },
        },
      });
    });

    await waitFor(() => expect(result.current.followUpQuestions).toHaveLength(3), { timeout: 100 });

    expect(result.current.followUpQuestions).toEqual(["Question 1?", "Question 2?", "Question 3?"]);
  });

  it("should abort previous stream when starting new one", async () => {
    const firstAbort = vi.fn();
    const firstController = {
      abort: firstAbort,
      signal: new AbortController().signal,
    } as AbortController;

    const secondController = new AbortController();

    // First stream
    vi.mocked(utils.streamAnswer).mockResolvedValueOnce(firstController);

    const { result } = renderHook(() => useAnswerStream());

    await act(async () => {
      await result.current.startStream({
        url: "http://localhost:8080/api/v1",
        request: {
          query: "first",
          queries: [{ table: "docs" }],
          generator: { provider: "openai", model: "gpt-4" },
        },
      });
    });

    // Second stream
    vi.mocked(utils.streamAnswer).mockResolvedValueOnce(secondController);

    await act(async () => {
      await result.current.startStream({
        url: "http://localhost:8080/api/v1",
        request: {
          query: "second",
          queries: [{ table: "docs" }],
          generator: { provider: "openai", model: "gpt-4" },
        },
      });
    });

    // First stream should have been aborted
    expect(firstAbort).toHaveBeenCalled();
  });
});
