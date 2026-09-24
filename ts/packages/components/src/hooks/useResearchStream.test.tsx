import type { ResearchAgentRequest } from "@antfly/sdk";
import { act, renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import * as utils from "../utils";
import { useResearchStream } from "./useResearchStream";

// Mock the streamResearch utility
vi.mock("../utils", async () => {
  const actual = await vi.importActual("../utils");
  return {
    ...actual,
    streamResearch: vi.fn(),
  };
});

const request: ResearchAgentRequest = {
  query: "How do hybrid search and reranking interact?",
  queries: [{ table: "docs" }],
};

describe("useResearchStream", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("initializes with idle state", () => {
    const { result } = renderHook(() => useResearchStream());

    expect(result.current.status).toBe("idle");
    expect(result.current.plan).toBeNull();
    expect(result.current.subQuestionProgress).toEqual([]);
    expect(result.current.findings).toEqual([]);
    expect(result.current.reflections).toEqual([]);
    expect(result.current.sections).toEqual([]);
    expect(result.current.reportMarkdown).toBe("");
    expect(result.current.verification).toBeNull();
    expect(result.current.result).toBeNull();
    expect(result.current.error).toBeNull();
  });

  it("accumulates plan, sub-question, finding, reflection, section and report progress", async () => {
    const mockController = new AbortController();

    vi.mocked(utils.streamResearch).mockImplementation(
      async (_url, _request, _headers, callbacks) => {
        setTimeout(() => {
          callbacks.onPlan?.({
            brief: "Explain the trade-offs",
            sub_questions: [{ id: "q1", question: "What is BM25?" }],
          });
          callbacks.onSubQuestionStarted?.({
            sub_question_id: "q1",
            question: "What is BM25?",
            round: 1,
          });
          callbacks.onFinding?.({ sub_question_id: "q1", summary: "BM25 scores lexical overlap." });
          callbacks.onReflection?.({ round: 1, done: true, gaps: [] });
          callbacks.onGeneration?.("# Overview\n\n");
          callbacks.onGeneration?.("Hybrid search combines BM25 and vectors.");
          callbacks.onSection?.({ index: 0, heading: "Overview" });
          callbacks.onVerification?.({ checked_sections: 1, supported_ratio: 1 });
          callbacks.onComplete?.();
        }, 10);
        return mockController;
      }
    );

    const { result } = renderHook(() => useResearchStream());

    await act(async () => {
      await result.current.startStream({ url: "http://localhost:8080/db/v1", request });
    });

    expect(result.current.status).toBe("streaming");

    await waitFor(() => expect(result.current.status).toBe("done"), { timeout: 100 });

    expect(result.current.plan).toEqual({
      brief: "Explain the trade-offs",
      sub_questions: [{ id: "q1", question: "What is BM25?" }],
    });
    expect(result.current.subQuestionProgress).toEqual([
      { sub_question_id: "q1", question: "What is BM25?", round: 1 },
    ]);
    expect(result.current.findings).toEqual([
      { sub_question_id: "q1", summary: "BM25 scores lexical overlap." },
    ]);
    expect(result.current.reflections).toEqual([{ round: 1, done: true, gaps: [] }]);
    expect(result.current.sections).toEqual([{ index: 0, heading: "Overview" }]);
    expect(result.current.reportMarkdown).toBe(
      "# Overview\n\nHybrid search combines BM25 and vectors."
    );
    expect(result.current.verification).toEqual({ checked_sections: 1, supported_ratio: 1 });
    expect(result.current.error).toBeNull();
  });

  it("populates state from a non-streaming research result", async () => {
    const finalResult = {
      status: "completed",
      research_state: { phase: "done" },
      plan: { brief: "b", sub_questions: [] },
      findings: [{ sub_question_id: "q1", summary: "s" }],
      reflections: [],
      verification: { checked_sections: 1, supported_ratio: 1 },
      report: { markdown: "Final report markdown." },
    };

    vi.mocked(utils.streamResearch).mockImplementation(
      async (_url, _request, _headers, callbacks) => {
        callbacks.onResearchAgentResult?.(finalResult as never);
        callbacks.onComplete?.();
        return new AbortController();
      }
    );

    const { result } = renderHook(() => useResearchStream());
    await act(async () => {
      await result.current.startStream({ url: "http://localhost:8080/db/v1", request });
    });

    expect(result.current.result).toEqual(finalResult);
    expect(result.current.reportMarkdown).toBe("Final report markdown.");
    expect(result.current.findings).toEqual(finalResult.findings);
    expect(result.current.status).toBe("done");
  });

  it("handles streaming errors", async () => {
    const error = new Error("Stream failed");
    vi.mocked(utils.streamResearch).mockImplementation(
      async (_url, _request, _headers, callbacks) => {
        setTimeout(() => callbacks.onError?.(error), 10);
        return new AbortController();
      }
    );

    const { result } = renderHook(() => useResearchStream());
    await act(async () => {
      await result.current.startStream({ url: "http://localhost:8080/db/v1", request });
    });

    await waitFor(() => expect(result.current.status).toBe("error"), { timeout: 100 });
    expect(result.current.error).toEqual(error);
  });

  it("stops the stream and aborts the controller", async () => {
    const mockAbort = vi.fn();
    const mockController = { abort: mockAbort, signal: new AbortController().signal };
    vi.mocked(utils.streamResearch).mockResolvedValue(mockController as unknown as AbortController);

    const { result } = renderHook(() => useResearchStream());
    await act(async () => {
      await result.current.startStream({ url: "http://localhost:8080/db/v1", request });
    });

    act(() => {
      result.current.stopStream();
    });

    expect(mockAbort).toHaveBeenCalled();
    expect(result.current.status).toBe("done");
  });

  it("resets all state", async () => {
    vi.mocked(utils.streamResearch).mockImplementation(
      async (_url, _request, _headers, callbacks) => {
        setTimeout(() => {
          callbacks.onGeneration?.("partial report");
          callbacks.onComplete?.();
        }, 10);
        return new AbortController();
      }
    );

    const { result } = renderHook(() => useResearchStream());
    await act(async () => {
      await result.current.startStream({ url: "http://localhost:8080/db/v1", request });
    });
    await waitFor(() => expect(result.current.reportMarkdown).toBe("partial report"), {
      timeout: 100,
    });

    act(() => {
      result.current.reset();
    });

    expect(result.current.status).toBe("idle");
    expect(result.current.reportMarkdown).toBe("");
    expect(result.current.plan).toBeNull();
  });

  it("aborts the previous stream when starting a new one", async () => {
    const firstAbort = vi.fn();
    const firstController = { abort: firstAbort, signal: new AbortController().signal };
    const secondController = new AbortController();

    vi.mocked(utils.streamResearch).mockResolvedValueOnce(
      firstController as unknown as AbortController
    );

    const { result } = renderHook(() => useResearchStream());
    await act(async () => {
      await result.current.startStream({ url: "http://localhost:8080/db/v1", request });
    });

    vi.mocked(utils.streamResearch).mockResolvedValueOnce(secondController);
    await act(async () => {
      await result.current.startStream({ url: "http://localhost:8080/db/v1", request });
    });

    expect(firstAbort).toHaveBeenCalled();
  });
});
