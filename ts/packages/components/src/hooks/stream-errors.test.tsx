import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { useAnswerStream } from "./useAnswerStream";
import { useChatStream } from "./useChatStream";

// Exercise the real SDK and streamAnswer adapter, not the setup-file mock.
vi.unmock("@antfly/sdk");

afterEach(() => vi.restoreAllMocks());

function mockResponse(tail: string) {
  vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
    new Response(`event: generation\ndata: "partial answer"\n\n${tail}`, {
      headers: { "Content-Type": "text/event-stream" },
    })
  );
}

describe("SDK stream failures reach React state", () => {
  it.each([
    "",
    'event: error\ndata: {"error":"GenerationFailed"}\n\n',
  ])("stops answer loading and preserves partial text on failure: %j", async (tail) => {
    mockResponse(tail);
    const { result } = renderHook(() => useAnswerStream());
    await act(async () =>
      result.current.startStream({
        url: "http://localhost:8080",
        request: { query: "question", queries: [{ table: "docs" }] },
      })
    );
    await waitFor(() => expect(result.current.error).not.toBeNull());
    expect(result.current.error?.message).toBe(
      tail ? "GenerationFailed" : "Retrieval agent stream ended before done"
    );
    expect(result.current.isStreaming).toBe(false);
    expect(result.current.answer).toBe("partial answer");
  });

  it("marks a disconnected chat turn failed rather than leaving it loading", async () => {
    mockResponse("");
    const { result } = renderHook(() => useChatStream());
    await act(async () =>
      result.current.sendMessage("question", {
        url: "http://localhost:8080",
        table: "docs",
        generator: { provider: "antfly", model: "test" },
      })
    );
    await waitFor(() =>
      expect(result.current.turns[0]?.error).toBe("Retrieval agent stream ended before done")
    );
    expect(result.current.isStreaming).toBe(false);
    expect(result.current.turns[0]?.isStreaming).toBe(false);
    expect(result.current.turns[0]?.assistantMessage).toBe("partial answer");
  });

  it.each([
    ["answer", () => useAnswerStream()],
    ["chat", () => useChatStream()],
  ] as const)("preserves %s deltas when done omits terminal fields", async (_name, useStream) => {
    mockResponse("event: done\ndata: {}\n\n");
    const { result } = renderHook(useStream);
    await act(async () => {
      if ("startStream" in result.current) {
        await result.current.startStream({
          url: "http://localhost:8080",
          request: { query: "question", queries: [{ table: "docs" }] },
        });
      } else {
        await result.current.sendMessage("question", {
          url: "http://localhost:8080",
          table: "docs",
          generator: { provider: "antfly", model: "test" },
        });
      }
    });
    await waitFor(() => expect(result.current.isStreaming).toBe(false));
    if ("answer" in result.current) expect(result.current.answer).toBe("partial answer");
    else expect(result.current.turns[0]?.assistantMessage).toBe("partial answer");
  });
});

describe("chat request context and cancellation", () => {
  it("sends the custom system prompt and tools with explicit pipeline steps", async () => {
    const fetch = vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      new Response("event: done\ndata: {}\n\n", {
        headers: { "Content-Type": "text/event-stream" },
      })
    );
    const { result } = renderHook(() => useChatStream());
    await act(async () =>
      result.current.sendMessage("question", {
        url: "http://localhost:8080",
        table: "docs",
        generator: { provider: "antfly", model: "test" },
        systemPrompt: "Use the supplied documents.",
        steps: { generation: {}, confidence: {} },
        tools: { enabled_tools: ["semantic_search"] },
      })
    );
    const body = JSON.parse(String(fetch.mock.calls[0][1]?.body));
    expect(body.steps.generation.system_prompt).toBe("Use the supplied documents.");
    expect(body.steps.confidence).toEqual({});
    expect(body.tools).toEqual({ enabled_tools: ["semantic_search"] });
  });

  it.each(["stop", "unmount"])("%s aborts before HTTP headers arrive", async (action) => {
    let signal: AbortSignal | undefined;
    vi.spyOn(globalThis, "fetch").mockImplementation((_url, init) => {
      signal = init?.signal ?? undefined;
      return new Promise((_resolve, reject) => {
        signal?.addEventListener("abort", () => reject(signal?.reason), { once: true });
      });
    });
    const { result, unmount } = renderHook(() => useChatStream());
    let pending: Promise<void>;
    act(() => {
      pending = result.current.sendMessage("question", {
        url: "http://localhost:8080",
        table: "docs",
        generator: { provider: "antfly", model: "test" },
      });
    });
    expect(signal?.aborted).toBe(false);
    if (action === "stop") act(() => result.current.abort());
    else unmount();
    await act(async () => pending);
    expect(signal?.aborted).toBe(true);
    if (action === "stop") {
      expect(result.current.isStreaming).toBe(false);
      expect(result.current.turns[0]?.error).toBeNull();
    }
  });
});

describe("answer request cancellation", () => {
  it.each(["stop", "unmount"])("%s aborts before HTTP headers arrive", async (action) => {
    let signal: AbortSignal | undefined;
    vi.spyOn(globalThis, "fetch").mockImplementation((_url, init) => {
      signal = init?.signal ?? undefined;
      return new Promise((_resolve, reject) => {
        signal?.addEventListener("abort", () => reject(signal?.reason), { once: true });
      });
    });
    const { result, unmount } = renderHook(() => useAnswerStream());
    let pending: Promise<void>;
    act(() => {
      pending = result.current.startStream({
        url: "http://localhost:8080",
        request: { query: "question", queries: [{ table: "docs" }] },
      });
    });
    expect(signal?.aborted).toBe(false);
    if (action === "stop") act(() => result.current.stopStream());
    else unmount();
    await act(async () => pending);
    expect(signal?.aborted).toBe(true);
    if (action === "stop") {
      expect(result.current.isStreaming).toBe(false);
      expect(result.current.error).toBeNull();
    }
  });
});
