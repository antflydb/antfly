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
});
