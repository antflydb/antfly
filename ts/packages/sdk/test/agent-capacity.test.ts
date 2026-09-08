import { afterEach, describe, expect, it, vi } from "vitest";
import { AntflyClient } from "../src/client.js";
import { InferenceCapacityError } from "../src/inference-client.js";

const capacity = {
  error: "GenerationCapacityUnavailable",
  message: "inference capacity temporarily unavailable",
  reason: "inference_capacity",
  retryable: true,
  retry_after_ms: 1000,
};
const request = { query: "question", queries: [{ table: "docs" }] };

afterEach(() => vi.restoreAllMocks());

describe("agent capacity errors", () => {
  it.each([
    "retrieval",
    "stream",
    "builder",
  ])("preserves the HTTP 503 contract for %s", async (operation) => {
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      new Response(JSON.stringify(capacity), {
        status: 503,
        headers: { "Content-Type": "application/json", "Retry-After": "1" },
      })
    );
    const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
    const result =
      operation === "builder"
        ? client.queryBuilderAgent({ intent: "question", table: "docs" })
        : operation === "stream"
          ? client.streamRetrievalAgent(request, {})
          : client.retrievalAgent(request);
    await expect(result).rejects.toBeInstanceOf(InferenceCapacityError);
    await expect(result).rejects.toMatchObject({
      status: 503,
      code: capacity.error,
      reason: capacity.reason,
      retryable: true,
      retryAfterMs: 1000,
    });
  });

  it.each([
    capacity,
    { error: "GenerationFailed" },
    { ...capacity, retryable: false },
  ])("preserves SSE error details and terminates the stream: %j", async (payload) => {
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      new Response(`event: error\ndata: ${JSON.stringify(payload)}\n\nevent: done\ndata: {}\n\n`, {
        headers: { "Content-Type": "text/event-stream" },
      })
    );
    const onError = vi.fn();
    const onErrorDetail = vi.fn();
    const onDone = vi.fn();
    await new AntflyClient({ baseUrl: "http://localhost:8080" }).streamRetrievalAgent(request, {
      onError,
      onErrorDetail,
      onDone,
    });
    await vi.waitFor(() => expect(onErrorDetail).toHaveBeenCalledOnce());
    const error = onErrorDetail.mock.calls[0]?.[0];
    expect(error).toBeInstanceOf(Error);
    if (payload === capacity) {
      expect(error).toBeInstanceOf(InferenceCapacityError);
      expect(error).toMatchObject({
        code: capacity.error,
        reason: capacity.reason,
        retryable: true,
        retryAfterMs: 1000,
      });
    } else {
      expect(error).not.toBeInstanceOf(InferenceCapacityError);
    }
    expect(onError).toHaveBeenCalledExactlyOnceWith(error.message);
    expect(onDone).not.toHaveBeenCalled();
  });

  it("keeps older plain-text query-builder errors readable", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      new Response("capacity unavailable", {
        status: 503,
        headers: { "Content-Type": "text/plain" },
      })
    );
    await expect(
      new AntflyClient({ baseUrl: "http://localhost:8080" }).queryBuilderAgent({
        intent: "question",
        table: "docs",
      })
    ).rejects.toThrow("Query builder agent failed: capacity unavailable");
  });
});
