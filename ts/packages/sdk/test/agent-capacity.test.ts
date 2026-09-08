import { afterEach, describe, expect, it, vi } from "vitest";
import { AntflyClient, QueryTemporarilyUnavailableError } from "../src/client.js";
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

const chatConfig = {
  table: "docs",
  semanticIndexes: ["embedding"],
  generator: { provider: "antfly" as const, model: "fixture" },
};

describe("streaming chat terminal lifecycle", () => {
  it.each([
    ["capacity", `event: error\ndata: ${JSON.stringify(capacity)}\n\n`],
    ["error", 'event: error\ndata: {"error":"GenerationFailed"}\n\n'],
    ["EOF", 'event: generation\ndata: "partial"\n\n'],
  ])("rejects messages on terminal %s and forwards both error callbacks", async (_, body) => {
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      new Response(body, {
        headers: { "Content-Type": "text/event-stream" },
      })
    );
    const onErrorDetail = vi.fn();
    const onError = vi.fn();
    const turn = await new AntflyClient({ baseUrl: "http://localhost:8080" }).chatAgent(
      "question",
      chatConfig,
      [],
      { onErrorDetail, onError }
    );
    if (!("abortController" in turn)) throw new Error("expected streaming turn");
    const outcome = turn.messages.then(
      () => "resolved",
      (error) => error
    );
    await vi.waitFor(() => expect(onErrorDetail).toHaveBeenCalledOnce());
    expect(
      await Promise.race([
        outcome,
        new Promise((resolve) => setTimeout(() => resolve("pending"), 50)),
      ])
    ).toBe(onErrorDetail.mock.calls[0]?.[0]);
    expect(onError).toHaveBeenCalledExactlyOnceWith(onErrorDetail.mock.calls[0]?.[0].message);
  });

  it("rejects messages on abort even with empty callbacks", async () => {
    const stream = new ReadableStream<Uint8Array>();
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      new Response(stream, {
        headers: { "Content-Type": "text/event-stream" },
      })
    );
    const turn = await new AntflyClient({ baseUrl: "http://localhost:8080" }).chatAgent(
      "question",
      chatConfig,
      [],
      {}
    );
    if (!("abortController" in turn)) throw new Error("expected streaming turn");
    const outcome = turn.messages.catch((error) => error);
    turn.abortController.abort();
    expect(
      await Promise.race([
        outcome,
        new Promise((resolve) => setTimeout(() => resolve("pending"), 50)),
      ])
    ).toMatchObject({ name: "AbortError" });
  });

  it("resolves completed messages and ignores a later abort", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      new Response(
        'event: generation\ndata: "answer"\n\nevent: done\ndata: {"status":"completed"}\n\n',
        { headers: { "Content-Type": "text/event-stream" } }
      )
    );
    const onMessagesUpdated = vi.fn();
    const turn = await new AntflyClient({ baseUrl: "http://localhost:8080" }).chatAgent(
      "question",
      chatConfig,
      [],
      { onMessagesUpdated }
    );
    if (!("abortController" in turn)) throw new Error("expected streaming turn");
    const messages = await turn.messages;
    expect(messages).toEqual([
      { role: "user", content: "question" },
      { role: "assistant", content: "answer" },
    ]);
    expect(onMessagesUpdated).toHaveBeenCalledExactlyOnceWith(messages);
    turn.abortController.abort();
    await expect(turn.messages).resolves.toEqual(messages);
  });
});

it.each([
  "doc_identity_unavailable",
  "query_embedding_temporarily_unavailable",
])("preserves typed query-builder dependency failure %s", async (code) => {
  vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
    new Response(JSON.stringify({ code, message: "temporarily unavailable", retryable: true }), {
      status: 503,
      headers: { "Content-Type": "application/json", "Retry-After": "1" },
    })
  );
  const result = new AntflyClient({ baseUrl: "http://localhost:8080" }).queryBuilderAgent({
    intent: "question",
    table: "docs",
  });
  await expect(result).rejects.toBeInstanceOf(QueryTemporarilyUnavailableError);
  await expect(result).rejects.toMatchObject({ code, retryable: true, retryAfterSeconds: 1 });
});

it.each([
  "json",
  "sse-done-only",
  "sse-final-answer",
])("preserves the final chat answer from %s", async (format) => {
  const finalResult = { status: "completed", generation: "final answer", hits: [] };
  const body =
    format === "json"
      ? JSON.stringify(finalResult)
      : (format === "sse-final-answer" ? 'event: generation\ndata: "partial"\n\n' : "") +
        `event: done\ndata: ${JSON.stringify(finalResult)}\n\n`;
  vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
    new Response(body, {
      headers: { "Content-Type": format === "json" ? "application/json" : "text/event-stream" },
    })
  );
  const onAssistantMessage = vi.fn();
  const onMessagesUpdated = vi.fn();
  const turn = await new AntflyClient({ baseUrl: "http://localhost:8080" }).chatAgent(
    "question",
    chatConfig,
    [],
    { onAssistantMessage, onMessagesUpdated }
  );
  if (!("abortController" in turn)) throw new Error("expected streaming turn");
  const messages = await turn.messages;
  expect(messages.at(-1)).toEqual({ role: "assistant", content: "final answer" });
  expect(onAssistantMessage).toHaveBeenCalledExactlyOnceWith("final answer");
  expect(onMessagesUpdated).toHaveBeenCalledExactlyOnceWith(messages);
});

it("preserves server-provided chat history on JSON fallback", async () => {
  const messages = [
    { role: "system", content: "server context" },
    { role: "user", content: "question" },
    { role: "assistant", content: "final answer" },
  ];
  vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
    new Response(JSON.stringify({ status: "completed", generation: "final answer", messages }), {
      headers: { "Content-Type": "application/json" },
    })
  );
  const onMessagesUpdated = vi.fn();
  const turn = await new AntflyClient({ baseUrl: "http://localhost:8080" }).chatAgent(
    "question",
    chatConfig,
    [],
    { onMessagesUpdated }
  );
  if (!("abortController" in turn)) throw new Error("expected streaming turn");
  await expect(turn.messages).resolves.toEqual(messages);
  expect(onMessagesUpdated).toHaveBeenCalledExactlyOnceWith(messages);
});
