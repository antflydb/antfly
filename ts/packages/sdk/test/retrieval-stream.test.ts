import { afterEach, describe, expect, it, vi } from "vitest";
import { AntflyClient } from "../src/client.js";

const request = { query: "question", queries: [{ table: "docs" }] };
const encoder = new TextEncoder();

function responseFromBytes(bytes: Uint8Array, split = false) {
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      if (split) {
        for (const byte of bytes) controller.enqueue(new Uint8Array([byte]));
      } else {
        controller.enqueue(bytes);
      }
      controller.close();
    },
  });
  return new Response(body, { headers: { "Content-Type": "text/event-stream" } });
}

afterEach(() => vi.restoreAllMocks());

describe("retrieval stream completion", () => {
  it.each([
    "",
    'event: generation\ndata: "partial"\n\n',
    "event: done\ndata: {}\n",
    "event: done\n\n",
    'event: generation\ndata: "event: done"\n\n',
    "event: done\ndata: not-json\n\n",
    "event: done\ndata: null\n\n",
    "event: generation\ndata: not-json\n\nevent: done\ndata: {}\n\n",
    'event: error\ndata: {"error":"GenerationFailed"}\n\nevent: done\ndata: {}\n\n',
  ])("reports incomplete or invalid streams exactly once: %j", async (body) => {
    const response = responseFromBytes(encoder.encode(body), true);
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(response);
    const onError = vi.fn();
    const onDone = vi.fn();
    await new AntflyClient({ baseUrl: "http://localhost:8080" }).streamRetrievalAgent(request, {
      onError,
      onDone,
      onGeneration: vi.fn(),
    });
    await vi.waitFor(() => expect(onError).toHaveBeenCalledOnce());
    expect(onDone).not.toHaveBeenCalled();
    expect(response.body?.locked).toBe(false);
  });

  it.each([
    "\n",
    "\r\n",
    "\r",
  ])("handles complete frames across bytes with %j delimiters", async (newline) => {
    const body =
      ': heartbeat\n\nevent:generation\ndata:"hé🐜"\n\nevent:done\ndata:{\ndata:"generation":"hé🐜"}\n\n';
    const response = responseFromBytes(encoder.encode(body.replaceAll("\n", newline)), true);
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(response);
    const onGeneration = vi.fn();
    const onError = vi.fn();
    const onDone = vi.fn();
    await new AntflyClient({ baseUrl: "http://localhost:8080" }).streamRetrievalAgent(request, {
      onGeneration,
      onError,
      onDone,
    });
    await vi.waitFor(() => expect(onDone).toHaveBeenCalledWith({ generation: "hé🐜" }));
    expect(onGeneration).toHaveBeenCalledWith("hé🐜");
    expect(onError).not.toHaveBeenCalled();
    expect(response.body?.locked).toBe(false);
  });

  it.each([
    new Error("connection lost"),
    new DOMException("connection lost", "AbortError"),
  ])("reports reader failures through onError: %j", async (failure) => {
    const response = new Response(
      new ReadableStream({
        start(controller) {
          controller.error(failure);
        },
      }),
      { headers: { "Content-Type": "text/event-stream" } }
    );
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(response);
    const onError = vi.fn();
    const onDone = vi.fn();
    await new AntflyClient({ baseUrl: "http://localhost:8080" }).streamRetrievalAgent(request, {
      onError,
      onDone,
    });
    await vi.waitFor(() => expect(onError).toHaveBeenCalledExactlyOnceWith("connection lost"));
    expect(onDone).not.toHaveBeenCalled();
    expect(response.body?.locked).toBe(false);
  });

  it("reports malformed UTF-8 rather than silently replacing it", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      responseFromBytes(new Uint8Array([0xc3, 0x28]))
    );
    const onError = vi.fn();
    await new AntflyClient({ baseUrl: "http://localhost:8080" }).streamRetrievalAgent(request, {
      onError,
    });
    await vi.waitFor(() =>
      expect(onError).toHaveBeenCalledExactlyOnceWith(
        "Retrieval agent stream contained invalid UTF-8"
      )
    );
  });

  it("releases an open stream at terminal completion", async () => {
    const cancel = vi.fn();
    const response = new Response(
      new ReadableStream({
        start(controller) {
          controller.enqueue(encoder.encode("event: done\ndata: {}\n\n"));
        },
        cancel,
      }),
      { headers: { "Content-Type": "text/event-stream" } }
    );
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(response);
    const onDone = vi.fn();
    await new AntflyClient({ baseUrl: "http://localhost:8080" }).streamRetrievalAgent(request, {
      onDone,
    });
    await vi.waitFor(() => expect(cancel).toHaveBeenCalledOnce());
    expect(onDone).toHaveBeenCalledOnce();
    expect(response.body?.locked).toBe(false);
  });

  it("keeps explicit cancellation distinct from a failed stream", async () => {
    let streamController: ReadableStreamDefaultController<Uint8Array>;
    const response = new Response(
      new ReadableStream<Uint8Array>({
        start(controller) {
          streamController = controller;
        },
      }),
      { headers: { "Content-Type": "text/event-stream" } }
    );
    vi.spyOn(globalThis, "fetch").mockImplementationOnce(async (_url, init) => {
      init?.signal?.addEventListener("abort", () =>
        streamController.error(new DOMException("Aborted", "AbortError"))
      );
      return response;
    });
    const onDone = vi.fn();
    const onError = vi.fn();
    const controller = await new AntflyClient({
      baseUrl: "http://localhost:8080",
    }).streamRetrievalAgent(request, { onDone, onError });
    controller.abort();
    await vi.waitFor(() => expect(response.body?.locked).toBe(false));
    expect(onDone).not.toHaveBeenCalled();
    expect(onError).not.toHaveBeenCalled();
  });
});
