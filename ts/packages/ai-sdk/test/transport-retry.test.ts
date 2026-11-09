import { createServer } from "node:http";
import { APICallError } from "@ai-sdk/provider";
import { embedMany, generateText, rerank, streamText } from "ai";
import { afterEach, expect, it, vi } from "vitest";
import { providerInferenceError } from "../src/inference-error.js";
import { createAntfly } from "../src/provider.js";

afterEach(() => vi.unstubAllGlobals());

it.each([
  "generate",
  "stream",
  "embed",
  "rerank",
] as const)("retries a real socket reset during %s setup", async (operation) => {
  let calls = 0;
  const server = createServer((request, response) => {
    calls++;
    if (calls === 1) {
      request.socket.destroy();
      return;
    }
    const result =
      operation === "embed"
        ? { data: [{ index: 0, embedding: [1, 2] }] }
        : operation === "rerank"
          ? { data: [{ index: 0, score: 1 }] }
          : {
              id: "r",
              object: "chat.completion",
              model: "m",
              created: 1,
              choices: [
                {
                  index: 0,
                  message: { role: "assistant", content: "ok" },
                  delta: { content: "ok" },
                  finish_reason: "stop",
                },
              ],
            };
    response.writeHead(200, {
      "content-type": operation === "stream" ? "text/event-stream" : "application/json",
    });
    response.end(
      operation === "stream"
        ? `data: ${JSON.stringify(result)}\n\ndata: [DONE]\n\n`
        : JSON.stringify(result)
    );
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address() as { port: number };
  const provider = createAntfly({ baseUrl: `http://127.0.0.1:${address.port}` });
  try {
    if (operation === "generate")
      expect(
        (await generateText({ model: provider.languageModel("m"), prompt: "hi", maxRetries: 1 }))
          .text
      ).toBe("ok");
    if (operation === "stream")
      expect(
        await streamText({ model: provider.languageModel("m"), prompt: "hi", maxRetries: 1 }).text
      ).toBe("ok");
    if (operation === "embed")
      expect(
        (await embedMany({ model: provider.embeddingModel("m"), values: ["hi"], maxRetries: 1 }))
          .embeddings
      ).toEqual([[1, 2]]);
    if (operation === "rerank")
      expect(
        (
          await rerank({
            model: provider.rerankingModel("m"),
            query: "hi",
            documents: ["hi"],
            maxRetries: 1,
          })
        ).ranking
      ).toHaveLength(1);
    expect(calls).toBe(2);
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}, 10000);

it.each([
  "generate",
  "embed",
  "rerank",
] as const)("normalizes transient failures while reading %s response bodies", async (operation) => {
  const network = new TypeError("fetch failed at a private URL", {
    cause: Object.assign(new Error("private socket details"), { code: "ECONNRESET" }),
  });
  vi.stubGlobal(
    "fetch",
    async () =>
      new Response(
        new ReadableStream({
          start(controller) {
            controller.error(network);
          },
        }),
        { headers: { "content-type": "application/json" } }
      )
  );
  const provider = createAntfly({ baseUrl: "http://inference.test" });
  const request =
    operation === "generate"
      ? provider.languageModel("m").doGenerate({ prompt: [] })
      : operation === "embed"
        ? provider.embeddingModel("m").doEmbed({ values: ["a"] })
        : provider
            .rerankingModel("m")
            .doRerank({ query: "a", documents: { type: "text", values: ["a"] } });
  await expect(request).rejects.toSatisfy((error: unknown) => {
    expect(APICallError.isInstance(error)).toBe(true);
    expect((error as APICallError).isRetryable).toBe(true);
    expect((error as APICallError).requestBodyValues).toBeUndefined();
    expect((error as Error).cause).not.toBe(network);
    expect((error as Error).message).not.toContain("private");
    return true;
  });
});

it.each([
  new TypeError("bad request construction"),
  Object.assign(new Error("certificate expired"), { code: "CERT_HAS_EXPIRED" }),
])("does not classify permanent or programming errors as retryable", (error) => {
  expect(providerInferenceError(error, "generate")).toBe(error);
});

it("preserves caller abort reasons even when they carry a network error code", () => {
  const controller = new AbortController();
  const reason = Object.assign(new Error("caller stopped"), { code: "ECONNRESET" });
  controller.abort(reason);
  expect(() => providerInferenceError(reason, "generate", controller.signal)).toThrow(reason);
});
