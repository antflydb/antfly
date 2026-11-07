import { APICallError } from "@ai-sdk/provider";
import { InferenceClient } from "@antfly/sdk";
import { embedMany, generateText, streamText } from "ai";
import { afterEach, expect, it, vi } from "vitest";
import { getInferenceError, isRetryableCapacityError, retryDelayMs } from "../src/asset/errors.js";
import { fetchSourceResponse } from "../src/asset/transport.js";
import { AntflyChatLanguageModel } from "../src/language-model.js";
import { createAntfly } from "../src/provider.js";

afterEach(() => vi.unstubAllGlobals());
const capacity = () =>
  Response.json(
    {
      error: "MODEL_RESOURCE_BUSY",
      message: "busy",
      reason: "inference_capacity",
      retryable: true,
      retry_after_ms: 1,
    },
    { status: 503 }
  );
const generation = () =>
  Response.json({
    id: "r",
    model: "m",
    created: 1,
    choices: [{ index: 0, message: { role: "assistant", content: "ok" }, finish_reason: "stop" }],
    usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
  });
const chunk = {
  id: "r",
  model: "m",
  created: 1,
  choices: [{ index: 0, delta: { content: "ok" }, finish_reason: "stop" }],
  usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
};
const provider = () => createAntfly({ baseUrl: "http://inference.test" });

it("generateText retries a server-declared capacity failure", async () => {
  const fetch = vi.fn(async () => (fetch.mock.calls.length === 1 ? capacity() : generation()));
  vi.stubGlobal("fetch", fetch);
  expect(
    (await generateText({ model: provider().languageModel("m"), prompt: "hi", maxRetries: 1 })).text
  ).toBe("ok");
  expect(fetch).toHaveBeenCalledTimes(2);
});

it("streamText retries failed HTTP setup before reading the first event", async () => {
  const fetch = vi.fn(async () =>
    fetch.mock.calls.length === 1
      ? capacity()
      : new Response(`data: ${JSON.stringify(chunk)}\n\ndata: [DONE]\n\n`, {
          headers: { "content-type": "text/event-stream" },
        })
  );
  vi.stubGlobal("fetch", fetch);
  const result = streamText({ model: provider().languageModel("m"), prompt: "hi", maxRetries: 1 });
  expect(await result.text).toBe("ok");
  expect(fetch).toHaveBeenCalledTimes(2);
});

it("embedMany retries a capacity failure", async () => {
  const fetch = vi.fn(async () =>
    fetch.mock.calls.length === 1
      ? capacity()
      : Response.json({ data: [{ index: 0, embedding: [1, 2] }] })
  );
  vi.stubGlobal("fetch", fetch);
  expect(
    (await embedMany({ model: provider().embeddingModel("m"), values: ["hi"], maxRetries: 1 }))
      .embeddings
  ).toEqual([[1, 2]]);
  expect(fetch).toHaveBeenCalledTimes(2);
});

it("preserves classification through exhausted retries without retaining request bodies", async () => {
  const fetch = vi.fn(async () => capacity());
  vi.stubGlobal("fetch", fetch);
  try {
    await generateText({
      model: provider().languageModel("m"),
      prompt: "private prompt",
      maxRetries: 1,
    });
    expect.fail("expected exhausted retries");
  } catch (error) {
    expect(isRetryableCapacityError(error)).toBe(true);
    expect(retryDelayMs(error)).toBe(1);
    expect(getInferenceError(error)?.code).toBe("MODEL_RESOURCE_BUSY");
    const lastError = (error as { lastError: APICallError }).lastError;
    expect(APICallError.isInstance(lastError)).toBe(true);
    expect(lastError.requestBodyValues).toBeUndefined();
    expect(lastError.responseHeaders?.["retry-after-ms"]).toBe("1");
  }
});

it.each([409, 503])("does not retry a permanent HTTP %s response", async (status) => {
  const fetch = vi.fn(async () =>
    Response.json(
      { error: "InferenceSourceMismatch", message: "version changed", retryable: false },
      { status }
    )
  );
  vi.stubGlobal("fetch", fetch);
  await expect(
    generateText({ model: provider().languageModel("m"), prompt: "hi", maxRetries: 1 })
  ).rejects.toThrow();
  expect(fetch).toHaveBeenCalledTimes(1);
});

it("splits embedding batches by configured item and UTF-8 byte bounds", async () => {
  const batches: string[][] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: Request) => {
      const values = (await input.json()).input as string[];
      batches.push(values);
      expect(values.length).toBeLessThanOrEqual(2);
      expect(values.reduce((n, v) => n + Buffer.byteLength(v), 0)).toBeLessThanOrEqual(5);
      return Response.json({
        data: values.map((value, index) => ({ index, embedding: [value.length] })),
      });
    })
  );
  const antfly = createAntfly({
    baseUrl: "http://inference.test",
    embedding: { maxEmbeddingsPerCall: 10, maxInputBytesPerCall: 5 },
  });
  const model = antfly.embeddingModel("m", { maxEmbeddingsPerCall: 2 });
  expect(
    (await embedMany({ model, values: ["é", "é", "é", "a"], maxRetries: 0 })).embeddings
  ).toEqual([[1], [1], [1], [1]]);
  expect(batches.flat()).toEqual(["é", "é", "é", "a"]);
  await expect(model.doEmbed({ values: ["a", "b", "c"] })).rejects.toThrow(/maxEmbeddingsPerCall/);
  await expect(model.doEmbed({ values: ["ééé"] })).rejects.toThrow(/maxInputBytesPerCall/);
});

it("doStream returns on headers and cancels the body before any token arrives", async () => {
  const cancel = vi.fn();
  vi.stubGlobal(
    "fetch",
    async () =>
      new Response(new ReadableStream({ cancel }, { highWaterMark: 0 }), {
        headers: { "content-type": "text/event-stream" },
      })
  );
  const model = new AntflyChatLanguageModel({
    modelId: "m",
    inference: new InferenceClient({ baseUrl: "http://inference.test" }),
  });
  const { stream } = await model.doStream({ prompt: [] });
  await stream.cancel();
  expect(cancel).toHaveBeenCalledTimes(1);
});

it("disposes responses across fetch settlement cancellation races", async () => {
  for (let depth = 0; depth < 12; depth++) {
    const controller = new AbortController();
    const cancel = vi.fn();
    const response = new Response(new ReadableStream({ cancel }, { highWaterMark: 0 }));
    const pending = fetchSourceResponse(
      async () => {
        let later = () => controller.abort();
        for (let i = 0; i < depth; i++) {
          const previous = later;
          later = () => queueMicrotask(previous);
        }
        queueMicrotask(later);
        return response;
      },
      "https://source.test/media",
      { signal: controller.signal }
    );
    let returned: Response | undefined;
    try {
      returned = await pending;
    } catch {
      /* cancellation is expected */
    }
    await new Promise((resolve) => setTimeout(resolve, 0));
    if (returned) await returned.body?.cancel().catch(() => {});
    expect(cancel.mock.calls.length, `microtask depth ${depth}`).toBe(1);
  }
});

it("disposes an aborted response through the public display handler too", async () => {
  const { createAssetDisplayHandler } = await import("../src/asset/display-route.js");
  const { encodeAssetReference } = await import("../src/asset/reference.js");
  const controller = new AbortController();
  const cancel = vi.fn();
  const resource = {
    type: "source",
    uri: "https://sources.test/media",
    mime_type: "video/mp4",
    version: '"v1"',
    length: 1,
  };
  const client = {
    getRawClient: () => ({
      GET: async () => ({ response: new Response(), data: { content: { resource } } }),
    }),
  } as never;
  const handler = createAssetDisplayHandler({
    client,
    authorize: () => true,
    referenceId: () => encodeAssetReference({ table: "assets", key: "a" }),
    fetch: async () => {
      queueMicrotask(() => queueMicrotask(() => controller.abort()));
      return new Response(new ReadableStream({ cancel }, { highWaterMark: 0 }));
    },
  });
  let returned: Response | undefined;
  try {
    returned = await handler(new Request("https://app.test/file", { signal: controller.signal }));
  } catch {}
  await new Promise((resolve) => setTimeout(resolve, 0));
  if (returned) await returned.body?.cancel().catch(() => {});
  expect(cancel).toHaveBeenCalledOnce();
});

it.each([
  0,
  -1,
  1.5,
  Number.NaN,
  Infinity,
  Number.MAX_SAFE_INTEGER + 1,
])("rejects invalid embedding bounds %s", (limit) => {
  expect(() => provider().embeddingModel("m", { maxEmbeddingsPerCall: limit })).toThrow(RangeError);
  expect(() => provider().embeddingModel("m", { maxInputBytesPerCall: limit })).toThrow(RangeError);
});

it("rerank exposes a retryable provider error and retains only retry headers", async () => {
  vi.stubGlobal("fetch", async () =>
    Response.json(
      { error: "busy", message: "busy", retryable: true },
      { status: 429, headers: { "retry-after-ms": "2", "x-private": "secret" } }
    )
  );
  await expect(
    provider()
      .rerankingModel("m")
      .doRerank({ query: "query", documents: { type: "text", values: ["document"] } })
  ).rejects.toSatisfy((error: unknown) => {
    expect(APICallError.isInstance(error)).toBe(true);
    const callError = error as APICallError;
    expect(callError.isRetryable).toBe(true);
    expect(callError.responseHeaders).toEqual({ "retry-after-ms": "2" });
    expect(callError.requestBodyValues).toBeUndefined();
    return true;
  });
});
