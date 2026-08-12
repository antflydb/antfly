import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  Client,
  InferenceAPIError,
  InferenceCapacityError,
  InferenceClient,
  serializeEmbeddings,
} from "../src/index.js";

describe("InferenceClient", () => {
  it("should create a client with base config", () => {
    const client = new InferenceClient({
      baseUrl: "http://localhost:8080",
    });
    expect(client).toBeDefined();
    expect(client.getRawClient).toBeDefined();
  });

  it("should create a client with custom headers", () => {
    const client = new InferenceClient({
      baseUrl: "http://localhost:8080",
      headers: {
        "X-Custom-Header": "test-value",
      },
    });
    expect(client).toBeDefined();
  });

  it("rejects invalid binary response limits", () => {
    expect(
      () => new InferenceClient({ baseUrl: "http://localhost:8080", maxBinaryResponseBytes: 0 })
    ).toThrow("maxBinaryResponseBytes must be a positive safe integer");
  });

  it("should have all expected methods", () => {
    const client = new InferenceClient({
      baseUrl: "http://localhost:8080",
    });

    expect(typeof client.embed).toBe("function");
    expect(typeof client.generate).toBe("function");
    expect(typeof client.generateStream).toBe("function");
    expect(typeof client.embedBinary).toBe("function");
    expect(typeof client.chunk).toBe("function");
    expect(typeof client.rerank).toBe("function");
    expect(typeof client.extract).toBe("function");
    expect(typeof client.extractRaw).toBe("function");
    expect(typeof client.classify).toBe("function");
    expect(typeof client.extractEntities).toBe("function");
    expect(typeof client.rewrite).toBe("function");
    expect(typeof client.transcribe).toBe("function");
    expect(typeof client.listModels).toBe("function");
    expect(typeof client.getRawClient).toBe("function");
  });

  it("should expose raw client for advanced usage", () => {
    const client = new InferenceClient({
      baseUrl: "http://localhost:8080",
    });

    const rawClient = client.getRawClient();
    expect(rawClient).toBeDefined();
    expect(typeof rawClient.GET).toBe("function");
    expect(typeof rawClient.POST).toBe("function");
  });

  it("should strip trailing slash from baseUrl", () => {
    const client = new InferenceClient({
      baseUrl: "http://localhost:8080/",
    });
    expect(client).toBeDefined();
  });

  it("should accept legacy /api-prefixed base URLs", () => {
    const client = new InferenceClient({
      baseUrl: "http://localhost:8080/api",
    });
    expect(client).toBeDefined();
  });

  it("should accept explicit /ai/v1-prefixed base URLs", () => {
    const client = new InferenceClient({
      baseUrl: "http://localhost:8080/ai/v1",
    });
    expect(client).toBeDefined();
  });
});

describe("InferenceClient with mock fetch", () => {
  const originalFetch = global.fetch;

  beforeEach(() => {
    vi.stubGlobal("fetch", vi.fn());
  });

  afterEach(() => {
    vi.stubGlobal("fetch", originalFetch);
  });

  async function lastFetchJSONBody(): Promise<Record<string, unknown>> {
    const [input, init] = vi.mocked(fetch).mock.calls.at(-1) ?? [];
    if (input instanceof Request) {
      return (await input.clone().json()) as Record<string, unknown>;
    }
    if (typeof init?.body === "string") {
      return JSON.parse(init.body) as Record<string, unknown>;
    }
    throw new Error("fetch call did not include a JSON request body");
  }

  describe("generate", () => {
    const request = {
      model: "gemma",
      messages: [{ role: "user" as const, content: "hello" }],
    };

    it("supports JSON and framed SSE generation without exposing the generated parser gap", async () => {
      const completion = {
        id: "chatcmpl-json",
        object: "chat.completion" as const,
        created: 1,
        model: "gemma",
        choices: [
          {
            index: 0,
            message: { role: "assistant" as const, content: "hello" },
            finish_reason: "stop" as const,
          },
        ],
        usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
      };
      vi.mocked(fetch).mockResolvedValueOnce(
        new Response(JSON.stringify(completion), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        })
      );

      const client = new InferenceClient({ baseUrl: "http://localhost:8080" });
      await expect(client.generate(request)).resolves.toEqual(completion);
      expect(await lastFetchJSONBody()).toMatchObject({ model: "gemma", stream: false });

      const encoder = new TextEncoder();
      const eventBytes = encoder.encode(
        ': heartbeat\r\n\r\nevent: message\r\ndata: {"id":"chatcmpl-stream","object":"chat.completion.chunk",\r\ndata: "created":1,"model":"gemma","choices":[{"index":0,"delta":{"content":"hé🐜"}}]}\r\n\r\ndata: [DONE]\r\n\r\n'
      );
      vi.mocked(fetch).mockResolvedValueOnce(
        new Response(
          new ReadableStream({
            start(controller) {
              for (let index = 0; index < eventBytes.length; index++) {
                controller.enqueue(eventBytes.slice(index, index + 1));
              }
              controller.close();
            },
          }),
          { status: 200, headers: { "Content-Type": "text/event-stream; charset=utf-8" } }
        )
      );

      const controller = new AbortController();
      const chunks = [];
      for await (const chunk of client.generateStream(request, { signal: controller.signal })) {
        chunks.push(chunk);
      }
      expect(chunks).toHaveLength(1);
      expect(chunks[0]?.choices[0]?.delta.content).toBe("hé🐜");
      expect(await lastFetchJSONBody()).toMatchObject({ model: "gemma", stream: true });
      const [, init] = vi.mocked(fetch).mock.calls.at(-1) ?? [];
      expect(init?.signal).toBe(controller.signal);
      expect((init?.headers as Record<string, string>).Accept).toBe("text/event-stream");
    });

    it("normalizes repeated trailing slashes and an explicit inference prefix", async () => {
      const completion = {
        id: "chatcmpl-json",
        object: "chat.completion" as const,
        created: 1,
        model: "gemma",
        choices: [],
        usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
      };
      vi.mocked(fetch).mockResolvedValueOnce(
        new Response(JSON.stringify(completion), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        })
      );

      const client = new InferenceClient({ baseUrl: "http://localhost:8080/ai/v1///" });
      await client.generate(request);
      const [input] = vi.mocked(fetch).mock.calls.at(-1) ?? [];
      expect(input instanceof Request ? input.url : input).toBe(
        "http://localhost:8080/ai/v1/generate"
      );
    });

    it("forwards consolidated client authentication and custom headers", async () => {
      const completion = {
        id: "chatcmpl-json",
        object: "chat.completion" as const,
        created: 1,
        model: "gemma",
        choices: [],
        usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
      };
      vi.mocked(fetch).mockResolvedValueOnce(
        new Response(JSON.stringify(completion), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        })
      );

      const client = new Client({
        baseUrl: "http://localhost:8080",
        auth: { type: "token", token: "secret" },
        headers: { "X-Tenant": "tenant-1" },
      });
      await client.Inference().generate(request);

      const [input, init] = vi.mocked(fetch).mock.calls.at(-1) ?? [];
      const headers = input instanceof Request ? input.headers : new Headers(init?.headers);
      expect(headers.get("Authorization")).toBe("Bearer secret");
      expect(headers.get("X-Tenant")).toBe("tenant-1");
    });

    it("returns typed 507 detail and cancels the response body when iteration stops", async () => {
      vi.mocked(fetch).mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            error: "MEMORY_BUDGET_EXCEEDED",
            message: "model needs 8 GiB",
            retryable: true,
          }),
          { status: 507, headers: { "Content-Type": "application/json" } }
        )
      );

      const client = new InferenceClient({ baseUrl: "http://localhost:8080" });
      let caught: unknown;
      try {
        await client.generate(request);
      } catch (error) {
        caught = error;
      }
      expect(caught).toBeInstanceOf(InferenceAPIError);
      expect(caught).toMatchObject({
        status: 507,
        code: "MEMORY_BUDGET_EXCEEDED",
        retryable: true,
      });
      expect((caught as Error).message).toContain("model needs 8 GiB (MEMORY_BUDGET_EXCEEDED)");

      vi.mocked(fetch).mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            error: "MEMORY_BUDGET_EXCEEDED",
            message: "model needs 8 GiB",
            retryable: true,
          }),
          { status: 507, headers: { "Content-Type": "application/json" } }
        )
      );
      await expect(client.generateStream(request).next()).rejects.toMatchObject({
        status: 507,
        code: "MEMORY_BUDGET_EXCEEDED",
        retryable: true,
      });

      const cancel = vi.fn();
      const body = new ReadableStream<Uint8Array>({
        start(streamController) {
          streamController.enqueue(
            new TextEncoder().encode(
              'data: {"id":"x","object":"chat.completion.chunk","created":1,"model":"gemma","choices":[]}\n\n'
            )
          );
        },
        cancel,
      });
      vi.mocked(fetch).mockResolvedValueOnce(
        new Response(body, { status: 200, headers: { "Content-Type": "text/event-stream" } })
      );
      for await (const _chunk of client.generateStream(request)) break;
      expect(cancel).toHaveBeenCalledOnce();
    });

    it("cancels the response body when a stream event contains invalid JSON", async () => {
      const cancel = vi.fn();
      const body = new ReadableStream<Uint8Array>({
        start(streamController) {
          streamController.enqueue(new TextEncoder().encode("data: not-json\n\n"));
        },
        cancel,
      });
      vi.mocked(fetch).mockResolvedValueOnce(
        new Response(body, { status: 200, headers: { "Content-Type": "text/event-stream" } })
      );

      const client = new InferenceClient({ baseUrl: "http://localhost:8080" });
      await expect(client.generateStream(request).next()).rejects.toThrow(
        "Generation stream returned invalid JSON"
      );
      expect(cancel).toHaveBeenCalledOnce();
    });

    it("rejects malformed UTF-8 in stream JSON and cancels the response body", async () => {
      const prefix = new TextEncoder().encode(
        'data: {"id":"x","object":"chat.completion.chunk","created":1,"model":"gemma","choices":[{"index":0,"delta":{"content":"'
      );
      const suffix = new TextEncoder().encode('"}}]}\n\n');
      const bytes = new Uint8Array(prefix.length + 2 + suffix.length);
      bytes.set(prefix);
      bytes.set([0xc3, 0x28], prefix.length);
      bytes.set(suffix, prefix.length + 2);
      const cancel = vi.fn();
      const body = new ReadableStream<Uint8Array>({
        start(streamController) {
          streamController.enqueue(bytes);
        },
        cancel,
      });
      vi.mocked(fetch).mockResolvedValueOnce(
        new Response(body, { status: 200, headers: { "Content-Type": "text/event-stream" } })
      );

      const client = new InferenceClient({ baseUrl: "http://localhost:8080" });
      await expect(client.generateStream(request).next()).rejects.toThrow(
        "Generation stream contained invalid UTF-8"
      );
      expect(cancel).toHaveBeenCalledOnce();
    });

    it("rejects oversized SSE transport chunks before decoding and cancels the body", async () => {
      const cancel = vi.fn();
      const body = new ReadableStream<Uint8Array>({
        start(streamController) {
          streamController.enqueue(new Uint8Array((16 << 20) + 1));
        },
        cancel,
      });
      vi.mocked(fetch).mockResolvedValueOnce(
        new Response(body, { status: 200, headers: { "Content-Type": "text/event-stream" } })
      );

      const client = new InferenceClient({ baseUrl: "http://localhost:8080" });
      await expect(client.generateStream(request).next()).rejects.toThrow(
        "Generation SSE chunk exceeded 16777216 bytes"
      );
      expect(cancel).toHaveBeenCalledOnce();
    });

    it("rejects oversized SSE lines accumulated across transport chunks", async () => {
      const cancel = vi.fn();
      const chunk = new Uint8Array(1 << 20);
      chunk.fill(0x61);
      const body = new ReadableStream<Uint8Array>({
        start(streamController) {
          for (let index = 0; index < 16; index++) streamController.enqueue(chunk);
          streamController.enqueue(new Uint8Array([0x61]));
        },
        cancel,
      });
      vi.mocked(fetch).mockResolvedValueOnce(
        new Response(body, { status: 200, headers: { "Content-Type": "text/event-stream" } })
      );

      const client = new InferenceClient({ baseUrl: "http://localhost:8080" });
      await expect(client.generateStream(request).next()).rejects.toThrow(
        "Generation SSE line exceeded 16777216 bytes"
      );
      expect(cancel).toHaveBeenCalledOnce();
    });

    it("bounds oversized error responses", async () => {
      vi.mocked(fetch).mockResolvedValueOnce(
        new Response("x".repeat((1 << 20) + 1), {
          status: 500,
          statusText: "Internal Server Error",
        })
      );

      const client = new InferenceClient({ baseUrl: "http://localhost:8080" });
      await expect(client.generate(request)).rejects.toThrow(
        "Internal Server Error (response body exceeded 1048576 bytes)"
      );
    });

    it("bounds oversized non-streaming generation responses", async () => {
      const cancel = vi.fn();
      const body = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(new TextEncoder().encode("x".repeat((16 << 20) + 1)));
        },
        cancel,
      });
      vi.mocked(fetch).mockResolvedValueOnce(
        new Response(body, {
          status: 200,
          headers: { "Content-Type": "application/json" },
        })
      );

      const client = new InferenceClient({ baseUrl: "http://localhost:8080" });
      await expect(client.generate(request)).rejects.toThrow(
        "Generation response exceeded 16777216 bytes"
      );
      expect(cancel).toHaveBeenCalledOnce();
    });

    it("rejects a stream that closes without [DONE]", async () => {
      vi.mocked(fetch).mockResolvedValueOnce(
        new Response(
          new ReadableStream({
            start(controller) {
              controller.enqueue(
                new TextEncoder().encode(
                  'data: {"id":"x","object":"chat.completion.chunk","created":1,"model":"gemma","choices":[]}\n\n'
                )
              );
              controller.close();
            },
          }),
          { status: 200, headers: { "Content-Type": "text/event-stream" } }
        )
      );
      const client = new InferenceClient({ baseUrl: "http://localhost:8080" });
      const consume = async () => {
        for await (const _chunk of client.generateStream(request)) {
          // Consume the stream.
        }
      };
      await expect(consume()).rejects.toThrow("ended before [DONE]");
    });
  });

  describe("embed (JSON response)", () => {
    it("should request JSON format and parse response", async () => {
      const mockResponse = {
        object: "list" as const,
        model: "bge-small-en-v1.5",
        data: [
          { object: "embedding" as const, index: 0, embedding: [0.1, 0.2, 0.3] },
          { object: "embedding" as const, index: 1, embedding: [0.4, 0.5, 0.6] },
        ],
        usage: { prompt_tokens: 2, total_tokens: 2 },
      };

      // openapi-fetch uses global fetch internally
      vi.mocked(fetch).mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: () => Promise.resolve(mockResponse),
        text: () => Promise.resolve(JSON.stringify(mockResponse)),
        headers: new Headers({ "Content-Type": "application/json" }),
      } as Response);

      const client = new InferenceClient({
        baseUrl: "http://localhost:8080/api",
      });

      const result = await client.embed("bge-small-en-v1.5", ["hello", "world"]);

      expect(result.model).toBe("bge-small-en-v1.5");
      expect(result.data).toHaveLength(2);
      expect(result.data[0]?.embedding).toEqual([0.1, 0.2, 0.3]);
      expect(result.data[1]?.embedding).toEqual([0.4, 0.5, 0.6]);

      // Verify fetch was called (openapi-fetch uses global fetch)
      expect(fetch).toHaveBeenCalled();
    });

    it("rejects and cancels declared oversized generated-client responses", async () => {
      const cancel = vi.fn();
      vi.mocked(fetch).mockResolvedValueOnce(
        new Response(
          new ReadableStream<Uint8Array>({
            start(controller) {
              controller.enqueue(new Uint8Array([123]));
            },
            cancel,
          }),
          {
            status: 200,
            headers: {
              "Content-Type": "application/json",
              "Content-Length": String((16 << 20) + 1),
            },
          }
        )
      );

      const client = new InferenceClient({ baseUrl: "http://localhost:8080" });
      await expect(client.embed("test-model", ["test"])).rejects.toThrow(
        "Inference response exceeded 16777216 bytes"
      );
      expect(cancel).toHaveBeenCalledOnce();
    });

    it("bounds streamed binary bodies across generated-client calls", async () => {
      const cancel = vi.fn();
      vi.mocked(fetch).mockResolvedValueOnce(
        new Response(
          new ReadableStream<Uint8Array>({
            start(controller) {
              controller.enqueue(new Uint8Array(17));
            },
            cancel,
          }),
          { status: 200, headers: { "Content-Type": "application/octet-stream" } }
        )
      );

      const client = new InferenceClient({
        baseUrl: "http://localhost:8080",
        maxBinaryResponseBytes: 16,
      });
      await expect(client.embed("test-model", ["test"])).rejects.toThrow(
        "Inference response exceeded 16 bytes"
      );
      expect(cancel).toHaveBeenCalledOnce();
    });

    it("leaves successful SSE streams unbuffered but still bounds SSE-labeled errors", async () => {
      vi.mocked(fetch).mockResolvedValueOnce(
        new Response(new ReadableStream({ start: (controller) => controller.close() }), {
          status: 200,
          headers: {
            "Content-Type": "text/event-stream; charset=utf-8",
            "Content-Length": String((16 << 20) + 1),
          },
        })
      );
      const client = new InferenceClient({ baseUrl: "http://localhost:8080" });
      const streamed = await client.getRawClient().POST("/ai/v1/embed", {
        body: { model: "test-model", input: ["test"] },
        headers: { Accept: "text/event-stream" },
        parseAs: "stream",
      });
      expect(streamed.data).toBeInstanceOf(ReadableStream);

      const cancel = vi.fn();
      vi.mocked(fetch).mockResolvedValueOnce(
        new Response(new ReadableStream({ cancel }), {
          status: 500,
          headers: {
            "Content-Type": "text/event-stream",
            "Content-Length": String((1 << 20) + 1),
          },
        })
      );
      await expect(client.embed("test-model", ["test"])).rejects.toThrow(
        "Inference response exceeded 1048576 bytes"
      );
      expect(cancel).toHaveBeenCalledOnce();
    });

    it("bounds successful SSE-mislabeled JSON responses when SSE was not requested", async () => {
      const cancel = vi.fn();
      vi.mocked(fetch).mockResolvedValueOnce(
        new Response(
          new ReadableStream<Uint8Array>({
            start(controller) {
              controller.enqueue(new Uint8Array((16 << 20) + 1));
            },
            cancel,
          }),
          { status: 200, headers: { "Content-Type": "text/event-stream" } }
        )
      );

      const client = new InferenceClient({ baseUrl: "http://localhost:8080" });
      await expect(client.embed("test-model", ["test"])).rejects.toThrow(
        "Inference response exceeded 16777216 bytes"
      );
      expect(cancel).toHaveBeenCalledOnce();
    });

    it("should handle embed errors", async () => {
      vi.mocked(fetch).mockResolvedValueOnce({
        ok: false,
        status: 403,
        json: () =>
          Promise.resolve({
            error: "CONTENT_NOT_ALLOWED",
            message: "remote content is blocked",
            retryable: false,
          }),
        text: () =>
          Promise.resolve(
            JSON.stringify({
              error: "CONTENT_NOT_ALLOWED",
              message: "remote content is blocked",
              retryable: false,
            })
          ),
        headers: new Headers({ "Content-Type": "application/json" }),
      } as Response);

      const client = new InferenceClient({
        baseUrl: "http://localhost:8080/api",
      });

      await expect(client.embed("invalid-model", ["test"])).rejects.toMatchObject({
        status: 403,
        code: "CONTENT_NOT_ALLOWED",
        retryable: false,
      });
    });

    it("should preserve structured transient capacity errors", async () => {
      const capacity = {
        error: "MODEL_RESOURCE_BUSY",
        message: "insufficient inference capacity is currently available",
        reason: "inference_capacity",
        retryable: true,
        retry_after_ms: 1000,
      };
      vi.mocked(fetch).mockResolvedValueOnce({
        ok: false,
        status: 503,
        json: () => Promise.resolve(capacity),
        text: () => Promise.resolve(JSON.stringify(capacity)),
        headers: new Headers({ "Content-Type": "application/json", "Retry-After": "1" }),
      } as Response);

      const client = new InferenceClient({ baseUrl: "http://localhost:8080" });
      const error = await client.embed("busy-model", ["test"]).catch((caught: unknown) => caught);

      expect(error).toBeInstanceOf(InferenceCapacityError);
      expect(error).toMatchObject({
        code: "MODEL_RESOURCE_BUSY",
        reason: "inference_capacity",
        retryable: true,
        retryAfterMs: 1000,
      });
    });
  });

  describe("embedBinary (dense-vector compatibility helper)", () => {
    it("extracts dense vectors from the current OpenAI-compatible JSON response", async () => {
      vi.mocked(fetch).mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            object: "list",
            model: "bge-small-en-v1.5",
            data: [
              { object: "embedding", index: 0, embedding: [0.1, 0.2] },
              { object: "embedding", index: 1, embedding: [0.3, 0.4] },
            ],
            usage: { prompt_tokens: 2, total_tokens: 2 },
          }),
          { status: 200, headers: { "Content-Type": "application/json; charset=utf-8" } }
        )
      );

      const client = new InferenceClient({ baseUrl: "http://localhost:8080/api" });
      await expect(client.embedBinary("bge-small-en-v1.5", ["hello", "world"])).resolves.toEqual([
        [0.1, 0.2],
        [0.3, 0.4],
      ]);
    });

    it("requests and deserializes the legacy binary format", async () => {
      const embeddings = [
        [0.1, 0.2, 0.3],
        [0.4, 0.5, 0.6],
      ];
      const binaryData = serializeEmbeddings(embeddings);

      vi.mocked(fetch).mockResolvedValueOnce(
        new Response(binaryData, {
          status: 200,
          headers: { "Content-Type": "application/octet-stream" },
        })
      );

      const client = new InferenceClient({
        baseUrl: "http://localhost:8080/api",
      });

      const result = await client.embedBinary("bge-small-en-v1.5", ["hello", "world"]);

      expect(result).toHaveLength(2);
      expect(result[0][0]).toBeCloseTo(0.1);
      expect(result[0][1]).toBeCloseTo(0.2);
      expect(result[0][2]).toBeCloseTo(0.3);
      expect(result[1][0]).toBeCloseTo(0.4);
      expect(result[1][1]).toBeCloseTo(0.5);
      expect(result[1][2]).toBeCloseTo(0.6);

      expect(fetch).toHaveBeenCalledTimes(1);
      const [url, options] = vi.mocked(fetch).mock.calls[0];
      expect(url).toBe("http://localhost:8080/ai/v1/embed");
      expect(options?.method).toBe("POST");
      expect(options?.headers).toBeDefined();
      const headers = options?.headers as Record<string, string>;
      expect(headers.Accept).toBe("application/octet-stream");
    });

    it.each([
      "inference_admission",
      "request_queue",
    ])("should parse %s capacity metadata from JSON error responses", async (reason) => {
      const capacity = {
        error: "SERVICE_UNAVAILABLE",
        message: "server at capacity, try again later",
        reason,
        retryable: true,
        retry_after_ms: 1000,
      };
      vi.mocked(fetch).mockResolvedValueOnce(
        new Response(JSON.stringify(capacity), {
          status: 503,
          headers: { "Content-Type": "application/json", "Retry-After": "1" },
        })
      );

      const client = new InferenceClient({ baseUrl: "http://localhost:8080" });
      const error = await client
        .embedBinary("busy-model", ["test"])
        .catch((caught: unknown) => caught);

      expect(error).toBeInstanceOf(InferenceCapacityError);
      expect(error).toMatchObject({ reason, retryAfterMs: 1000 });
    });

    it("should handle empty embeddings in binary response", async () => {
      const binaryData = serializeEmbeddings([]);

      vi.mocked(fetch).mockResolvedValueOnce(
        new Response(binaryData, {
          status: 200,
          headers: { "Content-Type": "application/octet-stream" },
        })
      );

      const client = new InferenceClient({
        baseUrl: "http://localhost:8080/api",
      });

      const result = await client.embedBinary("bge-small-en-v1.5", []);
      expect(result).toEqual([]);
    });

    it("should handle binary embed errors", async () => {
      vi.mocked(fetch).mockResolvedValueOnce(
        new Response("Invalid model", {
          status: 400,
          headers: { "Content-Type": "text/plain" },
        })
      );

      const client = new InferenceClient({
        baseUrl: "http://localhost:8080/api",
      });

      await expect(client.embedBinary("invalid-model", ["test"])).rejects.toThrow(
        "inference request failed (400): Invalid model"
      );
    });

    it("bounds and cancels oversized binary responses", async () => {
      const cancel = vi.fn();
      const body = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(new Uint8Array(17));
        },
        cancel,
      });
      vi.mocked(fetch).mockResolvedValueOnce(
        new Response(body, {
          status: 200,
          headers: { "Content-Type": "application/octet-stream" },
        })
      );
      const client = new InferenceClient({
        baseUrl: "http://localhost:8080/api",
        maxBinaryResponseBytes: 16,
      });

      await expect(client.embedBinary("test-model", ["test"])).rejects.toThrow(
        "Embedding response exceeded 16 bytes"
      );
      expect(cancel).toHaveBeenCalledOnce();
    });

    it("rejects sparse JSON and unregistered binary media types", async () => {
      vi.mocked(fetch).mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            object: "list",
            model: "sparse-model",
            data: [
              {
                object: "embedding",
                index: 0,
                embedding: { indices: [1], values: [0.5] },
              },
            ],
            usage: { prompt_tokens: 1, total_tokens: 1 },
          }),
          { status: 200, headers: { "Content-Type": "application/json" } }
        )
      );
      const client = new InferenceClient({ baseUrl: "http://localhost:8080/api" });
      await expect(client.embedBinary("sparse-model", ["test"])).rejects.toThrow(
        "item 0 is not a dense vector"
      );

      vi.mocked(fetch).mockResolvedValueOnce(
        new Response(serializeEmbeddings([[0.5]]), {
          status: 200,
          headers: { "Content-Type": "application/octet-streamx" },
        })
      );
      await expect(client.embedBinary("test-model", ["test"])).rejects.toThrow(
        'Unexpected embedding response content type "application/octet-streamx"'
      );
    });

    it("should handle high-dimensional embeddings in binary format", async () => {
      // Simulate 384-dimension BGE embeddings
      const dimension = 384;
      const embeddings: number[][] = [];
      for (let i = 0; i < 3; i++) {
        const vector: number[] = [];
        for (let j = 0; j < dimension; j++) {
          vector.push((Math.random() - 0.5) * 2);
        }
        embeddings.push(vector);
      }
      const binaryData = serializeEmbeddings(embeddings);

      vi.mocked(fetch).mockResolvedValueOnce(
        new Response(binaryData, {
          status: 200,
          headers: { "Content-Type": "application/octet-stream" },
        })
      );

      const client = new InferenceClient({
        baseUrl: "http://localhost:8080/api",
      });

      const result = await client.embedBinary("bge-small-en-v1.5", ["a", "b", "c"]);

      expect(result).toHaveLength(3);
      expect(result[0]).toHaveLength(384);
      expect(result[1]).toHaveLength(384);
      expect(result[2]).toHaveLength(384);

      // Verify values match (within float32 precision)
      for (let i = 0; i < 3; i++) {
        for (let j = 0; j < dimension; j++) {
          expect(result[i][j]).toBeCloseTo(embeddings[i][j], 5);
        }
      }
    });

    it("should pass truncate option in request body", async () => {
      const binaryData = serializeEmbeddings([[0.1, 0.2]]);

      vi.mocked(fetch).mockResolvedValueOnce(
        new Response(binaryData, {
          status: 200,
          headers: { "Content-Type": "application/octet-stream" },
        })
      );

      const client = new InferenceClient({
        baseUrl: "http://localhost:8080/api",
      });

      await client.embedBinary("bge-small-en-v1.5", ["test"], { truncate: true });

      const [, options] = vi.mocked(fetch).mock.calls[0];
      const body = JSON.parse(options?.body as string);
      expect(body.truncate).toBe(true);
      expect(body.model).toBe("bge-small-en-v1.5");
      expect(body.input).toEqual(["test"]);
    });
  });

  describe("extract", () => {
    it("should extract structured data from text", async () => {
      const mockResponse = {
        object: "extraction",
        model: "gliner2-base-v1",
        data: [
          {
            id: "0",
            structures: {
              person: [
                {
                  name: { value: "John Smith", score: 0.95 },
                  age: { value: "30", score: 0.88 },
                  company: { value: "Google", score: 0.91 },
                },
              ],
            },
          },
        ],
      };

      vi.mocked(fetch).mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: () => Promise.resolve(mockResponse),
        text: () => Promise.resolve(JSON.stringify(mockResponse)),
        headers: new Headers({ "Content-Type": "application/json" }),
      } as Response);

      const client = new InferenceClient({ baseUrl: "http://localhost:8080/api" });
      const result = await client.extract(
        "gliner2-base-v1",
        ["John Smith is 30 years old and works at Google."],
        { person: ["name::str", "age::str", "company::str"] },
        { includeConfidence: true }
      );

      expect(result.model).toBe("gliner2-base-v1");
      expect(result.data).toHaveLength(1);
      expect(result.data[0].structures?.person).toHaveLength(1);
      expect(fetch).toHaveBeenCalled();
    });

    it("should handle extract errors", async () => {
      vi.mocked(fetch).mockResolvedValueOnce({
        ok: false,
        status: 400,
        json: () => Promise.resolve({ error: "Model does not support extraction" }),
        text: () => Promise.resolve(JSON.stringify({ error: "Model does not support extraction" })),
        headers: new Headers({ "Content-Type": "application/json" }),
      } as Response);

      const client = new InferenceClient({ baseUrl: "http://localhost:8080/api" });
      await expect(
        client.extract("invalid-model", ["test"], { person: ["name::str"] })
      ).rejects.toMatchObject({ status: 400, code: "Model does not support extraction" });
    });

    it("should accept all optional parameters without error", async () => {
      const mockResponse = { object: "extraction", model: "gliner2-base-v1", data: [{}] };

      vi.mocked(fetch).mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: () => Promise.resolve(mockResponse),
        text: () => Promise.resolve(JSON.stringify(mockResponse)),
        headers: new Headers({ "Content-Type": "application/json" }),
      } as Response);

      const client = new InferenceClient({ baseUrl: "http://localhost:8080/api" });
      const result = await client.extract(
        "gliner2-base-v1",
        ["test"],
        { person: ["name::str"] },
        {
          threshold: 0.5,
          flatNer: false,
          includeConfidence: true,
          includeSpans: true,
        }
      );

      expect(fetch).toHaveBeenCalled();
      expect(result.model).toBe("gliner2-base-v1");
    });

    it("should classify through the canonical extract endpoint", async () => {
      const mockResponse = {
        object: "extraction",
        model: "gliner2-base-v1",
        data: [
          {
            classifications: [
              { name: "classification", label: "positive", score: 0.91 },
              { name: "classification", label: "negative", score: 0.09 },
            ],
          },
        ],
      };

      vi.mocked(fetch).mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: () => Promise.resolve(mockResponse),
        text: () => Promise.resolve(JSON.stringify(mockResponse)),
        headers: new Headers({ "Content-Type": "application/json" }),
      } as Response);

      const client = new InferenceClient({ baseUrl: "http://localhost:8080/api" });
      const result = await client.classify(
        "gliner2-base-v1",
        ["This product is excellent."],
        ["positive", "negative"],
        { multiLabel: true }
      );

      expect(result).toEqual([
        {
          index: 0,
          classifications: [
            { label: "positive", score: 0.91 },
            { label: "negative", score: 0.09 },
          ],
        },
      ]);
      const body = await lastFetchJSONBody();
      expect(body).toMatchObject({
        model: "gliner2-base-v1",
        inputs: [{ id: "0", content: "This product is excellent." }],
        schema: {
          classifications: [
            {
              name: "classification",
              labels: ["positive", "negative"],
              multi_label: true,
            },
          ],
        },
        options: {
          include_confidence: true,
        },
      });
    });

    it("should extract entities through the canonical extract endpoint", async () => {
      const mockResponse = {
        object: "extraction",
        model: "gliner2-base-v1",
        data: [
          {
            entities: [
              { text: "John Smith", label: "person", start: 0, end: 10, score: 0.95 },
              { text: "Google", label: "company", start: 20, end: 26, score: 0.9 },
            ],
          },
        ],
      };

      vi.mocked(fetch).mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: () => Promise.resolve(mockResponse),
        text: () => Promise.resolve(JSON.stringify(mockResponse)),
        headers: new Headers({ "Content-Type": "application/json" }),
      } as Response);

      const client = new InferenceClient({ baseUrl: "http://localhost:8080/api" });
      const result = await client.extractEntities(
        "gliner2-base-v1",
        ["John Smith works at Google."],
        ["person", "company"]
      );

      expect(result).toEqual([
        {
          index: 0,
          entities: [
            { text: "John Smith", label: "person", start: 0, end: 10, score: 0.95 },
            { text: "Google", label: "company", start: 20, end: 26, score: 0.9 },
          ],
        },
      ]);
      const body = await lastFetchJSONBody();
      expect(body).toMatchObject({
        model: "gliner2-base-v1",
        inputs: [{ id: "0", content: "John Smith works at Google." }],
        schema: {
          entities: ["person", "company"],
        },
        options: {
          flat_ner: true,
          include_confidence: true,
          include_spans: true,
        },
      });
    });
  });

  describe("rewrite", () => {
    it("should rewrite text using seq2seq model", async () => {
      const mockResponse = {
        model: "lmqg/flan-t5-small-squad-qg",
        texts: [["What engineer designed and built the Eiffel Tower?"]],
      };

      vi.mocked(fetch).mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: () => Promise.resolve(mockResponse),
        text: () => Promise.resolve(JSON.stringify(mockResponse)),
        headers: new Headers({ "Content-Type": "application/json" }),
      } as Response);

      const client = new InferenceClient({ baseUrl: "http://localhost:8080/api" });
      const result = await client.rewrite("lmqg/flan-t5-small-squad-qg", [
        "generate question: The Eiffel Tower is named after <hl> Gustave Eiffel <hl>.",
      ]);

      expect(result.model).toBe("lmqg/flan-t5-small-squad-qg");
      expect(result.texts).toHaveLength(1);
      expect(result.texts[0][0]).toContain("Eiffel Tower");
      expect(fetch).toHaveBeenCalled();
    });

    it("should handle rewrite errors", async () => {
      vi.mocked(fetch).mockResolvedValueOnce({
        ok: false,
        status: 400,
        json: () => Promise.resolve({ error: "Invalid model" }),
        text: () => Promise.resolve(JSON.stringify({ error: "Invalid model" })),
        headers: new Headers({ "Content-Type": "application/json" }),
      } as Response);

      const client = new InferenceClient({ baseUrl: "http://localhost:8080/api" });
      await expect(client.rewrite("invalid-model", ["test"])).rejects.toMatchObject({
        status: 400,
        code: "Invalid model",
      });
    });

    it("should handle multiple inputs", async () => {
      const mockResponse = {
        model: "flan-t5",
        texts: [["question 1"], ["question 2"]],
      };

      vi.mocked(fetch).mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: () => Promise.resolve(mockResponse),
        text: () => Promise.resolve(JSON.stringify(mockResponse)),
        headers: new Headers({ "Content-Type": "application/json" }),
      } as Response);

      const client = new InferenceClient({ baseUrl: "http://localhost:8080/api" });
      const result = await client.rewrite("flan-t5", ["input 1", "input 2"]);

      expect(result.texts).toHaveLength(2);
    });
  });

  describe("transcribe", () => {
    it("should transcribe audio", async () => {
      const mockResponse = {
        model: "openai/whisper-tiny",
        text: "Hello, how are you today?",
      };

      vi.mocked(fetch).mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: () => Promise.resolve(mockResponse),
        text: () => Promise.resolve(JSON.stringify(mockResponse)),
        headers: new Headers({ "Content-Type": "application/json" }),
      } as Response);

      const client = new InferenceClient({ baseUrl: "http://localhost:8080/api" });
      const result = await client.transcribe("base64audiodata", {
        model: "openai/whisper-tiny",
        language: "en",
      });

      expect(result.model).toBe("openai/whisper-tiny");
      expect(result.text).toBe("Hello, how are you today?");
      expect(fetch).toHaveBeenCalled();
    });

    it("should handle transcribe errors", async () => {
      vi.mocked(fetch).mockResolvedValueOnce({
        ok: false,
        status: 400,
        json: () => Promise.resolve({ error: "Invalid audio" }),
        text: () => Promise.resolve(JSON.stringify({ error: "Invalid audio" })),
        headers: new Headers({ "Content-Type": "application/json" }),
      } as Response);

      const client = new InferenceClient({ baseUrl: "http://localhost:8080/api" });
      await expect(client.transcribe("bad-data")).rejects.toMatchObject({
        status: 400,
        code: "Invalid audio",
      });
    });
  });

  describe("JSON vs Binary format comparison", () => {
    it("should return equivalent results from both formats", async () => {
      const embeddings = [
        [0.123, -0.456, 0.789],
        [-0.321, 0.654, -0.987],
      ];

      const jsonResponse = {
        object: "list" as const,
        model: "test-model",
        data: embeddings.map((embedding, index) => ({
          object: "embedding" as const,
          index,
          embedding,
        })),
        usage: { prompt_tokens: 2, total_tokens: 2 },
      };

      const binaryData = serializeEmbeddings(embeddings);

      // First call returns JSON
      vi.mocked(fetch).mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: () => Promise.resolve(jsonResponse),
        text: () => Promise.resolve(JSON.stringify(jsonResponse)),
        headers: new Headers({ "Content-Type": "application/json" }),
      } as Response);

      // Second call returns binary
      vi.mocked(fetch).mockResolvedValueOnce(
        new Response(binaryData, {
          status: 200,
          headers: { "Content-Type": "application/octet-stream" },
        })
      );

      const client = new InferenceClient({
        baseUrl: "http://localhost:8080/api",
      });

      const jsonResult = await client.embed("test-model", ["a", "b"]);
      const binaryResult = await client.embedBinary("test-model", ["a", "b"]);

      // JSON response includes model, binary does not
      expect(jsonResult.model).toBe("test-model");
      expect(jsonResult.data.length).toBe(binaryResult.length);

      // Verify embeddings are equivalent (within float32 precision)
      for (let i = 0; i < embeddings.length; i++) {
        for (let j = 0; j < embeddings[i].length; j++) {
          const jsonEmbedding = jsonResult.data[i]?.embedding;
          expect(Array.isArray(jsonEmbedding) ? jsonEmbedding[j] : undefined).toBeCloseTo(
            binaryResult[i][j],
            5
          );
        }
      }
    });
  });
});
