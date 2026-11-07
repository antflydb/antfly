import { AntflyClient, InferenceAPIError, InferenceClient } from "@antfly/sdk";
import { afterEach, describe, expect, it, vi } from "vitest";
import { assetProviderReference } from "../src/asset/reference.js";
import { AntflyChatLanguageModel } from "../src/language-model.js";

afterEach(() => vi.unstubAllGlobals());

function model(): AntflyChatLanguageModel {
  return new AntflyChatLanguageModel({
    inference: new InferenceClient({ baseUrl: "http://generate.test" }),
    modelId: "test-model",
  });
}

describe("doGenerate", () => {
  it("converts a text response into a text content part", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response(
            JSON.stringify({
              id: "chatcmpl-1",
              object: "chat.completion",
              created: 1700000000,
              model: "test-model",
              choices: [
                {
                  index: 0,
                  message: { role: "assistant", content: "hello" },
                  finish_reason: "stop",
                },
              ],
              usage: { prompt_tokens: 5, completion_tokens: 2, total_tokens: 7 },
            }),
            { headers: { "Content-Type": "application/json" } }
          )
      )
    );
    const result = await model().doGenerate({
      prompt: [{ role: "user", content: [{ type: "text", text: "hi" }] }],
    });
    expect(result.content).toEqual([{ type: "text", text: "hello" }]);
    expect(result.finishReason).toEqual({ unified: "stop", raw: "stop" });
    expect(result.usage.inputTokens.total).toBe(5);
    expect(result.usage.outputTokens.total).toBe(2);
    expect(result.response?.id).toBe("chatcmpl-1");
  });

  it("converts a tool-call response into a tool-call content part", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response(
            JSON.stringify({
              id: "chatcmpl-2",
              object: "chat.completion",
              created: 1700000000,
              model: "test-model",
              choices: [
                {
                  index: 0,
                  message: {
                    role: "assistant",
                    tool_calls: [
                      {
                        id: "call_1",
                        type: "function",
                        function: { name: "foo", arguments: '{"a":1}' },
                      },
                    ],
                  },
                  finish_reason: "tool_calls",
                },
              ],
            }),
            { headers: { "Content-Type": "application/json" } }
          )
      )
    );
    const result = await model().doGenerate({
      prompt: [{ role: "user", content: [{ type: "text", text: "call foo" }] }],
      tools: [{ type: "function", name: "foo", inputSchema: { type: "object", properties: {} } }],
    });
    expect(result.content).toEqual([
      { type: "tool-call", toolCallId: "call_1", toolName: "foo", input: '{"a":1}' },
    ]);
    expect(result.finishReason.unified).toBe("tool-calls");
  });

  it("surfaces a gateway error as InferenceAPIError rather than swallowing it", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response(
            JSON.stringify({
              error: "video_limit_exceeded",
              message: "Local Gemma 4 accepts at most one video per request",
              retryable: false,
            }),
            { status: 400, headers: { "Content-Type": "application/json" } }
          )
      )
    );
    await expect(
      model().doGenerate({ prompt: [{ role: "user", content: [{ type: "text", text: "hi" }] }] })
    ).rejects.toSatisfy((error: unknown) => {
      expect(error).toBeInstanceOf(InferenceAPIError);
      expect((error as InferenceAPIError).code).toBe("video_limit_exceeded");
      return true;
    });
  });

  it("warns instead of applying unsupported stopSequences/seed", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (_input: RequestInfo | URL, init?: RequestInit) => {
        const body = JSON.parse(String(init?.body));
        expect(body.stop).toBeUndefined();
        expect(body.seed).toBeUndefined();
        return new Response(
          JSON.stringify({
            id: "c",
            object: "chat.completion",
            created: 1,
            model: "test-model",
            choices: [
              { index: 0, message: { role: "assistant", content: "ok" }, finish_reason: "stop" },
            ],
          }),
          { headers: { "Content-Type": "application/json" } }
        );
      })
    );
    const result = await model().doGenerate({
      prompt: [{ role: "user", content: [{ type: "text", text: "hi" }] }],
      stopSequences: ["STOP"],
      seed: 42,
    });
    expect(result.warnings).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ type: "unsupported", feature: "stopSequences" }),
        expect.objectContaining({ type: "unsupported", feature: "seed" }),
      ])
    );
  });
});

describe("doStream", () => {
  it("streams text deltas and a finish part", async () => {
    const sse =
      `data: ${JSON.stringify({
        id: "c1",
        object: "chat.completion.chunk",
        created: 1,
        model: "test-model",
        choices: [{ index: 0, delta: { role: "assistant", content: "Hi" } }],
      })}\n\n` +
      `data: ${JSON.stringify({
        id: "c1",
        object: "chat.completion.chunk",
        created: 1,
        model: "test-model",
        choices: [{ index: 0, delta: { content: " there" }, finish_reason: "stop" }],
      })}\n\n` +
      "data: [DONE]\n\n";
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response(sse, { headers: { "Content-Type": "text/event-stream" } }))
    );
    const { stream } = await model().doStream({
      prompt: [{ role: "user", content: [{ type: "text", text: "hi" }] }],
    });
    const parts: unknown[] = [];
    for await (const part of stream as unknown as AsyncIterable<unknown>) parts.push(part);
    const types = parts.map((p) => (p as { type: string }).type);
    expect(types).toEqual([
      "stream-start",
      "response-metadata",
      "text-start",
      "text-delta",
      "text-delta",
      "text-end",
      "finish",
    ]);
    const deltas = parts.filter((p) => (p as { type: string }).type === "text-delta") as {
      delta: string;
    }[];
    expect(deltas.map((d) => d.delta).join("")).toBe("Hi there");
    const finish = parts.at(-1) as { finishReason: { unified: string } };
    expect(finish.finishReason.unified).toBe("stop");
  });

  it("streams tool-call deltas assembled from incremental arguments", async () => {
    const sse =
      `data: ${JSON.stringify({
        id: "c1",
        object: "chat.completion.chunk",
        created: 1,
        model: "test-model",
        choices: [
          {
            index: 0,
            delta: {
              role: "assistant",
              tool_calls: [
                {
                  index: 0,
                  id: "call_1",
                  type: "function",
                  function: { name: "foo", arguments: "" },
                },
              ],
            },
          },
        ],
      })}\n\n` +
      `data: ${JSON.stringify({
        id: "c1",
        object: "chat.completion.chunk",
        created: 1,
        model: "test-model",
        choices: [
          { index: 0, delta: { tool_calls: [{ index: 0, function: { arguments: '{"a":1}' } }] } },
        ],
      })}\n\n` +
      `data: ${JSON.stringify({
        id: "c1",
        object: "chat.completion.chunk",
        created: 1,
        model: "test-model",
        choices: [{ index: 0, delta: {}, finish_reason: "tool_calls" }],
      })}\n\n` +
      "data: [DONE]\n\n";
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response(sse, { headers: { "Content-Type": "text/event-stream" } }))
    );
    const { stream } = await model().doStream({
      prompt: [{ role: "user", content: [{ type: "text", text: "call foo" }] }],
    });
    const parts: { type: string; [key: string]: unknown }[] = [];
    for await (const part of stream as unknown as AsyncIterable<{ type: string }>) {
      parts.push(part as { type: string });
    }
    expect(parts.map((p) => p.type)).toEqual([
      "stream-start",
      "response-metadata",
      "tool-input-start",
      "tool-input-delta",
      "tool-input-end",
      "tool-call",
      "finish",
    ]);
    const call = parts.find((p) => p.type === "tool-call") as unknown as {
      toolCallId: string;
      input: string;
    };
    expect(call.toolCallId).toBe("call_1");
    expect(call.input).toBe('{"a":1}');
  });
});

describe("providerReference.antfly file parts", () => {
  const reference = { table: "files", key: "row-1" };
  const resource = {
    type: "source" as const,
    uri: "https://sources.example.com/v1/media/abc/def",
    mime_type: "video/mp4",
    version: '"etag-1"',
    length: 4,
  };

  it("resolves the reference into a typed source content part when constructed with a client", async () => {
    let generateBody: string | undefined;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url =
          typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
        if (url.includes("/db/v1/tables/")) {
          return new Response(JSON.stringify({ content: { resource } }), {
            headers: { "Content-Type": "application/json", "x-antfly-version": "1" },
          });
        }
        generateBody = String(init?.body);
        return new Response(
          JSON.stringify({
            id: "c",
            object: "chat.completion",
            created: 1,
            model: "test-model",
            choices: [
              { index: 0, message: { role: "assistant", content: "ok" }, finish_reason: "stop" },
            ],
          }),
          { headers: { "Content-Type": "application/json" } }
        );
      })
    );
    const withClient = new AntflyChatLanguageModel({
      inference: new InferenceClient({ baseUrl: "http://generate.test" }),
      client: new AntflyClient({ baseUrl: "http://generate.test" }),
      authorizeAsset: () => true,
      modelId: "test-model",
    });
    await withClient.doGenerate({
      prompt: [
        {
          role: "user",
          content: [
            { type: "text", text: "describe" },
            {
              type: "file",
              mediaType: "video/mp4",
              data: { type: "reference", reference: assetProviderReference(reference) },
            },
          ],
        },
      ],
    });
    const body = JSON.parse(generateBody ?? "{}");
    expect(body.messages[0].content[1]).toEqual({ type: "source", source: resource });
  });

  it("throws a clear error for a providerReference.antfly part when constructed without a client", async () => {
    await expect(
      model().doGenerate({
        prompt: [
          {
            role: "user",
            content: [
              {
                type: "file",
                mediaType: "video/mp4",
                data: { type: "reference", reference: assetProviderReference(reference) },
              },
            ],
          },
        ],
      })
    ).rejects.toThrow(/authorizeAsset/);
  });

  it("refuses forged references without an access decision, or against one", async () => {
    const part = {
      type: "file" as const,
      mediaType: "video/mp4",
      data: {
        type: "reference" as const,
        reference: assetProviderReference({ table: "secrets", key: "x" }),
      },
    };
    const options = { prompt: [{ role: "user" as const, content: [part] }] };
    const inference = {
      generate: vi.fn(async () => {
        throw new Error("must not reach the model");
      }),
    } as never;
    const client = { getRawClient: () => ({ GET: vi.fn() }) } as never;
    const unconfigured = new AntflyChatLanguageModel({ inference, modelId: "m", client });
    await expect(unconfigured.doGenerate(options as never)).rejects.toThrow("authorizeAsset");
    const denied = new AntflyChatLanguageModel({
      inference,
      modelId: "m",
      client,
      authorizeAsset: () => false,
    });
    await expect(denied.doGenerate(options as never)).rejects.toThrow("denied");
    const throwing = new AntflyChatLanguageModel({
      inference,
      modelId: "m",
      client,
      authorizeAsset: () => {
        throw new Error("session lookup failed");
      },
    });
    await expect(throwing.doGenerate(options as never)).rejects.toThrow("denied");
  });
});

it("replays parallel tool results as ordered individual wire messages", async () => {
  const { toInferenceMessages } = await import("../src/prompt.js");
  const messages = await toInferenceMessages(
    [
      {
        role: "assistant",
        content: [
          { type: "tool-call", toolCallId: "a", toolName: "search", input: { q: "a" } },
          { type: "tool-call", toolCallId: "b", toolName: "search", input: { q: "b" } },
        ],
      },
      {
        role: "tool",
        content: [
          {
            type: "tool-result",
            toolCallId: "a",
            toolName: "search",
            output: { type: "text", value: "first" },
          },
          {
            type: "tool-result",
            toolCallId: "b",
            toolName: "search",
            output: { type: "json", value: { result: "second" } },
          },
        ],
      },
      { role: "user", content: [{ type: "text", text: "continue" }] },
    ],
    []
  );
  expect(messages.slice(1)).toEqual([
    { role: "tool", tool_call_id: "a", content: "first" },
    { role: "tool", tool_call_id: "b", content: '{"result":"second"}' },
    { role: "user", content: [{ type: "text", text: "continue" }] },
  ]);
});
