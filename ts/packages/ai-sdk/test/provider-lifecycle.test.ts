import { AntflyClient, InferenceClient } from "@antfly/sdk";
import { afterEach, expect, it, vi } from "vitest";
import { createAssetMiddleware } from "../src/asset/middleware.js";
import { assetProviderReference } from "../src/asset/reference.js";
import { AntflyChatLanguageModel } from "../src/language-model.js";

afterEach(() => vi.unstubAllGlobals());
it("distinct representations with equal strong ETags must upload independently", async () => {
  const uploadFile = vi.fn(async () => ({ providerReference: { vendor: "file" }, warnings: [] }));
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      const url = input instanceof Request ? input.url : String(input);
      if (url.includes("/documents/"))
        return Response.json({
          content: {
            resource: {
              type: "source",
              uri: url.includes("first") ? "https://media.test/first" : "https://media.test/second",
              mime_type: "text/plain",
              version: '"revision-1"',
              length: 1,
            },
          },
        });
      return new Response(url.endsWith("first") ? "a" : "b");
    })
  );
  const m = createAssetMiddleware({
    client: new AntflyClient({ baseUrl: "http://catalog.test" }),
    authorize: () => true,
    allowVendorUpload: true,
    files: { specificationVersion: "v4", provider: "vendor", uploadFile },
  });
  await m.transformParams?.({
    type: "generate",
    model: { provider: "vendor" } as never,
    params: {
      prompt: [
        {
          role: "user",
          content: ["first", "second"].map((key) => ({
            type: "file" as const,
            mediaType: "text/plain",
            data: {
              type: "reference" as const,
              reference: assetProviderReference({ table: "files", key }),
            },
          })),
        },
      ],
    },
  });
  expect(uploadFile).toHaveBeenCalledTimes(2);
});
it("preserves a tool name arriving after its initial delta", async () => {
  const chunks = [
    { tool_calls: [{ index: 0, id: "call-real", type: "function", function: { arguments: "" } }] },
    { tool_calls: [{ index: 0, function: { name: "lookup", arguments: "{}" } }] },
  ];
  const text = `${chunks
    .map(
      (delta) =>
        "data: " +
        JSON.stringify({ id: "c", created: 1, model: "m", choices: [{ index: 0, delta }] }) +
        "\n\n"
    )
    .join("")}data: [DONE]\n\n`;
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => new Response(text, { headers: { "content-type": "text/event-stream" } }))
  );
  const model = new AntflyChatLanguageModel({
    inference: new InferenceClient({ baseUrl: "http://inference.test" }),
    modelId: "m",
  });
  const { stream } = await model.doStream({ prompt: [] });
  const parts = [];
  const reader = stream.getReader();
  while (true) {
    const next = await reader.read();
    if (next.done) break;
    parts.push(next.value);
  }
  expect(parts.find((p) => p.type === "tool-call")).toMatchObject({
    toolName: "lookup",
    toolCallId: "call-real",
    input: "{}",
  });
});

it.each([
  { mime: "text/plain", uploads: 1 },
  { mime: "application/json", uploads: 2 },
])("shares verified bytes across assets only for matching MIME ($mime)", async ({
  mime,
  uploads,
}) => {
  const uploadFile = vi.fn(async () => ({ providerReference: { vendor: "file" }, warnings: [] }));
  const digest = "a".repeat(64);
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      const url = input instanceof Request ? input.url : String(input);
      if (url.includes("/documents/")) {
        const first = url.includes("first");
        return Response.json({
          metadata: { sha256: digest },
          content: {
            resource: {
              type: "source",
              uri: first ? "https://media.test/first" : "https://media.test/second",
              mime_type: first ? "text/plain" : mime,
              version: first ? '"etag1"' : '"etag2"',
              length: 1,
            },
          },
        });
      }
      return new Response("a");
    })
  );
  const middleware = createAssetMiddleware({
    client: new AntflyClient({ baseUrl: "http://catalog.test" }),
    authorize: () => true,
    allowVendorUpload: true,
    files: { specificationVersion: "v4", provider: "vendor", uploadFile },
  });
  await middleware.transformParams?.({
    type: "generate",
    model: { provider: "vendor" } as never,
    params: {
      prompt: [
        {
          role: "user",
          content: ["first", "second"].map((key) => ({
            type: "file" as const,
            mediaType: "text/plain",
            data: {
              type: "reference" as const,
              reference: assetProviderReference({ table: "files", key }),
            },
          })),
        },
      ],
    },
  });
  expect(uploadFile).toHaveBeenCalledTimes(uploads);
});

it.each([
  [
    { index: 0, id: "real", function: { arguments: "" } },
    { index: 0, function: { name: "look" } },
    { index: 0, function: { name: "up" } },
    { index: 0, function: { arguments: '{"x":' } },
    { index: 0, function: { arguments: "1}" } },
  ],
  [
    { index: 0, function: { arguments: '{"x":1}' } },
    { index: 0, function: { name: "lookup" } },
    { index: 0, id: "real" },
  ],
])("buffers fragmented tool metadata and early arguments (%#)", async (...deltas) => {
  const text = `${deltas.map((delta) => `data: ${JSON.stringify({ id: "c", created: 1, model: "m", choices: [{ index: 0, delta: { tool_calls: [delta] } }] })}\n\n`).join("")}data: [DONE]\n\n`;
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => new Response(text, { headers: { "content-type": "text/event-stream" } }))
  );
  const model = new AntflyChatLanguageModel({
    inference: new InferenceClient({ baseUrl: "http://inference.test" }),
    modelId: "m",
  });
  const { stream } = await model.doStream({ prompt: [] });
  const reader = stream.getReader();
  const parts = [];
  while (true) {
    const next = await reader.read();
    if (next.done) break;
    parts.push(next.value);
  }
  expect(parts.filter((p) => p.type === "tool-input-start")).toEqual([
    { type: "tool-input-start", id: "real", toolName: "lookup" },
  ]);
  expect(
    parts
      .filter((p) => p.type === "tool-input-delta")
      .map((p) => p.delta)
      .join("")
  ).toBe('{"x":1}');
  expect(parts.find((p) => p.type === "tool-call")).toMatchObject({
    toolName: "lookup",
    toolCallId: "real",
    input: '{"x":1}',
  });
});

it("keeps the cache digest out of the inference source wire contract", async () => {
  const { toInferenceMessages } = await import("../src/prompt.js");
  const source = {
    type: "source" as const,
    uri: "https://media.test/pinned",
    mime_type: "text/plain",
    version: '"etag"',
    length: 1,
  };
  const messages = await toInferenceMessages(
    [
      {
        role: "user",
        content: [
          {
            type: "file",
            mediaType: "text/plain",
            data: {
              type: "reference",
              reference: assetProviderReference({ table: "files", key: "k" }),
            },
          },
        ],
      },
    ],
    [],
    async () => ({ ...source, content_sha256: "a".repeat(64) })
  );
  expect(messages).toEqual([{ role: "user", content: [{ type: "source", source }] }]);
});
