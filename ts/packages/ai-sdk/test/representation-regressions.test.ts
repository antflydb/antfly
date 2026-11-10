import { once } from "node:events";
import { createServer } from "node:http";
import { gzipSync } from "node:zlib";
import { AntflyClient, InferenceClient } from "@antfly/sdk";
import { generateText, stepCountIs } from "ai";
import { afterEach, expect, it, vi } from "vitest";
import { createAssetDisplayHandler } from "../src/asset/display-route.js";
import { encodeAssetReference } from "../src/asset/reference.js";
import { AntflyChatLanguageModel } from "../src/language-model.js";
import { createMemoryTools } from "../src/tools/memory.js";

afterEach(() => vi.unstubAllGlobals());
it("display headers describe the returned representation", async () => {
  const bytes = Buffer.from("hello".repeat(1000));
  const compressed = gzipSync(bytes);
  const server = createServer((req, res) => {
    const identity = req.headers["accept-encoding"] === "identity";
    const body = identity ? bytes : compressed;
    res.setHeader("Content-Type", "text/plain");
    res.setHeader("Content-Length", body.length);
    if (!identity) res.setHeader("Content-Encoding", "gzip");
    res.end(body);
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  if (!address || typeof address === "string") throw Error("No address");
  try {
    const resource = {
      type: "source",
      uri: `http://127.0.0.1:${address.port}/media`,
      mime_type: "text/plain",
      version: '"v1"',
      length: bytes.length,
    };
    const client = {
      getRawClient: () => ({
        GET: async () => ({ response: new Response(), data: { content: { resource } } }),
      }),
    } as never;
    const handler = createAssetDisplayHandler({
      client,
      authorize: () => true,
      referenceId: () => encodeAssetReference({ table: "files", key: "one" }),
    });
    const response = await handler(new Request("http://app.test/display"));
    const actual = await response.arrayBuffer();
    expect(actual.byteLength).toBe(bytes.length);
    expect(Number(response.headers.get("Content-Length"))).toBe(actual.byteLength);
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});
it("memory tool outputs can complete an AI SDK tool loop with full-width integers", async () => {
  let generations = 0;
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = input instanceof Request ? input : new Request(input, init);
      if (request.url.includes("/memory/v1/"))
        return new Response(
          '{"id":"op1","action":"observe","request_hash":"hash","owner":"user","revision":18446744073709551615,"config_generation":1,"created_at":1,"phase":"succeeded","evidence_accepted":true,"assertions_ready":true,"attempts":1,"error_code":null,"next_retry_at":null}',
          { headers: { "Content-Type": "application/json" } }
        );
      generations++;
      return Response.json({
        id: "r",
        model: "m",
        created: 1,
        choices: [
          {
            message:
              generations === 1
                ? {
                    role: "assistant",
                    tool_calls: [
                      {
                        id: "c1",
                        type: "function",
                        function: {
                          name: "antfly_memory_remember",
                          arguments: JSON.stringify({
                            subject: "Alice",
                            predicate: "owns",
                            value: "operations",
                            statement: "Alice owns operations.",
                          }),
                        },
                      },
                    ],
                  }
                : { role: "assistant", content: "Saved." },
            finish_reason: generations === 1 ? "tool_calls" : "stop",
          },
        ],
        usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
      });
    })
  );
  const client = new AntflyClient({ baseUrl: "http://memory.test" });
  const model = new AntflyChatLanguageModel({
    modelId: "m",
    inference: new InferenceClient({ baseUrl: "http://inference.test" }),
  });
  const result = await generateText({
    model,
    tools: createMemoryTools({ client, space: "s" }),
    prompt: "Remember Alice owns operations.",
    stopWhen: stepCountIs(2),
  });
  expect(result.text).toBe("Saved.");
  expect(generations).toBe(2);
});
