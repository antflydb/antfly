import { InferenceClient } from "@antfly/sdk";
import { afterEach, expect, it, vi } from "vitest";
import { AntflyEmbeddingModel } from "../src/embedding-model.js";
import { AntflyChatLanguageModel } from "../src/language-model.js";
import { AntflyRerankingModel } from "../src/reranking-model.js";

afterEach(() => vi.unstubAllGlobals());

it.each([
  "generate",
  "stream",
  "embed",
  "rerank",
])("forwards per-call %s headers without losing configured defaults", async (operation) => {
  let captured: Headers | undefined;
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = input instanceof Request ? input : new Request(input, init);
      captured = request.headers;
      if (operation === "embed")
        return Response.json({ object: "list", data: [{ index: 0, embedding: [1, 2] }] });
      if (operation === "rerank")
        return Response.json({ object: "list", data: [{ index: 0, score: 1 }] });
      if (operation === "stream")
        return new Response(
          'data: {"id":"r","model":"m","created":1,"choices":[{"delta":{"content":"ok"},"finish_reason":"stop"}]}\n\ndata: [DONE]\n\n',
          { headers: { "Content-Type": "text/event-stream" } }
        );
      return Response.json({
        id: "r",
        model: "m",
        created: 1,
        choices: [{ message: { role: "assistant", content: "ok" }, finish_reason: "stop" }],
      });
    })
  );
  const inference = new InferenceClient({
    baseUrl: "http://inference.test",
    headers: { Authorization: "Bearer configured", "X-Default": "retained" },
  });
  const headers = {
    authorization: "Bearer caller",
    "X-Request-ID": "per-call",
  };
  const language = new AntflyChatLanguageModel({ inference, modelId: "m" });
  const prompt = [{ role: "user" as const, content: [{ type: "text" as const, text: "hi" }] }];
  if (operation === "generate") await language.doGenerate({ prompt, headers });
  else if (operation === "stream") {
    const { stream } = await language.doStream({ prompt, headers });
    const reader = stream.getReader();
    try {
      while (!(await reader.read()).done) {
        /* Drain the transport. */
      }
    } finally {
      reader.releaseLock();
    }
  } else if (operation === "embed")
    await new AntflyEmbeddingModel({ inference, modelId: "m" }).doEmbed({
      values: ["hi"],
      headers,
    });
  else
    await new AntflyRerankingModel({ inference, modelId: "m" }).doRerank({
      query: "hi",
      documents: { type: "text", values: ["hi"] },
      headers,
    });
  expect(captured?.get("Authorization")).toBe("Bearer caller");
  expect(captured?.get("X-Request-ID")).toBe("per-call");
  expect(captured?.get("X-Default")).toBe("retained");
  expect(captured?.get("Accept")).toBe(
    operation === "stream" ? "text/event-stream" : "application/json"
  );
});
