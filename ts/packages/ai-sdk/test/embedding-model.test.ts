import { InferenceClient } from "@antfly/sdk";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AntflyEmbeddingModel } from "../src/embedding-model.js";

afterEach(() => vi.unstubAllGlobals());

describe("doEmbed", () => {
  it("returns dense embeddings in input order with usage", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response(
            JSON.stringify({
              object: "list",
              model: "embed-test",
              data: [
                { object: "embedding", index: 0, embedding: [0.1, 0.2] },
                { object: "embedding", index: 1, embedding: [0.3, 0.4] },
              ],
              usage: { prompt_tokens: 4, total_tokens: 4 },
            }),
            { headers: { "Content-Type": "application/json" } }
          )
      )
    );
    const model = new AntflyEmbeddingModel({
      inference: new InferenceClient({ baseUrl: "http://embed.test" }),
      modelId: "embed-test",
    });
    const result = await model.doEmbed({ values: ["a", "b"] });
    expect(result.embeddings).toEqual([
      [0.1, 0.2],
      [0.3, 0.4],
    ]);
    expect(result.usage).toEqual({ tokens: 4 });
  });

  it("throws on a sparse embedding rather than silently truncating it", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response(
            JSON.stringify({
              object: "list",
              model: "embed-test",
              data: [{ object: "embedding", index: 0, embedding: { indices: [0], values: [1] } }],
            }),
            { headers: { "Content-Type": "application/json" } }
          )
      )
    );
    const model = new AntflyEmbeddingModel({
      inference: new InferenceClient({ baseUrl: "http://embed.test" }),
      modelId: "embed-test",
    });
    await expect(model.doEmbed({ values: ["a"] })).rejects.toThrow(/sparse/);
  });

  it("throws when the response carries per-input errors", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response(
            JSON.stringify({
              object: "list",
              model: "embed-test",
              data: [],
              errors: [
                {
                  index: 0,
                  code: "bad_input",
                  message: "nope",
                  stage: "parse",
                  retryable: false,
                  status: 400,
                },
              ],
            }),
            { headers: { "Content-Type": "application/json" } }
          )
      )
    );
    const model = new AntflyEmbeddingModel({
      inference: new InferenceClient({ baseUrl: "http://embed.test" }),
      modelId: "embed-test",
    });
    await expect(model.doEmbed({ values: ["a"] })).rejects.toThrow(/failed for 1 of 1/);
  });
});
