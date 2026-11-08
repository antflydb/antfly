import { InferenceClient } from "@antfly/sdk";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AntflyRerankingModel } from "../src/reranking-model.js";

afterEach(() => vi.unstubAllGlobals());

describe("doRerank", () => {
  it("sorts documents by descending relevance and respects topN", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response(
            JSON.stringify({
              model: "rerank-test",
              data: [
                { index: 0, score: 0.2 },
                { index: 1, score: 0.9 },
                { index: 2, score: 0.5 },
              ],
            }),
            { headers: { "Content-Type": "application/json" } }
          )
      )
    );
    const model = new AntflyRerankingModel({
      inference: new InferenceClient({ baseUrl: "http://rerank.test" }),
      modelId: "rerank-test",
    });
    const result = await model.doRerank({
      query: "q",
      documents: { type: "text", values: ["a", "b", "c"] },
      topN: 2,
    });
    expect(result.ranking).toEqual([
      { index: 1, relevanceScore: 0.9 },
      { index: 2, relevanceScore: 0.5 },
    ]);
  });

  it("rejects object documents instead of silently stringifying them", async () => {
    const model = new AntflyRerankingModel({
      inference: new InferenceClient({ baseUrl: "http://rerank.test" }),
      modelId: "rerank-test",
    });
    await expect(
      model.doRerank({ query: "q", documents: { type: "object", values: [{ a: 1 }] } })
    ).rejects.toThrow(/arbitrary JSON objects/);
  });
});
