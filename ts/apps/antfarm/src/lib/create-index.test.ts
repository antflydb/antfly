import { describe, expect, it } from "vitest";
import { createIndexArguments } from "./create-index";

describe("createIndexArguments", () => {
  it("moves the index name to the request path arguments", () => {
    expect(
      createIndexArguments({
        name: "semantic",
        type: "embeddings",
        dimension: 384,
        embedder: { provider: "antfly", model: "test" },
      })
    ).toEqual({
      indexName: "semantic",
      request: {
        type: "embeddings",
        dimension: 384,
        embedder: { provider: "antfly", model: "test" },
      },
    });
  });
});
