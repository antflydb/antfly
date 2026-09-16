import { describe, expect, it } from "vitest";
import { createIndexArguments } from "./create-index";

describe("createIndexArguments", () => {
  it("preserves ordered composite relational keys on the shared index route", () => {
    const keys = [{ column: "tenant" }, { column: "created_at", direction: "desc" as const }];
    expect(createIndexArguments({ name: "recent", type: "relational", keys })).toEqual({
      indexName: "recent",
      request: { type: "relational", keys },
    });
  });

  it("rejects relational configs without ordered keys", () => {
    expect(() => createIndexArguments({ name: "invalid", type: "relational" })).toThrow(
      "Relational indexes require"
    );
  });
  it("preserves expression keys from the raw JSON editor on the shared route", () => {
    const keys = [
      { column: "tenant" },
      {
        expression: {
          op: "lower_ascii" as const,
          args: [{ op: "column" as const, column: "email" }],
        },
        result_type: "string" as const,
      },
    ];
    expect(
      createIndexArguments({ name: "email", type: "relational", keys, include_columns: ["email"] })
    ).toEqual({
      indexName: "email",
      request: { type: "relational", keys, include_columns: ["email"] },
    });
  });

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
