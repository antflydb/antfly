import { describe, expect, it } from "vitest";
import { createIndexArguments, indexEmbedderConfigFromForm } from "./create-index";

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

describe("indexEmbedderConfigFromForm", () => {
  it("submits OpenRouter with its model, credentials, and custom endpoint", () => {
    const embedder = {
      provider: "openrouter",
      model: "openai/text-embedding-3-small",
      // biome-ignore lint/suspicious/noTemplateCurlyInString: Server-side secret reference.
      api_key: "${secret:team.openrouter}",
      url: "https://gateway.example/api/v1",
    };
    const args = createIndexArguments({
      name: "semantic",
      type: "embeddings",
      field: "body",
      dimension: 1536,
      embedder: indexEmbedderConfigFromForm(embedder),
    });
    expect(args).toEqual({
      indexName: "semantic",
      request: { type: "embeddings", field: "body", dimension: 1536, embedder },
    });
  });

  it("allows OpenRouter endpoint and credentials to use server defaults", () => {
    expect(
      indexEmbedderConfigFromForm({
        provider: "openrouter",
        model: "openai/text-embedding-3-small",
      })
    ).toEqual({
      provider: "openrouter",
      model: "openai/text-embedding-3-small",
    });
  });

  it.each([
    { provider: "ollama", model: "nomic-embed-text", url: "http://localhost:11434" },
    { provider: "openai", model: "text-embedding-3-small", api_key: "test-key" },
    { provider: "bedrock", model: "amazon.titan-embed-text-v2:0", region: "us-east-1" },
    { provider: "antfly", model: "all-MiniLM-L6-v2" },
  ])("preserves the existing $provider configuration", (config) => {
    expect(indexEmbedderConfigFromForm(config)).toEqual(config);
  });
});
