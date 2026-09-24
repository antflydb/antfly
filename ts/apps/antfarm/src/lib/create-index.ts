import type { CreateIndexRequest, IndexConfig, IndexEmbedderConfig } from "@antfly/sdk";

export interface CreateIndexArguments {
  indexName: string;
  request: CreateIndexRequest;
}

/** Split the read/list index shape into the path name and name-free create body. */
export function createIndexArguments(config: IndexConfig): CreateIndexArguments {
  const { name: indexName, ...request } = config;
  // IndexConfig's legacy oneOf does not correlate `type` with its config
  // member. Narrow at this boundary instead of casting an invalid request.
  switch (request.type) {
    case "relational":
      if (!("keys" in request) || request.keys.length === 0 || request.keys.length > 32) {
        throw new TypeError("Relational indexes require between 1 and 32 ordered keys.");
      }
      return { indexName, request: { ...request, type: "relational", keys: request.keys } };
    case "full_text":
      return { indexName, request: { ...request, type: "full_text" } };
    case "embeddings":
      return { indexName, request: { ...request, type: "embeddings" } };
    case "graph":
      return { indexName, request: { ...request, type: "graph" } };
    case "algebraic":
      return { indexName, request: { ...request, type: "algebraic" } };
  }
}

/** Convert the provider fields from the index form into the create request. */
export function indexEmbedderConfigFromForm({
  provider,
  model,
  api_key,
  url,
  region,
}: {
  provider: string;
  model: string;
  api_key?: string;
  url?: string;
  region?: string;
}): IndexEmbedderConfig {
  switch (provider) {
    case "ollama":
      return { provider, model, url };
    case "openai":
    case "openrouter":
      return { provider, model, api_key, url };
    case "bedrock":
      return { provider, model, region };
    case "antfly":
      return { provider, model };
    default:
      throw new Error("Invalid provider");
  }
}
