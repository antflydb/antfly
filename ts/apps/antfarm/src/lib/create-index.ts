import type { CreateIndexRequest, IndexConfig, IndexEmbedderConfig } from "@antfly/sdk";

export interface CreateIndexArguments {
  indexName: string;
  request: CreateIndexRequest;
}

/** Split the read/list index shape into the path name and name-free create body. */
export function createIndexArguments(config: IndexConfig): CreateIndexArguments {
  const { name: indexName, ...request } = config;
  return { indexName, request };
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
