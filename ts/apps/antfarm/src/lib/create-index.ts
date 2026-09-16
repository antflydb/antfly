import type { CreateIndexRequest, IndexConfig } from "@antfly/sdk";

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
