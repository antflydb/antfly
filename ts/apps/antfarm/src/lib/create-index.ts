import type { CreateIndexRequest, IndexConfig } from "@antfly/sdk";

export interface CreateIndexArguments {
  indexName: string;
  request: CreateIndexRequest;
}

/** Split the read/list index shape into the path name and name-free create body. */
export function createIndexArguments(config: IndexConfig): CreateIndexArguments {
  const { name: indexName, ...request } = config;
  return { indexName, request };
}
