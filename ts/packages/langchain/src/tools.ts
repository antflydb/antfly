import { DynamicStructuredTool } from "@langchain/core/tools";
import { z } from "zod";
import type { AntflyGraphStore } from "./graphs.js";
import type { AntflyRetriever } from "./retriever.js";
/** Fixed scopes live on the supplied adapters; model arguments cannot change them. */
export function createAntflySearchTool(retriever: AntflyRetriever) {
  return new DynamicStructuredTool({
    name: "antfly_search",
    description: "Search the configured knowledge corpus. Returns documents with citation IDs.",
    schema: z.object({ query: z.string().min(1).max(10000) }).strict(),
    func: async (input: { query: string }) => JSON.stringify(await retriever.invoke(input.query)),
  });
}
export function createAntflyGraphTools(graph: AntflyGraphStore) {
  return [
    new DynamicStructuredTool({
      name: "antfly_graph_schema",
      description: "Inspect the configured graph schema and JSON dialect.",
      schema: z.object({}).strict(),
      func: async () => {
        if (!graph.structuredSchema.discovery_complete) await graph.refreshSchema();
        return graph.getSchema();
      },
    }),
    new DynamicStructuredTool({
      name: "antfly_graph_query",
      description:
        "Run bounded canonical Antfly graph JSON in the fixed graph namespace. Cypher is unsupported.",
      schema: z.object({ query: z.string().min(1).max(100000) }).strict(),
      func: async (input: { query: string }) => JSON.stringify(await graph.query(input.query)),
    }),
  ] as const;
}
