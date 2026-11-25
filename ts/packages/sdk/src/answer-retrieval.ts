import type { RetrievalAgentRequest } from "./types.js";

export interface AnswerRetrievalOptions {
  /** Planned retrieval starts from a table scope. Semantic retrieval starts from the user's text. */
  retrievalMode?: "planned" | "semantic";
  semanticIndexes?: string[];
  maxInternalIterations?: number;
  fields?: string[];
  limit?: number;
  filterQuery?: Record<string, unknown>;
  exclusionQuery?: Record<string, unknown>;
  tools?: RetrievalAgentRequest["tools"];
}

export function answerRetrieval(
  table: string | undefined,
  text: string,
  options: AnswerRetrievalOptions
): Pick<RetrievalAgentRequest, "queries" | "tools" | "max_internal_iterations"> {
  const indexes = options.semanticIndexes?.length ? options.semanticIndexes : undefined;
  const mode = options.retrievalMode ?? (indexes ? "semantic" : "planned");
  if (mode === "semantic" && !indexes) {
    throw new Error("Semantic answer retrieval requires at least one semanticIndexes entry.");
  }
  if (mode === "planned" && options.maxInternalIterations === 0) {
    throw new Error("Planned answer retrieval requires a positive tool budget.");
  }
  return {
    queries: [
      {
        table,
        semantic_search: mode === "semantic" ? text : undefined,
        indexes,
        fields: options.fields,
        limit: options.limit ?? 10,
        filter_query: options.filterQuery,
        exclusion_query: options.exclusionQuery,
      },
    ],
    max_internal_iterations: options.maxInternalIterations ?? (mode === "planned" ? 8 : undefined),
    tools:
      options.tools ??
      (mode === "planned"
        ? {
            enabled_tools: indexes
              ? ["full_text_search", "semantic_search", "add_filter"]
              : ["full_text_search", "add_filter"],
          }
        : undefined),
  };
}
