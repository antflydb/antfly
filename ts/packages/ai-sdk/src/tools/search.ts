import type { AntflyClient } from "@antfly/sdk";
import { type Tool, tool } from "ai";
import { z } from "zod";

export interface AntflySearchToolOptions {
  client: AntflyClient;
  /** Table to search. Fixed in code; never a model-controlled argument. */
  table: string;
  /**
   * Embedding indexes to combine with full-text search via RRF. The server
   * embeds the query string itself when these are set, so no embedding
   * model needs to be wired in here.
   */
  indexes?: string[];
  /** Fields to project. Defaults to the server's default projection. */
  fields?: string[];
  /** Most rows a search can return, whatever limit the model asks for. Default 10. */
  maxResults?: number;
}

/**
 * A single search tool over `AntflyClient.tables.query`, scoped to one
 * fixed table (and optionally fixed semantic indexes) at construction time.
 * Combines full-text and (when `indexes` is set) semantic search with RRF,
 * matching the hybrid-search pattern documented in
 * `specs/openapi/antfly/metadata.yaml`.
 */
export function createSearchTool(options: AntflySearchToolOptions): Record<string, Tool> {
  const maxResults = options.maxResults ?? 10;
  const search = tool({
    description: `Search the Antfly table '${options.table}' for documents relevant to a query${
      options.indexes?.length ? " (hybrid full-text + semantic search)" : " (full-text search)"
    }.`,
    inputSchema: z
      .object({
        query: z.string().min(1).max(10000),
        limit: z.number().int().min(1).max(100).optional(),
      })
      .strict(),
    execute: async ({ query, limit }, { abortSignal }) => {
      abortSignal?.throwIfAborted();
      const response = await options.client.tables.query(
        options.table,
        {
          full_text_search: { query },
          ...(options.indexes?.length ? { semantic_search: query, indexes: options.indexes } : {}),
          fields: options.fields,
          limit: Math.min(limit ?? maxResults, maxResults),
        },
        { signal: abortSignal }
      );
      return response;
    },
  });
  return { antfly_search: search };
}
