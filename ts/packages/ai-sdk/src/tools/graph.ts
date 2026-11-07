import type { AntflyClient } from "@antfly/sdk";
import { type Tool, tool } from "ai";
import { z } from "zod";

export interface AntflyGraphToolOptions {
  client: AntflyClient;
  /** Table holding the graph index. Fixed in code; never a model-controlled argument. */
  table: string;
  /** Graph index name. Defaults to "graph"; fixed in code. */
  index?: string;
  /** Row/node cap applied to every operation. Default 100. */
  maxResults?: number;
  /** Traversal depth cap applied to every traverse/path operation. Default 6. */
  maxDepth?: number;
}

type GraphOperation = Record<string, unknown>;
const OPERATION_KINDS = ["match", "traverse", "shortest_path", "k_shortest_paths"] as const;

function clamp(value: number | undefined, max: number): number {
  return Math.max(1, Math.min(value ?? max, max));
}

/**
 * Runs bounded canonical Antfly graph JSON (`match`, `traverse`,
 * `shortest_path`, `k_shortest_paths`) against one fixed table and index,
 * via `AntflyClient.tables.query`'s `graph_queries`. The table, index and
 * result/depth bounds are fixed at construction time; the model only
 * supplies operation bodies. Cypher is not supported, matching
 * `@antfly/langchain`'s graph tool.
 *
 * Unlike `@antfly/langchain`'s `AntflyGraphStore`, this tool does not own a
 * node/edge storage convention, so it does not re-validate node identity or
 * namespace scoping beyond the table/index/limit bounds below -- it is a
 * read pass-through onto whatever graph index the caller already built.
 */
export function createGraphTool(options: AntflyGraphToolOptions): Record<string, Tool> {
  const index = options.index ?? "graph";
  const maxResults = options.maxResults ?? 100;
  const maxDepth = options.maxDepth ?? 6;

  function scoped(operations: Record<string, GraphOperation>): Record<string, GraphOperation> {
    const entries = Object.entries(operations);
    if (!entries.length || entries.length > 8) {
      throw new Error("Expected 1 to 8 named graph operations.");
    }
    const copy: Record<string, GraphOperation> = {};
    for (const [name, raw] of entries) {
      const op: GraphOperation = { ...raw };
      const kinds = OPERATION_KINDS.filter((k) => k in op);
      if (kinds.length !== 1) {
        throw new Error(
          `Graph operation '${name}' must have exactly one of ${OPERATION_KINDS.join(", ")}.`
        );
      }
      if (op.index !== undefined && op.index !== index) {
        throw new Error(`Graph operation '${name}' may not override the fixed index '${index}'.`);
      }
      op.index = index;
      const kind = kinds[0];
      const body = { ...(op[kind] as Record<string, unknown>) };
      if (kind === "match") {
        const existingReturn = (body.return as Record<string, unknown>) ?? {};
        if (existingReturn.aggregates === undefined) {
          body.return = {
            ...existingReturn,
            limit: clamp(existingReturn.limit as number, maxResults),
          };
        } else {
          body.return = existingReturn;
        }
      } else {
        body.max_depth = clamp(body.max_depth as number, maxDepth);
        if (kind === "traverse") body.limit = clamp(body.limit as number, maxResults);
        if (kind === "k_shortest_paths") body.k = clamp(body.k as number, maxResults);
      }
      op[kind] = body;
      copy[name] = op;
    }
    return copy;
  }

  const graphQuery = tool({
    description: `Run one or more named, bounded canonical Antfly graph operations (match/traverse/shortest_path/k_shortest_paths; not Cypher) against the fixed table '${options.table}' and graph index '${index}'. Pass an object mapping each operation name to its canonical body, e.g. {"neighbors": {"traverse": {"start": {"keys": ["n1"]}}}}.`,
    inputSchema: z
      .object({
        operations: z.record(z.string(), z.record(z.string(), z.unknown())),
      })
      .strict(),
    execute: async ({ operations }) => {
      const graph_queries = scoped(operations as Record<string, GraphOperation>);
      // The server's GraphQuery schema is a precise discriminated union; this
      // tool intentionally accepts the looser bounded-JSON shape the model
      // builds (validated by `scoped()` above, not the TS type) and lets the
      // server reject anything it doesn't recognize. `tables.query` returns
      // the `{ responses: [...] }` envelope (unlike `client.query`, which
      // unwraps it), already checked against this request by
      // `validateGraphQueryResponses` inside the SDK.
      const envelope = await options.client.tables.query(options.table, {
        graph_queries: graph_queries as never,
        limit: 0,
      });
      const first = (envelope as { responses?: { graph_results?: unknown }[] } | undefined)
        ?.responses?.[0];
      return first?.graph_results ?? {};
    },
  });
  return { antfly_graph_query: graphQuery };
}
