import type {
  ClassificationTransformationResult,
  EvalResult,
  GenerationConfidence,
  QueryHit,
  RetrievalAgentStreamCallbacks,
} from "@antfly/sdk";
import {
  AntflyClient,
  type QueryRequest,
  type QueryResponses,
  type RetrievalAgentRequest,
  type RetrievalAgentResult,
} from "@antfly/sdk";
import qs from "qs";

export interface MultiqueryRequest {
  query: QueryRequest;
}

let defaultClient: AntflyClient | null = null;

export function initializeAntflyClient(
  url: string,
  headers: Record<string, string> = {}
): AntflyClient {
  defaultClient = new AntflyClient({
    baseUrl: url,
    headers,
  });
  return defaultClient;
}

export function getAntflyClient(): AntflyClient {
  if (!defaultClient) {
    throw new Error("AntflyClient not initialized. Call initializeAntflyClient first.");
  }
  return defaultClient;
}

export async function multiquery(
  url: string,
  msearchData: MultiqueryRequest[],
  headers: Record<string, string> = {}
): Promise<QueryResponses | undefined> {
  try {
    let client = defaultClient;

    if (!client) {
      client = initializeAntflyClient(url, headers);
    }

    const queries = msearchData.map((item) => item.query);
    const result = await client.multiquery(queries);
    return result;
  } catch (error) {
    console.error("Failed to connect to Antfly:", error);

    return {
      responses: msearchData.map(() => ({
        status: 500,
        took: 0,
        error: error instanceof Error ? error.message : "Connection failed",
      })),
    };
  }
}

export function conjunctsFrom(queries?: Map<string, unknown>): Record<string, unknown> {
  if (!queries) return { match_all: {} };
  if (queries.size === 0) return { match_none: {} };
  if (queries.size === 1) return queries.values().next().value as Record<string, unknown>;
  const conjuncts = Array.from(queries.values()).filter(
    (a) => !(a && typeof a === "object" && "match_all" in a && Object.keys(a).length === 1)
  );
  if (conjuncts.length === 0) return { match_all: {} };
  if (conjuncts.length === 1) return conjuncts[0] as Record<string, unknown>;
  return { conjuncts };
}

export function disjunctsFrom(queries?: Array<Record<string, unknown>>): Record<string, unknown> {
  if (!queries) return { match_all: {} };
  if (queries.length === 0) return { match_none: {} };
  if (queries.length === 1) return queries[0];
  const disjuncts = Array.from(queries.values()).filter(
    (a) => !(a && typeof a === "object" && "match_all" in a && Object.keys(a).length === 1)
  );
  if (disjuncts.length === 0) return { match_all: {} };
  if (disjuncts.length === 1) return disjuncts[0] as Record<string, unknown>;
  return { disjuncts };
}

export function toTermQueries(
  fields: string[] = [],
  selectedValues: string[] = []
): Array<Record<string, unknown>> {
  const queries = fields.flatMap((field) =>
    selectedValues.map((value) => {
      return { field, match: value };
    })
  );
  if (queries.length === 0) return [{ match_all: {} }];
  return queries;
}

export function fromUrlQueryString(str = ""): Map<string, unknown> {
  return new Map([
    ...Object.entries(qs.parse(str?.replace(/^\?/, "") || "")).map(([k, v]) => {
      try {
        return [k, typeof v === "string" ? JSON.parse(v) : v] as [string, unknown];
      } catch {
        return [k, v] as [string, unknown];
      }
    }),
  ]);
}

export function toUrlQueryString(params: Map<string, unknown>): string {
  return qs.stringify(
    Object.fromEntries(
      Array.from(params)
        .filter(([, v]) => (Array.isArray(v) ? v.length : v))
        .map(([k, v]) => [k, JSON.stringify(v)])
    )
  );
}

export const defer = (f: () => void): void => {
  queueMicrotask(f);
};

// Table resolution helpers for Option 3 (multi-table support)

/**
 * Normalize table parameter to array format (internal use)
 * Supports future multi-table queries while maintaining backwards compatibility
 */
export function normalizeTable(table?: string | string[]): string[] {
  if (!table) return [];
  return Array.isArray(table) ? table : [table];
}

/**
 * Resolve which table to use for a widget query
 * Priority: widget.table > defaultTable
 * Returns single table for Phase 1 (can be extended to return array in Phase 2)
 */
export function resolveTable(
  widgetTable: string | string[] | undefined,
  defaultTable: string
): string {
  if (widgetTable) {
    // If widget has table override, use it (take first if array)
    const tables = normalizeTable(widgetTable);
    return tables[0] || defaultTable;
  }
  return defaultTable;
}

// Retrieval Agent streaming types and functions
export interface AnswerCallbacks {
  onClassification?: (data: ClassificationTransformationResult) => void;
  onReasoning?: (chunk: string) => void;
  onHit?: (hit: QueryHit) => void;
  onGeneration?: (chunk: string) => void;
  onConfidence?: (data: GenerationConfidence) => void;
  onFollowup?: (question: string) => void;
  onEvalResult?: (data: EvalResult) => void;
  onStepStarted?: (step: import("@antfly/sdk").SSEStepStarted) => void;
  onStepCompleted?: (step: import("@antfly/sdk").AgentStep) => void;
  onComplete?: () => void;
  onError?: (error: Error | string) => void;
  onRetrievalAgentResult?: (result: RetrievalAgentResult) => void;
}

/**
 * Stream Retrieval Agent results from the Antfly /agents/retrieval endpoint using Server-Sent Events or JSON
 * @param url - Base URL of the Antfly server (e.g., http://localhost:8080/api/v1)
 * @param request - Retrieval agent request with query, mode, and optional step configs
 * @param headers - Optional HTTP headers for authentication
 * @param callbacks - Structured callbacks for retrieval events (classification, hits, reasoning, answer, follow-up, complete, error)
 * @returns AbortController to cancel the stream
 */
export async function streamAnswer(
  url: string,
  request: RetrievalAgentRequest,
  headers: Record<string, string> = {},
  callbacks: AnswerCallbacks
): Promise<AbortController> {
  try {
    // Always create a fresh client with the base URL
    const client = new AntflyClient({
      baseUrl: url,
      headers,
    });

    // Determine if we should stream based on presence of streaming callbacks
    const shouldStream = !!(
      callbacks.onClassification ||
      callbacks.onReasoning ||
      callbacks.onHit ||
      callbacks.onGeneration ||
      callbacks.onConfidence ||
      callbacks.onFollowup ||
      callbacks.onStepStarted ||
      callbacks.onStepCompleted
    );

    // Build the request with streaming flag
    const retrievalRequest: RetrievalAgentRequest = {
      ...request,
      stream: shouldStream,
    };

    // Build SDK callbacks if streaming
    const sdkCallbacks: RetrievalAgentStreamCallbacks | undefined = shouldStream
      ? {
          onClassification: callbacks.onClassification,
          onReasoning: callbacks.onReasoning,
          onHit: callbacks.onHit,
          onGeneration: callbacks.onGeneration,
          onStepStarted: callbacks.onStepStarted,
          onStepCompleted: callbacks.onStepCompleted,
          onConfidence: callbacks.onConfidence,
          onFollowup: callbacks.onFollowup,
          onEvalResult: callbacks.onEvalResult,
          onDone: (result) => {
            callbacks.onRetrievalAgentResult?.(result);
            if (callbacks.onComplete) {
              callbacks.onComplete();
            }
          },
          onError: (error: string) => {
            if (callbacks.onError) {
              callbacks.onError(error);
            }
          },
        }
      : undefined;

    // Call the retrieval agent endpoint
    const result = await client.retrievalAgent(retrievalRequest, sdkCallbacks);

    // Handle non-streaming response (RetrievalAgentResult)
    if (result && typeof result === "object" && "generation" in result) {
      if (callbacks.onRetrievalAgentResult) {
        callbacks.onRetrievalAgentResult(result as RetrievalAgentResult);
      }
      if (callbacks.onComplete) {
        callbacks.onComplete();
      }
      return new AbortController(); // Return a dummy controller for consistency
    }

    // Handle streaming response (AbortController)
    if (result && typeof result === "object" && "abort" in result) {
      return result as AbortController;
    }

    // Fallback
    if (callbacks.onComplete) {
      callbacks.onComplete();
    }
    return new AbortController();
  } catch (error) {
    if (error instanceof Error) {
      if (error.name === "AbortError") {
        // Stream was aborted - this is expected behavior
      } else if (callbacks.onError) {
        callbacks.onError(error);
      }
    } else if (callbacks.onError) {
      callbacks.onError(new Error("Unknown error occurred during retrieval agent streaming"));
    }
    return new AbortController();
  }
}
