/**
 * Type exports and utilities for the Antfly SDK
 * Re-exports commonly used types from the generated OpenAPI types
 */

import type { components, operations } from "./public-api.js";
import type { components as QueryComponents } from "./query.js";

// Antfly Query type for type-safe query construction
export type AntflyQuery = QueryComponents["schemas"]["Query"];

// Export individual Antfly query types for convenience
export type TermQuery = QueryComponents["schemas"]["TermQuery"];
export type MatchQuery = QueryComponents["schemas"]["MatchQuery"];
export type MatchPhraseQuery = QueryComponents["schemas"]["MatchPhraseQuery"];
export type PhraseQuery = QueryComponents["schemas"]["PhraseQuery"];
export type MultiPhraseQuery = QueryComponents["schemas"]["MultiPhraseQuery"];
export type FuzzyQuery = QueryComponents["schemas"]["FuzzyQuery"];
export type PrefixQuery = QueryComponents["schemas"]["PrefixQuery"];
export type RegexpQuery = QueryComponents["schemas"]["RegexpQuery"];
export type WildcardQuery = QueryComponents["schemas"]["WildcardQuery"];
export type QueryStringQuery = QueryComponents["schemas"]["QueryStringQuery"];
export type NumericRangeQuery = QueryComponents["schemas"]["NumericRangeQuery"];
export type TermRangeQuery = QueryComponents["schemas"]["TermRangeQuery"];
export type DateRangeStringQuery = QueryComponents["schemas"]["DateRangeStringQuery"];
export type BooleanQuery = QueryComponents["schemas"]["BooleanQuery"];
export type ConjunctionQuery = QueryComponents["schemas"]["ConjunctionQuery"];
export type DisjunctionQuery = QueryComponents["schemas"]["DisjunctionQuery"];
export type MatchAllQuery = QueryComponents["schemas"]["MatchAllQuery"];
export type MatchNoneQuery = QueryComponents["schemas"]["MatchNoneQuery"];
export type DocIdQuery = QueryComponents["schemas"]["DocIdQuery"];
export type BoolFieldQuery = QueryComponents["schemas"]["BoolFieldQuery"];
export type IPRangeQuery = QueryComponents["schemas"]["IPRangeQuery"];
export type GeoBoundingBoxQuery = QueryComponents["schemas"]["GeoBoundingBoxQuery"];
export type GeoDistanceQuery = QueryComponents["schemas"]["GeoDistanceQuery"];
export type GeoBoundingPolygonQuery = QueryComponents["schemas"]["GeoBoundingPolygonQuery"];
export type GeoShapeQuery = QueryComponents["schemas"]["GeoShapeQuery"];
export type Boost = QueryComponents["schemas"]["Boost"];
export type Fuzziness = QueryComponents["schemas"]["Fuzziness"];

// Request/Response types - Override with proper Antfly query types
export type QueryRequest = Omit<
  components["schemas"]["QueryRequest"],
  "full_text_search" | "filter_query" | "exclusion_query" | "graph_queries"
> & {
  /** Full JSON Antfly search query with proper type checking */
  full_text_search?: AntflyQuery;
  /** Full JSON Antfly filter query with proper type checking */
  filter_query?: AntflyQuery;
  /** Full JSON Antfly exclusion query with proper type checking */
  exclusion_query?: AntflyQuery;
  /** Named graph queries; MATCH node filters use the same Antfly query AST. */
  graph_queries?: Record<string, GraphQuery>;
};
export type QueryResult = components["schemas"]["QueryResult"];
export type QueryHit = components["schemas"]["QueryHit"];
export type QueryHitsTotal = components["schemas"]["QueryHitsTotal"];
export type QueryResponses = components["schemas"]["QueryResponses"];

export interface QueryHitsTotalFormatOptions {
  locale?: Intl.LocalesArgument;
  singular?: string;
  plural?: string;
}

/** Returns structured hit-count metadata, including whether the value is exact. */
export function queryResultHitsTotal(
  result: QueryResult | null | undefined
): QueryHitsTotal | undefined {
  return result?.hits?.total ?? undefined;
}

/**
 * Returns only the numeric hit-count value.
 *
 * For user-facing output, inspect `total.relation` or use `formatQueryHitsTotal`
 * so lower-bound totals are not presented as exact counts.
 */
export function queryHitsTotalValue(total: QueryHitsTotal | null | undefined): number | undefined {
  return total?.value;
}

/** Returns true for exact totals, false for lower-bound totals, and undefined when absent. */
export function queryHitsTotalIsExact(
  total: QueryHitsTotal | null | undefined
): boolean | undefined {
  return total == null ? undefined : total.relation === "exact";
}

/** Formats hit-count metadata without losing lower-bound semantics. */
export function formatQueryHitsTotal(
  total: QueryHitsTotal | null | undefined,
  options: QueryHitsTotalFormatOptions = {}
): string | undefined {
  if (total == null) return undefined;
  const singular = options.singular ?? "hit";
  const plural = options.plural ?? `${singular}s`;
  const noun = total.value === 1 ? singular : plural;
  const prefix = total.relation === "gte" ? ">= " : "";
  return `${prefix}${new Intl.NumberFormat(options.locale).format(total.value)} ${noun}`;
}

/**
 * Returns only the numeric hit-count value from a query result.
 *
 * For user-facing output, prefer `queryResultHitsTotal` plus
 * `formatQueryHitsTotal` so lower-bound totals are not rendered as exact.
 */
export function queryResultTotalHits(result: QueryResult | null | undefined): number | undefined {
  return queryHitsTotalValue(queryResultHitsTotal(result));
}

// Fix BatchRequest to allow any object for inserts
export interface BatchRequest {
  inserts?: Record<string, unknown>;
  deletes?: string[];
  transforms?: components["schemas"]["Transform"][];
  sync_level?: components["schemas"]["SyncLevel"];
}
export type BatchResult = components["schemas"]["BatchResponse"];
export interface MultiBatchRequest {
  tables: Record<string, BatchRequest>;
  sync_level?: components["schemas"]["SyncLevel"];
}
export type MultiBatchResult = components["schemas"]["MultiBatchResponse"];
export type LinearMergeRequest = components["schemas"]["LinearMergeRequest"];
export type LinearMergeResult = components["schemas"]["LinearMergeResult"];
export interface WriteOptions {
  /**
   * Maximum encoded JSON request body size in bytes.
   * Non-positive or omitted values use the SDK default.
   */
  maxRequestBytes?: number;
  /**
   * Maximum success response body size in bytes.
   * Non-positive or omitted values use the SDK default.
   */
  maxResponseBytes?: number;
  /** AbortSignal to cancel the request. */
  signal?: AbortSignal;
}

// Table types
export type Table = components["schemas"]["Table"];
export type CreateTableRequest = components["schemas"]["CreateTableRequest"];
export type TableSchema = components["schemas"]["TableSchema"];
export type TableMigration = components["schemas"]["TableMigration"];
export type TableStatus = components["schemas"]["TableStatus"];

// Document artifact types
export type DocumentArtifactChildRange = components["schemas"]["DocumentArtifactChildRange"];
export type DocumentArtifactManifest = components["schemas"]["DocumentArtifactManifest"];
export type DocumentArtifactManifestList = components["schemas"]["DocumentArtifactManifestList"];
export type DocumentArtifactReprocessResponse =
  components["schemas"]["DocumentArtifactReprocessResponse"];
export type DocumentArtifactTableReprocessRequest =
  components["schemas"]["DocumentArtifactTableReprocessRequest"];
export type DocumentArtifactTableReprocessResponse =
  components["schemas"]["DocumentArtifactTableReprocessResponse"];
export type DocumentArtifactReprocessFailure =
  components["schemas"]["DocumentArtifactReprocessFailure"];
export type DocumentArtifactReprocessShardCursor =
  components["schemas"]["DocumentArtifactReprocessShardCursor"];
export type DocumentArtifactReprocessJobStartRequest =
  components["schemas"]["DocumentArtifactReprocessJobStartRequest"];
export type DocumentArtifactReprocessJob = components["schemas"]["DocumentArtifactReprocessJob"];
export type TableArtifactEnrichmentList = components["schemas"]["TableArtifactEnrichmentList"];
export type EnrichmentConfig = components["schemas"]["EnrichmentConfig"];

// Index types
export type IndexConfig = components["schemas"]["IndexConfig"];
export type IndexType = components["schemas"]["IndexType"];
export type IndexStatus = components["schemas"]["IndexStatus"];

// Graph index types
export type GraphIndexConfig = components["schemas"]["GraphIndexConfig"];
export type EdgeTypeConfig = components["schemas"]["EdgeTypeConfig"];
export type EdgeTopology = NonNullable<EdgeTypeConfig["topology"]>;

// Graph query and traversal types
export type Edge = components["schemas"]["Edge"];
export type EdgeDirection = components["schemas"]["EdgeDirection"];
export type EdgesResponse = components["schemas"]["EdgesResponse"];
export type TraversalRules = components["schemas"]["TraversalRules"];
export type TraversalResult = components["schemas"]["TraversalResult"];
export type GraphMatchNode = Omit<components["schemas"]["GraphMatchNode"], "filter"> & {
  filter?: AntflyQuery;
};
export type GraphOptionalMatch = Omit<components["schemas"]["GraphOptionalMatch"], "nodes"> & {
  nodes?: Record<string, GraphMatchNode>;
};
export type GraphMatch = Omit<components["schemas"]["GraphMatch"], "nodes" | "optional"> & {
  nodes: Record<string, GraphMatchNode>;
  optional?: GraphOptionalMatch[];
};
export type GraphMatchQuery = Omit<components["schemas"]["GraphMatchQuery"], "match"> & {
  match: GraphMatch;
};
export type GraphTraversal = Omit<components["schemas"]["GraphTraversal"], "filter"> & {
  filter?: AntflyQuery;
};
export type GraphTraverseQuery = Omit<components["schemas"]["GraphTraverseQuery"], "traverse"> & {
  traverse: GraphTraversal;
};
export type GraphShortestPath = Omit<components["schemas"]["GraphShortestPath"], "filter"> & {
  filter?: AntflyQuery;
};
export type GraphShortestPathQuery = Omit<
  components["schemas"]["GraphShortestPathQuery"],
  "shortest_path"
> & { shortest_path: GraphShortestPath };
export type GraphKShortestPaths = Omit<components["schemas"]["GraphKShortestPaths"], "filter"> & {
  filter?: AntflyQuery;
};
export type GraphKShortestPathsQuery = Omit<
  components["schemas"]["GraphKShortestPathsQuery"],
  "k_shortest_paths"
> & { k_shortest_paths: GraphKShortestPaths };
export type GraphQuery =
  | GraphMatchQuery
  | GraphTraverseQuery
  | GraphShortestPathQuery
  | GraphKShortestPathsQuery;
/** @deprecated Use GraphQuery through QueryRequest.graph_queries. */
export type LegacyGraphQuery = components["schemas"]["LegacyGraphQuery"];
/** @deprecated Compatibility type for LegacyGraphQuery. */
export type GraphQueryType = components["schemas"]["GraphQueryType"];
/** @deprecated Compatibility type for LegacyGraphQuery. */
export type GraphQueryParams = components["schemas"]["GraphQueryParams"];
/** @deprecated Compatibility type for LegacyGraphQuery. */
export type PatternStep = components["schemas"]["PatternStep"];
/** @deprecated Compatibility type for legacy graph result matches. */
export type PatternMatch = components["schemas"]["PatternMatch"];
export type GraphQueryResult = components["schemas"]["GraphQueryResult"];
export type GraphResultNode = components["schemas"]["GraphResultNode"];
export type GraphNodeSelector = components["schemas"]["GraphNodeSelector"];
export type GraphReturn = components["schemas"]["GraphReturn"];
export type GraphWhereExpression = components["schemas"]["GraphWhereExpression"];
export type PathWeightMode = components["schemas"]["PathWeightMode"];

// User and permission types
export type User = components["schemas"]["User"];
export type CreateUserRequest = components["schemas"]["CreateUserRequest"];
export type UpdatePasswordRequest = components["schemas"]["UpdatePasswordRequest"];
export type Permission = components["schemas"]["Permission"];
export type ResourceType = components["schemas"]["ResourceType"];
export type PermissionType = components["schemas"]["PermissionType"];

// Backup/Restore types
export type BackupRequest = components["schemas"]["BackupRequest"];
export type RestoreRequest = components["schemas"]["RestoreRequest"];
export type ClusterRestoreRequest = components["schemas"]["ClusterRestoreRequest"];
export type RestoreJob = components["schemas"]["RestoreJob"];

// Lookup/Scan types
export type ScanKeysRequest = Omit<components["schemas"]["ScanKeysRequest"], "filter_query"> & {
  /** Full JSON Antfly filter query with proper type checking */
  filter_query?: AntflyQuery;
};

// Schema types
export type DocumentSchema = components["schemas"]["DocumentSchema"];

// Embedding types
export type Embedding = components["schemas"]["Embedding"];
export type DenseEmbedding = number[];
export type SparseEmbedding = { indices: number[]; values: number[] };
/** Base64-encoded string of little-endian float32 bytes (~4x more compact than DenseEmbedding). */
export type PackedDenseEmbedding = string;
/** Packed sparse embedding with base64-encoded little-endian uint32 indices and float32 values. */
export type PackedSparseEmbedding = {
  packed_indices: string;
  packed_values: string;
};

// Search and aggregation types
export type AggregationType = components["schemas"]["AggregationType"];
export type AggregationRequest = components["schemas"]["AggregationRequest"];
export type AggregationResult = components["schemas"]["AggregationResult"];
export type AggregationBucket = components["schemas"]["AggregationBucket"];
export type CalendarInterval = components["schemas"]["CalendarInterval"];
export type DistanceUnit = components["schemas"]["DistanceUnit"];
export type SignificanceAlgorithm = components["schemas"]["SignificanceAlgorithm"];
export type AggregationRange = components["schemas"]["AggregationRange"];
export type AggregationDateRange = components["schemas"]["AggregationDateRange"];
export type DistanceRange = components["schemas"]["DistanceRange"];
export type AntflyType = components["schemas"]["AntflyType"];
export type FieldMappingType = components["schemas"]["FieldMappingType"];
export type DocumentFieldMapping = components["schemas"]["DocumentFieldMapping"];
export type DocumentSubfieldMapping = components["schemas"]["DocumentSubfieldMapping"];
export type TemplateFieldMapping = components["schemas"]["TemplateFieldMapping"];
export type DynamicTemplate = components["schemas"]["DynamicTemplate"];

// Connection types
export type ConnectionsResponse = components["schemas"]["ConnectionsResponse"];
export type Connection = components["schemas"]["Connection"];
export type ConnectionKind = components["schemas"]["ConnectionKind"];
export type ConnectionStatus = components["schemas"]["ConnectionStatus"];
export type InferenceProviderType = components["schemas"]["InferenceProviderType"];
export type ConnectedModel = components["schemas"]["ConnectedModel"];
export type ConnectedModelType = components["schemas"]["ConnectedModelType"];
export type InferenceConnection = components["schemas"]["InferenceConnection"];
export type ExternalIoProtocol = components["schemas"]["ExternalIoProtocol"];
export type ExternalIoConnection = components["schemas"]["ExternalIoConnection"];
export type CdcConnection = components["schemas"]["CdcConnection"];

// Model and reranker types
export type EmbedderConfig = components["schemas"]["EmbedderConfig"];
export type RerankerConfig = components["schemas"]["RerankerConfig"];
export type GeneratorConfig = components["schemas"]["GeneratorConfig"];
export type EmbedderProvider = components["schemas"]["EmbedderProvider"];
export const embedderProviders: components["schemas"]["EmbedderProvider"][] = [
  "antfly",
  "ollama",
  "gemini",
  "vertex",
  "openai",
  "openrouter",
  "bedrock",
  "cohere",
  "mock",
];
export type GeneratorProvider = components["schemas"]["GeneratorProvider"];
export const generatorProviders: components["schemas"]["GeneratorProvider"][] = [
  "antfly",
  "ollama",
  "gemini",
  "openai",
  "anthropic",
  "vertex",
  "cohere",
  "openrouter",
];

// AI response types
export type ClassificationTransformationResult =
  components["schemas"]["ClassificationTransformationResult"];
export type RouteType = components["schemas"]["RouteType"];
export type QueryStrategy = components["schemas"]["QueryStrategy"];
export type SemanticQueryMode = components["schemas"]["SemanticQueryMode"];

// GenerationConfidence is a convenience type for the confidence assessment fields
// on RetrievalAgentResult (generation_confidence + context_relevance)
export interface GenerationConfidence {
  generation_confidence: number;
  context_relevance: number;
}

// Query Builder Agent types
export type QueryBuilderRequest = components["schemas"]["QueryBuilderRequest"];
export type QueryBuilderResult = components["schemas"]["QueryBuilderResult"];

// Chat/Retrieval types (used by retrieval agent's tool-calling mode)
export type ChatMessage = components["schemas"]["ChatMessage"];
export type ChatMessageRole = components["schemas"]["ChatMessageRole"];
export type ToolCall = components["schemas"]["ToolCall"];
export type ToolCallFunction = components["schemas"]["ToolCallFunction"];
export type ChatToolName = components["schemas"]["ChatToolName"];
export type ChatToolsConfig = components["schemas"]["ChatToolsConfig"];
export type FilterSpec = components["schemas"]["FilterSpec"];
export type AgentDecision = components["schemas"]["AgentDecision"];
export type AgentQuestion = components["schemas"]["AgentQuestion"];
export type AgentQuestionKind = components["schemas"]["AgentQuestionKind"];
export type AgentStatus = components["schemas"]["AgentStatus"];
export type AgentStep = components["schemas"]["AgentStep"];
export type AgentStepKind = components["schemas"]["AgentStepKind"];
export type AgentStepStatus = components["schemas"]["AgentStepStatus"];
export type WebSearchConfig = components["schemas"]["WebSearchConfig"];
export type FetchConfig = components["schemas"]["FetchConfig"];

// Eval types
export type EvalConfig = components["schemas"]["EvalConfig"];
export type EvalRequest = components["schemas"]["EvalRequest"];
export type EvalResult = components["schemas"]["EvalResult"];
export type EvalScores = components["schemas"]["EvalScores"];
export type EvaluatorScore = components["schemas"]["EvaluatorScore"];
export type EvalSummary = components["schemas"]["EvalSummary"];
export type EvaluatorName = components["schemas"]["EvaluatorName"];

// Error type
export type AntflyError = components["schemas"]["Error"];

// Join types
export type JoinClause = components["schemas"]["JoinClause"];
export type JoinCondition = components["schemas"]["JoinCondition"];
export type JoinFilters = components["schemas"]["JoinFilters"];
export type JoinOperator = components["schemas"]["JoinOperator"];
export type JoinProfile = components["schemas"]["JoinProfile"];
export type JoinStrategy = components["schemas"]["JoinStrategy"];
export type JoinType = components["schemas"]["JoinType"];

// Query profiling types
export type QueryProfile = components["schemas"]["QueryProfile"];
export type SortProfile = components["schemas"]["SortProfile"];
export type ShardsProfile = components["schemas"]["ShardsProfile"];
export type RerankerProfile = components["schemas"]["RerankerProfile"];
export type MergeProfile = components["schemas"]["MergeProfile"];

// Utility type for extracting response data
export type ResponseData<T extends keyof operations> = operations[T]["responses"] extends {
  200: infer R;
}
  ? R extends { content: { "application/json": infer D } }
    ? D
    : never
  : never;

// Authentication configuration for the client
export type AntflyAuth =
  | { type: "basic"; username: string; password: string }
  | { type: "apiKey"; keyId: string; keySecret: string }
  | { type: "token"; token: string }
  | { username: string; password: string }; // backwards compat (no 'type' field)

// Configuration types for the client
export interface AntflyConfig {
  baseUrl: string;
  headers?: Record<string, string>;
  auth?: AntflyAuth;
}

// Retrieval Agent types
export type RetrievalAgentRequest = components["schemas"]["RetrievalAgentRequest"];
export type RetrievalAgentResult = components["schemas"]["RetrievalAgentResult"];
export type RetrievalAgentSteps = components["schemas"]["RetrievalAgentSteps"];
export type SSEStepStarted = components["schemas"]["SSEStepStarted"];

// Retrieval Agent streaming callbacks for structured SSE events
export interface RetrievalAgentStreamCallbacks {
  onClassification?: (data: ClassificationTransformationResult) => void;
  onReasoning?: (chunk: string) => void;
  onHit?: (hit: QueryHit) => void;
  onGeneration?: (chunk: string) => void;
  onConfidence?: (data: GenerationConfidence) => void;
  onFollowup?: (question: string) => void;
  onEvalResult?: (data: EvalResult) => void;
  onFilterApplied?: (filter: FilterSpec) => void;
  onSearchExecuted?: (data: { query: string }) => void;
  onStepStarted?: (step: SSEStepStarted) => void;
  onStepProgress?: (data: Record<string, unknown>) => void;
  onStepCompleted?: (step: AgentStep) => void;
  onDone?: (data: RetrievalAgentResult) => void;
  onError?: (error: string) => void;
}

// Chat Agent convenience types for multi-turn conversation
export interface ChatAgentConfig {
  /** Generator configuration (provider, model, temperature) */
  generator: GeneratorConfig;
  /** Table to search */
  table?: string;
  /** Semantic indexes to use */
  semanticIndexes?: string[];
  /** Domain-specific knowledge for the agent */
  agentKnowledge?: string;
  /** System prompt override */
  systemPrompt?: string;
  /** Maximum tool iterations per turn */
  maxInternalIterations?: number;
  /** Number of follow-up questions to generate */
  followUpCount?: number;
  /** Results limit per search */
  limit?: number;
  /** Retrieval agent steps configuration */
  steps?: RetrievalAgentSteps;
}

/** Callbacks for chat agent streaming, extending retrieval callbacks with chat-specific events */
export interface ChatStreamCallbacks extends RetrievalAgentStreamCallbacks {
  /** Called when the assistant's full message is assembled after streaming completes */
  onAssistantMessage?: (message: string) => void;
  /** Called with the updated full message history after each turn */
  onMessagesUpdated?: (messages: ChatMessage[]) => void;
}

/** Result from a chat agent turn */
export interface ChatAgentTurnResult {
  /** The retrieval agent result for this turn */
  result: RetrievalAgentResult;
  /** Updated conversation history including this turn */
  messages: ChatMessage[];
}

// Web search result from the web_search tool
export interface WebSearchResultItem {
  title: string;
  url: string;
  snippet: string;
  source?: string;
}

// Helper type for query building with proper Antfly query types
export interface QueryOptions {
  table?: string;
  fullTextSearch?: AntflyQuery;
  semanticSearch?: string;
  limit?: number;
  offset?: number;
  fields?: string[];
  orderBy?: Record<string, boolean>;
  aggregations?: Record<string, AggregationRequest>;
}
