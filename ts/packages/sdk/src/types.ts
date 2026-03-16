/**
 * Type exports and utilities for the Antfly SDK
 * Re-exports commonly used types from the generated OpenAPI types
 */

import type { components, operations } from "./antfly-api.js";
import type { components as BleveComponents } from "./bleve-query.js";

// Bleve Query type for type-safe query construction
export type BleveQuery = BleveComponents["schemas"]["Query"];

// Export individual Bleve query types for convenience
export type TermQuery = BleveComponents["schemas"]["TermQuery"];
export type MatchQuery = BleveComponents["schemas"]["MatchQuery"];
export type MatchPhraseQuery = BleveComponents["schemas"]["MatchPhraseQuery"];
export type PhraseQuery = BleveComponents["schemas"]["PhraseQuery"];
export type MultiPhraseQuery = BleveComponents["schemas"]["MultiPhraseQuery"];
export type FuzzyQuery = BleveComponents["schemas"]["FuzzyQuery"];
export type PrefixQuery = BleveComponents["schemas"]["PrefixQuery"];
export type RegexpQuery = BleveComponents["schemas"]["RegexpQuery"];
export type WildcardQuery = BleveComponents["schemas"]["WildcardQuery"];
export type QueryStringQuery = BleveComponents["schemas"]["QueryStringQuery"];
export type NumericRangeQuery = BleveComponents["schemas"]["NumericRangeQuery"];
export type TermRangeQuery = BleveComponents["schemas"]["TermRangeQuery"];
export type DateRangeStringQuery = BleveComponents["schemas"]["DateRangeStringQuery"];
export type BooleanQuery = BleveComponents["schemas"]["BooleanQuery"];
export type ConjunctionQuery = BleveComponents["schemas"]["ConjunctionQuery"];
export type DisjunctionQuery = BleveComponents["schemas"]["DisjunctionQuery"];
export type MatchAllQuery = BleveComponents["schemas"]["MatchAllQuery"];
export type MatchNoneQuery = BleveComponents["schemas"]["MatchNoneQuery"];
export type DocIdQuery = BleveComponents["schemas"]["DocIdQuery"];
export type BoolFieldQuery = BleveComponents["schemas"]["BoolFieldQuery"];
export type IPRangeQuery = BleveComponents["schemas"]["IPRangeQuery"];
export type GeoBoundingBoxQuery = BleveComponents["schemas"]["GeoBoundingBoxQuery"];
export type GeoDistanceQuery = BleveComponents["schemas"]["GeoDistanceQuery"];
export type GeoBoundingPolygonQuery = BleveComponents["schemas"]["GeoBoundingPolygonQuery"];
export type GeoShapeQuery = BleveComponents["schemas"]["GeoShapeQuery"];
export type Boost = BleveComponents["schemas"]["Boost"];
export type Fuzziness = BleveComponents["schemas"]["Fuzziness"];

// Request/Response types - Override with proper Bleve query types
export type QueryRequest = Omit<
  components["schemas"]["QueryRequest"],
  "full_text_search" | "filter_query" | "exclusion_query"
> & {
  /** Full JSON Bleve search query with proper type checking */
  full_text_search?: BleveQuery;
  /** Full JSON Bleve filter query with proper type checking */
  filter_query?: BleveQuery;
  /** Full JSON Bleve exclusion query with proper type checking */
  exclusion_query?: BleveQuery;
};
export type QueryResult = components["schemas"]["QueryResult"];
export type QueryHit = components["schemas"]["QueryHit"];
export type QueryResponses = components["schemas"]["QueryResponses"];

// Fix BatchRequest to allow any object for inserts
export interface BatchRequest {
  inserts?: Record<string, unknown>;
  deletes?: string[];
}

// Table types
export type Table = components["schemas"]["Table"];
export type CreateTableRequest = components["schemas"]["CreateTableRequest"];
export type TableSchema = components["schemas"]["TableSchema"];
export type TableMigration = components["schemas"]["TableMigration"];
export type TableStatus = components["schemas"]["TableStatus"];

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
export type GraphQuery = components["schemas"]["GraphQuery"];
export type GraphQueryResult = components["schemas"]["GraphQueryResult"];
export type GraphResultNode = components["schemas"]["GraphResultNode"];
export type GraphQueryType = components["schemas"]["GraphQueryType"];
export type GraphNodeSelector = components["schemas"]["GraphNodeSelector"];
export type GraphQueryParams = components["schemas"]["GraphQueryParams"];

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

// Lookup/Scan types
export type ScanKeysRequest = Omit<components["schemas"]["ScanKeysRequest"], "filter_query"> & {
  /** Full JSON Bleve filter query with proper type checking */
  filter_query?: BleveQuery;
};

// Schema types
export type DocumentSchema = components["schemas"]["DocumentSchema"];

// Embedding types
export type Embedding = components["schemas"]["Embedding"];
export type DenseEmbedding = number[];
export type SparseEmbedding = { indices: number[]; values: number[] };

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

// Model and reranker types
export type EmbedderConfig = components["schemas"]["EmbedderConfig"];
export type RerankerConfig = components["schemas"]["RerankerConfig"];
export type GeneratorConfig = components["schemas"]["GeneratorConfig"];
export type EmbedderProvider = components["schemas"]["EmbedderProvider"];
export const embedderProviders: components["schemas"]["EmbedderProvider"][] = [
  "termite",
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
  "ollama",
  "gemini",
  "openai",
  "anthropic",
  "vertex",
  "cohere",
  "termite",
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
export type ChatToolCall = components["schemas"]["ChatToolCall"];
export type ChatToolResult = components["schemas"]["ChatToolResult"];
export type ChatToolName = components["schemas"]["ChatToolName"];
export type ChatToolsConfig = components["schemas"]["ChatToolsConfig"];
export type FilterSpec = components["schemas"]["FilterSpec"];
export type ClarificationRequest = components["schemas"]["ClarificationRequest"];
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
  | { type: "bearer"; token: string }
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
export type RetrievalReasoningStep = components["schemas"]["RetrievalReasoningStep"];

// Retrieval Agent streaming callbacks for structured SSE events
export interface RetrievalAgentStreamCallbacks {
  onClassification?: (data: ClassificationTransformationResult) => void;
  onReasoning?: (chunk: string) => void;
  onHit?: (hit: QueryHit) => void;
  onGeneration?: (chunk: string) => void;
  onConfidence?: (data: GenerationConfidence) => void;
  onFollowup?: (question: string) => void;
  onEvalResult?: (data: EvalResult) => void;
  onClarificationRequired?: (data: ClarificationRequest) => void;
  onFilterApplied?: (filter: FilterSpec) => void;
  onSearchExecuted?: (data: { query: string }) => void;
  onStepStarted?: (step: { id: string; step: string; action: string }) => void;
  onStepProgress?: (data: Record<string, unknown>) => void;
  onStepCompleted?: (step: RetrievalReasoningStep) => void;
  onDone?: (data?: { complete: boolean }) => void;
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
  maxIterations?: number;
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

// Web search result from websearch tool
export interface WebSearchResultItem {
  title: string;
  url: string;
  snippet: string;
  source?: string;
}

// Helper type for query building with proper Bleve query types
export interface QueryOptions {
  table?: string;
  fullTextSearch?: BleveQuery;
  semanticSearch?: string;
  limit?: number;
  offset?: number;
  fields?: string[];
  orderBy?: Record<string, boolean>;
  aggregations?: Record<string, AggregationRequest>;
}
