/**
 * Antfly SDK for TypeScript
 *
 * A TypeScript SDK for interacting with the Antfly API, suitable for both
 * frontend and backend applications.
 *
 * @example
 * ```typescript
 * import { AntflyClient } from '@antfly/sdk';
 *
 * const client = new AntflyClient({
 *   baseUrl: 'http://localhost:8080',
 *   auth: {
 *     username: 'admin',
 *     password: 'password'
 *   }
 * });
 *
 * // Query data
 * const results = await client.query({
 *   table: 'products',
 *   limit: 10
 * });
 *
 * // Create a table
 * await client.tables.create('products', {
 *   num_shards: 3,
 *   schema: {
 *     key: 'id',
 *     default_type: 'product'
 *   }
 * });
 * ```
 */

// Main client export
export { AntflyClient } from "./client.js";
// Re-export the generated types for advanced users
export type { components, operations, paths } from "./public-api.js";
export type { components as query_components } from "./query.js";
// Query helper functions
export {
  boolean,
  conjunction,
  dateRange,
  disjunction,
  docIds,
  fuzzy,
  geoBoundingBox,
  geoDistance,
  match,
  matchAll,
  matchNone,
  matchPhrase,
  numericRange,
  prefix,
  queryString,
  term,
} from "./query-helpers.js";
export { Client, type SDKConfig } from "./sdk.js";
export { InferenceClient } from "./inference-client.js";
export { deserializeEmbeddings, serializeEmbeddings } from "./inference-codec.js";
export type {
  Chunk,
  ChunkConfig,
  ChunkRequest,
  ChunkResponse,
  Config as InferenceRuntimeConfig,
  ContentPart,
  ContentSecurityConfig,
  Credentials,
  EmbedInput,
  EmbedRequest,
  EmbedResponse,
  ExtractClassification,
  ExtractEntity,
  ExtractRelation,
  ExtractRequest,
  ExtractResponse,
  ImageURL,
  ImageURLContentPart,
  Level,
  ModelsResponse,
  RequestOptions,
  RerankRequest,
  RerankResponse,
  RewriteRequest,
  RewriteResponse,
  Style,
  InferenceConfig,
  InferenceError,
  TextContentPart,
  TranscribeRequest,
  TranscribeResponse,
} from "./inference-types.js";
export { logLevels, logStyles } from "./inference-types.js";
// Type exports
export type {
  // Chat Agent types
  AgentDecision,
  AgentQuestion,
  AgentQuestionKind,
  AgentStatus,
  AgentStep,
  AgentStepKind,
  AgentStepStatus,
  AggregationBucket,
  AggregationDateRange,
  AggregationRange,
  AggregationRequest,
  AggregationResult,
  // Search and aggregation types
  AggregationType,
  // Authentication
  AntflyAuth,
  // Configuration
  AntflyConfig,
  // Error type
  AntflyError,
  AntflyType,
  // Backup/Restore types
  BackupRequest,
  BatchRequest, // Now using our custom type
  CalendarInterval,
  ChatAgentConfig,
  ChatAgentTurnResult,
  ChatMessage,
  ChatMessageRole,
  ChatStreamCallbacks,
  ChatToolName,
  ChatToolsConfig,
  // Chat types (used by retrieval agent)
  // Retrieval Agent result types
  ClassificationTransformationResult,
  CreateTableRequest,
  CreateUserRequest,
  DenseEmbedding,
  DistanceRange,
  DistanceUnit,
  // Schema types
  DocumentSchema,
  // Graph index types
  Edge,
  EdgeDirection,
  EdgesResponse,
  EdgeTopology,
  EdgeTypeConfig,
  // Model and reranker types
  EmbedderConfig,
  EmbedderProvider,
  // Embedding types
  Embedding,
  // Eval types
  EvalConfig,
  EvalResult,
  EvalScores,
  EvalSummary,
  EvaluatorName,
  EvaluatorScore,
  FetchConfig,
  FilterSpec,
  GenerationConfidence,
  GeneratorConfig,
  GeneratorProvider,
  GraphIndexConfig,
  GraphNodeSelector,
  GraphQuery,
  GraphQueryParams,
  GraphQueryResult,
  GraphQueryType,
  GraphResultNode,
  // Index types
  IndexConfig,
  IndexStatus,
  IndexType,
  // Join types
  JoinClause,
  JoinCondition,
  JoinFilters,
  JoinOperator,
  JoinProfile,
  JoinStrategy,
  JoinType,
  MergeProfile,
  Permission,
  PermissionType,
  // Query Builder Agent types
  QueryBuilderRequest,
  QueryBuilderResult,
  QueryHit,
  QueryOptions,
  QueryProfile,
  QueryRequest,
  // Core types
  QueryResponses,
  QueryResult,
  QueryStrategy,
  RerankerConfig,
  RerankerProfile,
  ResourceType,
  // Utility type for response data
  ResponseData,
  RestoreRequest,
  // Retrieval Agent types
  RetrievalAgentRequest,
  RetrievalAgentResult,
  RetrievalAgentSteps,
  RetrievalAgentStreamCallbacks,
  RouteType,
  SemanticQueryMode,
  ShardsProfile,
  SignificanceAlgorithm,
  SparseEmbedding,
  SSEStepStarted,
  // Table types
  Table,
  TableMigration,
  TableSchema,
  TableStatus,
  ToolCall,
  ToolCallFunction,
  TraversalResult,
  TraversalRules,
  UpdatePasswordRequest,
  // User and permission types
  User,
  // Web search types
  WebSearchConfig,
  WebSearchResultItem,
} from "./types.js";
export { embedderProviders, generatorProviders } from "./types.js";

// Default export for convenience
import { Client } from "./sdk.js";
export default Client;
