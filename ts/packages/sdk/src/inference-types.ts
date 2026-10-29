/**
 * Type exports and utilities for the Inference SDK
 * Re-exports commonly used types from the generated OpenAPI types
 */

import type { components, operations } from "./public-api.js";

// Request/Response types
export type EmbedRequest = components["schemas"]["InferenceEmbedRequest"];
export type EmbedOptions = Pick<
  EmbedRequest,
  "task_type" | "dimensions" | "instruction" | "error_policy"
> & { truncate?: boolean; signal?: AbortSignal };
export type EmbedResponse = components["schemas"]["InferenceEmbedResponse"];

export type ChunkRequest = components["schemas"]["InferenceChunkRequest"];
export type ChunkConfig = components["schemas"]["InferenceChunkConfig"];
export type ChunkResponse = components["schemas"]["InferenceChunkResponse"];
export type ChunkEmbeddingResponse = components["schemas"]["ChunkEmbeddingResponse"];
export type ChunkEmbeddingObject = components["schemas"]["ChunkEmbeddingObject"];
export type ChunkEmbeddingRequest = ChunkRequest & {
  embedding: NonNullable<ChunkRequest["embedding"]>;
};
export type ChunkSourceReference = components["schemas"]["ChunkSourceReference"];
export type MediaChunkPolicy = components["schemas"]["MediaChunkPolicy"];
export type ChunkEmbeddingTarget = components["schemas"]["ChunkEmbeddingTarget"];
export type ChunkEmbeddingContext = components["schemas"]["ChunkEmbeddingContext"];
export type ChunkEmbeddingStreamEvent =
  | { object: "chunk.embedding.start"; context: ChunkEmbeddingContext }
  | { object: "chunk.embedding"; data: ChunkEmbeddingObject }
  | { object: "chunk.embedding.complete"; response: ChunkEmbeddingResponse };
export type Chunk = components["schemas"]["InferenceChunk"];

export type RerankRequest = components["schemas"]["InferenceRerankRequest"];
export type RerankResponse = components["schemas"]["InferenceRerankResponse"];

export type ExtractRequest = components["schemas"]["ExtractionRequest"];
/** A strict mixed-task request; extractV2 supplies schema_version:2. */
export type ExtractV2Request = Omit<ExtractRequest, "schema_version">;
export type ExtractResponse = components["schemas"]["ExtractionResponse"];
export type GeneratedDecideRequest = components["schemas"]["GeneratedDecideRequest"];
export type GeneratedExtractRequest = components["schemas"]["GeneratedExtractionRequest"];
export type DecideRequest =
  | components["schemas"]["InferenceDecideRequest"]
  | GeneratedDecideRequest;
export type DecideResponse = components["schemas"]["InferenceDecideResponse"];
export type ExtractEntity = components["schemas"]["ExtractionEntity"];
export type ExtractRelation = components["schemas"]["ExtractionRelation"];
export type ExtractClassification = components["schemas"]["ExtractionClassification"];

export interface ClassificationResult {
  index: number;
  classifications: Array<{
    label: string;
    score?: number;
  }>;
}

export interface EntityExtractionResult {
  index: number;
  entities: Array<{
    text: string;
    label: string;
    score?: number;
    start?: number;
    end?: number;
  }>;
}

export type RewriteRequest = components["schemas"]["InferenceRewriteRequest"];
export type RewriteResponse = components["schemas"]["InferenceRewriteResponse"];

export type ModelsResponse = components["schemas"]["InferenceModelsResponse"];

export type TranscribeRequest = components["schemas"]["InferenceTranscribeRequest"];
export type TranscribeResponse = components["schemas"]["InferenceTranscribeResponse"];

export type InferenceChatMessage = components["schemas"]["InferenceChatMessage"];
export type GenerateRequest = components["schemas"]["InferenceGenerateRequest"];
export type GenerateResponse = components["schemas"]["InferenceGenerateResponse"];
export type GenerateChunk = components["schemas"]["InferenceGenerateChunk"];
export type ContextCheckpoint = components["schemas"]["InferenceContextCheckpoint"];
export type ContextDiagnostics = components["schemas"]["InferenceContextDiagnostics"];
export type ContextToolResultOutcome = components["schemas"]["InferenceContextToolResultOutcome"];
export type ContextPrepareRequest = components["schemas"]["InferenceContextPrepareRequest"];
export type ContextPrepareResponse = components["schemas"]["InferenceContextPrepareResponse"];

// Content part types for multimodal embeddings
export type ContentPart = components["schemas"]["InferenceContentPart"];
export type TextContentPart = components["schemas"]["InferenceTextContentPart"];
export type ImageURLContentPart = components["schemas"]["InferenceImageURLContentPart"];
export type ImageURL = components["schemas"]["InferenceImageURL"];

// Configuration types
export type Config = components["schemas"]["InferenceConfig"];
export type ContentSecurityConfig = components["schemas"]["ContentSecurityConfig"];
export type Credentials = components["schemas"]["Credentials"];
export type Level = components["schemas"]["Level"];
export type Style = components["schemas"]["Style"];

// Error type
export type InferenceError = components["schemas"]["InferenceError"];
export type TransientCapacityError = components["schemas"]["InferenceTransientCapacityError"];

// Utility type for extracting response data
export type ResponseData<T extends keyof operations> = operations[T]["responses"] extends {
  200: infer R;
}
  ? R extends { content: { "application/json": infer D } }
    ? D
    : never
  : never;

// Client configuration
export interface InferenceConfig {
  /** Base URL of the Inference API server (e.g., "http://localhost:8080/api") */
  baseUrl: string;
  /** Additional headers to include in requests */
  headers?: Record<string, string>;
  /** Maximum binary inference response size. Defaults to 64 MiB. */
  maxBinaryResponseBytes?: number;
}

// Helper type for embedding input - supports all three formats
export type EmbedInput = string | string[] | ContentPart[];

// A document to rerank: text, or text and image content parts
export type RerankDocument = string | ContentPart[];

// Log level values for convenience
export const logLevels: Level[] = ["debug", "info", "warn", "error"];

// Log style values for convenience
export const logStyles: Style[] = ["terminal", "json", "logfmt", "noop"];

// Request options for SDK methods
export interface RequestOptions {
  /** AbortSignal to cancel the request */
  signal?: AbortSignal;
}
