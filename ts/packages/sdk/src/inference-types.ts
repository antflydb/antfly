/**
 * Type exports and utilities for the Inference SDK
 * Re-exports commonly used types from the generated OpenAPI types
 */

import type { components, operations } from "./public-api.js";

// Request/Response types
export type EmbedRequest = components["schemas"]["InferenceEmbedRequest"];
export type EmbedResponse = components["schemas"]["InferenceEmbedResponse"];

export type ChunkRequest = components["schemas"]["InferenceChunkRequest"];
export type ChunkConfig = components["schemas"]["InferenceChunkConfig"];
export type ChunkResponse = components["schemas"]["InferenceChunkResponse"];
export type Chunk = components["schemas"]["InferenceChunk"];

export type RerankRequest = components["schemas"]["InferenceRerankRequest"];
export type RerankResponse = components["schemas"]["InferenceRerankResponse"];

export type RecognizeRequest = components["schemas"]["InferenceRecognizeRequest"];
export type RecognizeResponse = components["schemas"]["InferenceRecognizeResponse"];
export type RecognizeEntity = components["schemas"]["InferenceRecognizeEntity"];

export type ExtractRequest = components["schemas"]["InferenceExtractRequest"];
export type ExtractResponse = components["schemas"]["InferenceExtractResponse"];
export type ExtractFieldValue = components["schemas"]["InferenceExtractFieldValue"];

export type RewriteRequest = components["schemas"]["InferenceRewriteRequest"];
export type RewriteResponse = components["schemas"]["InferenceRewriteResponse"];

export type ModelsResponse = components["schemas"]["InferenceModelsResponse"];

export type TranscribeRequest = components["schemas"]["InferenceTranscribeRequest"];
export type TranscribeResponse = components["schemas"]["InferenceTranscribeResponse"];

// Content part types for multimodal embeddings
export type ContentPart = components["schemas"]["InferenceContentPart"];
export type TextContentPart = components["schemas"]["InferenceTextContentPart"];
export type ImageURLContentPart = components["schemas"]["InferenceImageURLContentPart"];
export type ImageURL = components["schemas"]["InferenceImageURL"];

// Configuration types
export type Config = components["schemas"]["InferenceConfig"];
export type ContentSecurityConfig = components["schemas"]["InferenceContentSecurityConfig"];
export type Credentials = components["schemas"]["InferenceCredentials"];
export type Level = components["schemas"]["InferenceLevel"];
export type Style = components["schemas"]["InferenceStyle"];

// Error type
export type InferenceError = components["schemas"]["InferenceError"];

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
}

// Helper type for embedding input - supports all three formats
export type EmbedInput = string | string[] | ContentPart[];

// Log level values for convenience
export const logLevels: Level[] = ["debug", "info", "warn", "error"];

// Log style values for convenience
export const logStyles: Style[] = ["terminal", "json", "logfmt", "noop"];

// Request options for SDK methods
export interface RequestOptions {
  /** AbortSignal to cancel the request */
  signal?: AbortSignal;
}
