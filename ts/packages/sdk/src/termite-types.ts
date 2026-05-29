/**
 * Type exports and utilities for the Termite SDK
 * Re-exports commonly used types from the generated OpenAPI types
 */

import type { components, operations } from "./public-api.js";

// Request/Response types
export type EmbedRequest = components["schemas"]["TermiteEmbedRequest"];
export type EmbedResponse = components["schemas"]["TermiteEmbedResponse"];

export type ChunkRequest = components["schemas"]["TermiteChunkRequest"];
export type ChunkConfig = components["schemas"]["TermiteChunkConfig"];
export type ChunkResponse = components["schemas"]["TermiteChunkResponse"];
export type Chunk = components["schemas"]["TermiteChunk"];

export type RerankRequest = components["schemas"]["TermiteRerankRequest"];
export type RerankResponse = components["schemas"]["TermiteRerankResponse"];

export type RecognizeRequest = components["schemas"]["ExtractionRequest"];
export type RecognizeResponse = components["schemas"]["ExtractionResponse"];
export type RecognizeEntity = components["schemas"]["ExtractionEntity"];

export type ExtractRequest = components["schemas"]["ExtractionRequest"];
export type ExtractResponse = components["schemas"]["ExtractionResponse"];
export type ExtractFieldValue = unknown;

export type RewriteRequest = components["schemas"]["TermiteRewriteRequest"];
export type RewriteResponse = components["schemas"]["TermiteRewriteResponse"];

export type ModelsResponse = components["schemas"]["TermiteModelsResponse"];
export type VersionResponse = components["schemas"]["TermiteVersionResponse"];

export type TranscribeRequest = components["schemas"]["TermiteTranscribeRequest"];
export type TranscribeResponse = components["schemas"]["TermiteTranscribeResponse"];

// Content part types for multimodal embeddings
export type ContentPart = components["schemas"]["TermiteContentPart"];
export type TextContentPart = components["schemas"]["TermiteTextContentPart"];
export type ImageURLContentPart = components["schemas"]["TermiteImageURLContentPart"];
export type ImageURL = components["schemas"]["TermiteImageURL"];

// Configuration types
export type Config = components["schemas"]["TermiteConfig"];
export type ContentSecurityConfig = components["schemas"]["TermiteContentSecurityConfig"];
export type Credentials = components["schemas"]["TermiteCredentials"];
export type Level = components["schemas"]["TermiteLevel"];
export type Style = components["schemas"]["TermiteStyle"];

// Error type
export type TermiteError = components["schemas"]["TermiteError"];

// Utility type for extracting response data
export type ResponseData<T extends keyof operations> = operations[T]["responses"] extends {
  200: infer R;
}
  ? R extends { content: { "application/json": infer D } }
    ? D
    : never
  : never;

// Client configuration
export interface TermiteConfig {
  /** Base URL of the Termite API server (e.g., "http://localhost:8080/api") */
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
