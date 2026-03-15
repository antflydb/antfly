/**
 * Type exports and utilities for the Termite SDK
 * Re-exports commonly used types from the generated OpenAPI types
 */

import type { components, operations } from "./termite-api.js";

// Request/Response types
export type EmbedRequest = components["schemas"]["EmbedRequest"];
export type EmbedResponse = components["schemas"]["EmbedResponse"];

export type ChunkRequest = components["schemas"]["ChunkRequest"];
export type ChunkConfig = components["schemas"]["ChunkConfig"];
export type ChunkResponse = components["schemas"]["ChunkResponse"];
export type Chunk = components["schemas"]["Chunk"];

export type RerankRequest = components["schemas"]["RerankRequest"];
export type RerankResponse = components["schemas"]["RerankResponse"];

export type RecognizeRequest = components["schemas"]["RecognizeRequest"];
export type RecognizeResponse = components["schemas"]["RecognizeResponse"];
export type RecognizeEntity = components["schemas"]["RecognizeEntity"];

export type ExtractRequest = components["schemas"]["ExtractRequest"];
export type ExtractResponse = components["schemas"]["ExtractResponse"];
export type ExtractFieldValue = components["schemas"]["ExtractFieldValue"];

export type RewriteRequest = components["schemas"]["RewriteRequest"];
export type RewriteResponse = components["schemas"]["RewriteResponse"];

export type ModelsResponse = components["schemas"]["ModelsResponse"];
export type VersionResponse = components["schemas"]["VersionResponse"];

export type TranscribeRequest = components["schemas"]["TranscribeRequest"];
export type TranscribeResponse = components["schemas"]["TranscribeResponse"];

// Content part types for multimodal embeddings
export type ContentPart = components["schemas"]["ContentPart"];
export type TextContentPart = components["schemas"]["TextContentPart"];
export type ImageURLContentPart = components["schemas"]["ImageURLContentPart"];
export type ImageURL = components["schemas"]["ImageURL"];

// Configuration types
export type Config = components["schemas"]["Config"];
export type ContentSecurityConfig = components["schemas"]["ContentSecurityConfig"];
export type Credentials = components["schemas"]["Credentials"];
export type Level = components["schemas"]["Level"];
export type Style = components["schemas"]["Style"];

// Error type
export type TermiteError = components["schemas"]["Error"];

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
