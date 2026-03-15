/**
 * Termite SDK for TypeScript
 *
 * A TypeScript SDK for interacting with the Termite ML inference API.
 * Termite provides local ML inference for embeddings, chunking, reranking, and transcription.
 *
 * @example
 * ```typescript
 * import { TermiteClient } from '@antfly/termite-sdk';
 *
 * const client = new TermiteClient({
 *   baseUrl: 'http://localhost:8080/api'
 * });
 *
 * // Generate embeddings
 * const embedResult = await client.embed('bge-small-en-v1.5', ['hello world']);
 * console.log(embedResult.embeddings[0]);
 *
 * // Chunk text
 * const chunkResult = await client.chunk('Long document text...', {
 *   model: 'fixed',
 *   text: { target_tokens: 500 }
 * });
 *
 * // Rerank by relevance
 * const rerankResult = await client.rerank(
 *   'bge-reranker-v2-m3',
 *   'search query',
 *   ['doc1 text', 'doc2 text']
 * );
 *
 * // Transcribe audio to text
 * const transcribeResult = await client.transcribe(audioBase64, {
 *   model: 'openai/whisper-tiny'
 * });
 * console.log(transcribeResult.text);
 *
 * // List available models
 * const models = await client.listModels();
 * ```
 */

// Main client export
export { TermiteClient } from "./client.js";
// Binary codec utilities for advanced usage
export { deserializeEmbeddings, serializeEmbeddings } from "./codec.js";
// Re-export the generated types for advanced users
export type { components, operations, paths } from "./termite-api.js";
// Type exports
export type {
  // Request/Response types
  Chunk,
  ChunkConfig,
  ChunkRequest,
  ChunkResponse,
  // Configuration types
  Config,
  ContentPart,
  ContentSecurityConfig,
  Credentials,
  EmbedInput,
  EmbedRequest,
  EmbedResponse,
  // Extraction types
  ExtractFieldValue,
  ExtractRequest,
  ExtractResponse,
  ImageURL,
  ImageURLContentPart,
  Level,
  ModelsResponse,
  // Recognize types
  RecognizeEntity,
  RecognizeRequest,
  RecognizeResponse,
  // Request options
  RequestOptions,
  RerankRequest,
  RerankResponse,
  // Utility types
  ResponseData,
  // Rewrite types
  RewriteRequest,
  RewriteResponse,
  Style,
  // Client configuration
  TermiteConfig,
  // Error type
  TermiteError,
  TextContentPart,
  // Transcription types
  TranscribeRequest,
  TranscribeResponse,
  VersionResponse,
} from "./types.js";
// Constant exports
export { logLevels, logStyles } from "./types.js";

// Default export for convenience
import { TermiteClient } from "./client.js";
export default TermiteClient;
