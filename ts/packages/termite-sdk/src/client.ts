/**
 * Termite SDK Client
 * Provides a high-level interface for interacting with the Termite ML inference API
 */

import createClient, { type Client } from "openapi-fetch";
import { deserializeEmbeddings } from "./codec.js";
import type { paths } from "./termite-api.js";
import type {
  ChunkConfig,
  ChunkResponse,
  EmbedInput,
  EmbedResponse,
  ExtractResponse,
  ModelsResponse,
  RecognizeResponse,
  RequestOptions,
  RerankResponse,
  RewriteResponse,
  TermiteConfig,
  TranscribeResponse,
  VersionResponse,
} from "./types.js";

export class TermiteClient {
  private client: Client<paths>;
  private baseUrl: string;
  private headers: Record<string, string>;

  constructor(config: TermiteConfig) {
    this.baseUrl = normalizeBaseUrl(config.baseUrl);
    this.headers = {
      "Content-Type": "application/json",
      ...config.headers,
    };

    this.client = createClient<paths>({
      baseUrl: this.baseUrl,
      headers: {
        ...this.headers,
        Accept: "application/json",
      },
    });
  }

  /**
   * Generate embeddings for text or multimodal content
   *
   * @param model - Name of the embedder model (e.g., "bge-small-en-v1.5")
   * @param input - Text string, array of strings, or array of content parts (for multimodal)
   * @param options - Optional parameters
   * @returns EmbedResponse with model name and embedding vectors
   *
   * @example Text embedding (single string)
   * ```typescript
   * const result = await client.embed("bge-small-en-v1.5", "hello world");
   * console.log(result.embeddings[0]); // [0.0123, -0.0456, ...]
   * ```
   *
   * @example Text embedding (multiple strings)
   * ```typescript
   * const result = await client.embed("bge-small-en-v1.5", ["hello", "world"]);
   * console.log(result.embeddings.length); // 2
   * ```
   *
   * @example Multimodal embedding (CLIP)
   * ```typescript
   * const result = await client.embed("clip-vit-base-patch32", [
   *   { type: "text", text: "a photo of a cat" },
   *   { type: "image_url", image_url: { url: "data:image/png;base64,..." } }
   * ]);
   * ```
   */
  async embed(
    model: string,
    input: EmbedInput,
    options?: { truncate?: boolean }
  ): Promise<EmbedResponse> {
    const { data, error } = await this.client.POST("/embed", {
      body: {
        model,
        input,
        truncate: options?.truncate,
      },
      parseAs: "json",
    });
    if (error) throw new Error(`Embed failed: ${error.error}`);
    // The API returns both application/octet-stream and application/json.
    // We request JSON via Accept header and parseAs, so cast appropriately.
    return data as EmbedResponse;
  }

  /**
   * Generate embeddings in binary format (more efficient for large batches)
   *
   * Binary format is the default response format from Termite and is more efficient
   * for transferring large embedding vectors. Use this when you need raw embeddings
   * without the model name in the response.
   *
   * @param model - Name of the embedder model (e.g., "bge-small-en-v1.5")
   * @param input - Text string, array of strings, or array of content parts (for multimodal)
   * @param options - Optional parameters
   * @returns 2D array of embedding vectors
   *
   * @example
   * ```typescript
   * const embeddings = await client.embedBinary("bge-small-en-v1.5", ["hello", "world"]);
   * console.log(embeddings[0]); // [0.0123, -0.0456, ...]
   * ```
   */
  async embedBinary(
    model: string,
    input: EmbedInput,
    options?: { truncate?: boolean }
  ): Promise<number[][]> {
    const response = await fetch(`${this.baseUrl}/embed`, {
      method: "POST",
      headers: {
        ...this.headers,
        Accept: "application/octet-stream",
      },
      body: JSON.stringify({
        model,
        input,
        truncate: options?.truncate,
      }),
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Embed failed: ${response.status} ${errorText}`);
    }

    const arrayBuffer = await response.arrayBuffer();
    return deserializeEmbeddings(arrayBuffer);
  }

  /**
   * Chunk text into smaller segments
   *
   * @param text - Text to chunk
   * @param config - Optional chunking configuration
   * @param options - Optional request options (e.g., abort signal)
   * @returns ChunkResponse with array of chunks and metadata
   *
   * @example Fixed chunking
   * ```typescript
   * const result = await client.chunk("This is a long document...", {
   *   model: "fixed",
   *   text: { target_tokens: 500, overlap_tokens: 50 }
   * });
   * for (const chunk of result.chunks) {
   *   console.log(chunk.text, chunk.start_char, chunk.end_char);
   * }
   * ```
   *
   * @example Semantic chunking
   * ```typescript
   * const result = await client.chunk("This is a long document...", {
   *   model: "chonky-mmbert-small-multilingual-1",
   *   threshold: 0.5
   * });
   * ```
   *
   * @example With abort signal
   * ```typescript
   * const controller = new AbortController();
   * const result = await client.chunk("Long text...", { model: "fixed" }, {
   *   signal: controller.signal
   * });
   * // Call controller.abort() to cancel the request
   * ```
   */
  async chunk(
    text: string,
    config?: ChunkConfig,
    options?: RequestOptions
  ): Promise<ChunkResponse> {
    const { data, error } = await this.client.POST("/chunk", {
      body: {
        text,
        config,
      },
      signal: options?.signal,
    });
    if (error) throw new Error(`Chunk failed: ${error.error}`);
    if (!data) throw new Error("Chunk failed: unexpected empty response");
    return data;
  }

  /**
   * Rerank prompts by relevance to a query
   *
   * @param model - Name of the reranker model (e.g., "bge-reranker-v2-m3")
   * @param query - Search query for relevance scoring
   * @param prompts - Pre-rendered text prompts to rerank
   * @returns RerankResponse with relevance scores for each prompt
   *
   * @example
   * ```typescript
   * const result = await client.rerank(
   *   "bge-reranker-v2-m3",
   *   "machine learning applications",
   *   [
   *     "Introduction to Machine Learning: This guide covers...",
   *     "Deep Learning Fundamentals: Neural networks are...",
   *     "Cooking recipes for beginners"
   *   ]
   * );
   * // result.scores might be [0.85, 0.92, 0.12]
   * // Higher scores indicate more relevance to the query
   * ```
   */
  async rerank(model: string, query: string, prompts: string[]): Promise<RerankResponse> {
    const { data, error } = await this.client.POST("/rerank", {
      body: {
        model,
        query,
        prompts,
      },
    });
    if (error) throw new Error(`Rerank failed: ${error.error}`);
    if (!data) throw new Error("Rerank failed: unexpected empty response");
    return data;
  }

  /**
   * Recognize named entities in text
   *
   * @param model - Name of the recognizer model (e.g., "gliner-multi-v2.1")
   * @param texts - Array of texts to extract entities from
   * @param options - Optional parameters
   * @param options.labels - Custom entity labels (GLiNER models only)
   * @param options.relationLabels - Relation types to extract (for models with relations capability)
   * @returns RecognizeResponse with entities per input text
   *
   * @example
   * ```typescript
   * const result = await client.recognize(
   *   "gliner-multi-v2.1",
   *   ["John Smith works at Google in London."],
   *   { labels: ["person", "organization", "location"] }
   * );
   * // result.entities[0] contains entities for the first text
   * ```
   */
  async recognize(
    model: string,
    texts: string[],
    options?: { labels?: string[]; relationLabels?: string[] }
  ): Promise<RecognizeResponse> {
    const { data, error } = await this.client.POST("/recognize", {
      body: {
        model,
        texts,
        labels: options?.labels,
        relation_labels: options?.relationLabels,
      },
    });
    if (error) throw new Error(`Recognize failed: ${error.error}`);
    if (!data) throw new Error("Recognize failed: unexpected empty response");
    return data;
  }

  /**
   * Extract structured data from text using GLiNER2 models
   *
   * @param model - Name of the extractor model (e.g., "fastino/gliner2-base-v1")
   * @param texts - Array of texts to extract from
   * @param schema - Extraction schema mapping structure names to field definitions
   * @param options - Optional parameters
   * @returns ExtractResponse with structured results per input text
   *
   * @example
   * ```typescript
   * const result = await client.extract(
   *   "fastino/gliner2-base-v1",
   *   ["John Smith is 30 years old and works at Google."],
   *   { person: ["name::str", "age::str", "company::str"] }
   * );
   * ```
   */
  async extract(
    model: string,
    texts: string[],
    schema: Record<string, string[]>,
    options?: {
      threshold?: number;
      flatNer?: boolean;
      includeConfidence?: boolean;
      includeSpans?: boolean;
    }
  ): Promise<ExtractResponse> {
    const { data, error } = await this.client.POST("/extract", {
      body: {
        model,
        texts,
        schema,
        threshold: options?.threshold,
        flat_ner: options?.flatNer,
        include_confidence: options?.includeConfidence,
        include_spans: options?.includeSpans,
      },
    });
    if (error) throw new Error(`Extract failed: ${error.error}`);
    if (!data) throw new Error("Extract failed: unexpected empty response");
    return data;
  }

  /**
   * Rewrite/transform text using Seq2Seq models
   *
   * @param model - Name of the rewriter model (e.g., "lmqg/flan-t5-small-squad-qg")
   * @param inputs - Array of input texts to rewrite
   * @returns RewriteResponse with transformed texts
   *
   * @example
   * ```typescript
   * const result = await client.rewrite(
   *   "lmqg/flan-t5-small-squad-qg",
   *   ["generate question: The Eiffel Tower is in <hl> Paris <hl>."]
   * );
   * ```
   */
  async rewrite(model: string, inputs: string[]): Promise<RewriteResponse> {
    const { data, error } = await this.client.POST("/rewrite", {
      body: {
        model,
        inputs,
      },
    });
    if (error) throw new Error(`Rewrite failed: ${error.error}`);
    if (!data) throw new Error("Rewrite failed: unexpected empty response");
    return data;
  }

  /**
   * Transcribe audio to text (speech-to-text)
   *
   * @param audio - Base64-encoded audio data (WAV, MP3, FLAC, etc.)
   * @param options - Optional parameters
   * @param options.model - Name of transcriber model (e.g., "openai/whisper-tiny")
   * @param options.language - Force specific language for transcription
   * @returns TranscribeResponse with transcribed text and metadata
   *
   * @example Basic transcription
   * ```typescript
   * const audioBase64 = fs.readFileSync('audio.wav').toString('base64');
   * const result = await client.transcribe(audioBase64);
   * console.log(result.text); // "Hello, how are you today?"
   * ```
   *
   * @example With specific model and language
   * ```typescript
   * const result = await client.transcribe(audioBase64, {
   *   model: "openai/whisper-tiny",
   *   language: "en"
   * });
   * ```
   */
  async transcribe(
    audio: string,
    options?: { model?: string; language?: string }
  ): Promise<TranscribeResponse> {
    const { data, error } = await this.client.POST("/transcribe", {
      body: {
        audio,
        model: options?.model,
        language: options?.language,
      },
    });
    if (error) throw new Error(`Transcribe failed: ${error.error}`);
    if (!data) throw new Error("Transcribe failed: unexpected empty response");
    return data;
  }

  /**
   * List available models
   *
   * @returns ModelsResponse with lists of available embedders, chunkers, and rerankers
   *
   * @example
   * ```typescript
   * const models = await client.listModels();
   * console.log("Embedders:", models.embedders);
   * console.log("Chunkers:", models.chunkers);
   * console.log("Rerankers:", models.rerankers);
   * ```
   */
  async listModels(): Promise<ModelsResponse> {
    const { data, error } = await this.client.GET("/models");
    if (error) throw new Error(`List models failed: ${error.error}`);
    if (!data) throw new Error("List models failed: unexpected empty response");
    return data;
  }

  /**
   * Get version information
   *
   * @returns VersionResponse with version, git commit, build time, and Go version
   *
   * @example
   * ```typescript
   * const version = await client.getVersion();
   * console.log(`Termite ${version.version} (${version.git_commit})`);
   * ```
   */
  async getVersion(): Promise<VersionResponse> {
    const { data, error } = await this.client.GET("/version");
    if (error) throw new Error(`Get version failed: ${error.error}`);
    if (!data) throw new Error("Get version failed: unexpected empty response");
    return data;
  }

  /**
   * Get the underlying OpenAPI client for advanced use cases
   */
  getRawClient() {
    return this.client;
  }
}

function normalizeBaseUrl(baseUrl: string): string {
  const trimmed = baseUrl.trim().replace(/\/$/, "");

  if (trimmed === "" || trimmed === "/") {
    return "/ml/v1";
  }

  if (trimmed.endsWith("/ml/v1")) {
    return trimmed;
  }

  if (trimmed.endsWith("/api")) {
    return `${trimmed.slice(0, -4)}/ml/v1`;
  }

  return `${trimmed}/ml/v1`;
}
