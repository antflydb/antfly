/**
 * Inference SDK Client
 * Provides a high-level interface for interacting with the Inference ML inference API
 */

import createClient, { type Client } from "openapi-fetch";
import { readLimitedResponseBytes, readLimitedResponseText } from "./client.js";
import { deserializeEmbeddings } from "./inference-codec.js";
import type {
  ChunkConfig,
  ChunkResponse,
  ClassificationResult,
  EmbedInput,
  EmbedResponse,
  EntityExtractionResult,
  ExtractRequest,
  ExtractResponse,
  GenerateChunk,
  GenerateRequest,
  GenerateResponse,
  InferenceConfig,
  InferenceError,
  ModelsResponse,
  RequestOptions,
  RerankResponse,
  RewriteResponse,
  TranscribeResponse,
  TransientCapacityError,
} from "./inference-types.js";
import type { paths } from "./public-api.js";

const MAX_GENERATION_SSE_LINE_BYTES = 16 << 20;
const MAX_GENERATION_SSE_EVENT_CHARS = 16 << 20;
const MAX_INFERENCE_JSON_RESPONSE_BYTES = 16 << 20;
const MAX_GENERATION_RESPONSE_BYTES = MAX_INFERENCE_JSON_RESPONSE_BYTES;
const MAX_INFERENCE_ERROR_BYTES = 1 << 20;
const DEFAULT_MAX_BINARY_RESPONSE_BYTES = 64 << 20;

export class InferenceAPIError extends Error {
  constructor(
    readonly status: number,
    readonly code: string | undefined,
    detail: string,
    readonly retryable: boolean | undefined
  ) {
    super(`inference request failed (${status}): ${detail}`);
    this.name = "InferenceAPIError";
  }
}

/** Structured temporary-capacity failure with an actionable backoff delay. */
export class InferenceCapacityError extends InferenceAPIError {
  readonly code: string;
  readonly reason: TransientCapacityError["reason"];
  readonly retryAfterMs: number;
  readonly retryable = true as const;

  constructor(details: TransientCapacityError) {
    super(503, details.error, `${details.message} (${details.error})`, true);
    this.name = "InferenceCapacityError";
    this.code = details.error;
    this.reason = details.reason;
    this.retryAfterMs = details.retry_after_ms;
  }
}

function isTransientCapacityError(value: unknown): value is TransientCapacityError {
  if (typeof value !== "object" || value === null) return false;
  const candidate = value as Record<string, unknown>;
  return (
    typeof candidate.error === "string" &&
    typeof candidate.message === "string" &&
    (candidate.reason === "inference_capacity" || candidate.reason === "request_queue") &&
    candidate.retryable === true &&
    typeof candidate.retry_after_ms === "number" &&
    Number.isFinite(candidate.retry_after_ms) &&
    candidate.retry_after_ms > 0
  );
}

export class InferenceClient {
  private client: Client<paths>;
  private baseUrl: string;
  private headers: Record<string, string>;
  private maxBinaryResponseBytes: number;

  constructor(config: InferenceConfig) {
    this.baseUrl = normalizeBaseUrl(config.baseUrl);
    this.headers = {
      "Content-Type": "application/json",
      ...config.headers,
    };
    this.maxBinaryResponseBytes =
      config.maxBinaryResponseBytes ?? DEFAULT_MAX_BINARY_RESPONSE_BYTES;
    if (!Number.isSafeInteger(this.maxBinaryResponseBytes) || this.maxBinaryResponseBytes <= 0) {
      throw new Error("maxBinaryResponseBytes must be a positive safe integer");
    }

    this.client = createClient<paths>({
      baseUrl: this.baseUrl,
      headers: {
        ...this.headers,
        Accept: "application/json",
      },
      fetch: (request) => fetchLimitedInferenceResponse(request, this.maxBinaryResponseBytes),
    });
  }

  /** Generate one non-streaming chat completion. */
  async generate(
    request: Omit<GenerateRequest, "stream">,
    options?: RequestOptions
  ): Promise<GenerateResponse> {
    const response = await fetch(`${this.baseUrl}/ai/v1/generate`, {
      method: "POST",
      headers: {
        ...this.headers,
        Accept: "application/json",
      },
      body: JSON.stringify({ ...request, stream: false }),
      signal: options?.signal,
    });
    if (!response.ok) throw await inferenceAPIErrorResponse(response);
    const { text, truncated } = await readLimitedResponseText(
      response,
      MAX_GENERATION_RESPONSE_BYTES
    );
    if (truncated) {
      throw new Error(`Generation response exceeded ${MAX_GENERATION_RESPONSE_BYTES} bytes`);
    }
    try {
      return JSON.parse(text) as GenerateResponse;
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      throw new Error(`Generation returned invalid JSON: ${detail}`);
    }
  }

  /** Stream chat-completion chunks until the server sends the [DONE] sentinel. */
  async *generateStream(
    request: Omit<GenerateRequest, "stream">,
    options?: RequestOptions
  ): AsyncGenerator<GenerateChunk, void, void> {
    const response = await fetch(`${this.baseUrl}/ai/v1/generate`, {
      method: "POST",
      headers: {
        ...this.headers,
        Accept: "text/event-stream",
      },
      body: JSON.stringify({ ...request, stream: true }),
      signal: options?.signal,
    });
    if (!response.ok) throw await inferenceAPIErrorResponse(response);

    const contentType = response.headers
      .get("content-type")
      ?.split(";", 1)[0]
      ?.trim()
      .toLowerCase();
    if (contentType !== "text/event-stream") {
      await response.body?.cancel();
      throw new Error(
        `Generation stream returned content type ${JSON.stringify(response.headers.get("content-type") ?? "")}`
      );
    }
    if (!response.body) throw new Error("Generation stream returned an empty body");

    for await (const frame of parseSSEFrames(response.body)) {
      if (frame.event === "error") throw new Error(`Generation stream failed: ${frame.data}`);
      if (frame.data.trim() === "[DONE]") return;
      try {
        yield JSON.parse(frame.data) as GenerateChunk;
      } catch (error) {
        const detail = error instanceof Error ? error.message : String(error);
        throw new Error(`Generation stream returned invalid JSON: ${detail}`);
      }
    }
    throw new Error("Generation stream ended before [DONE]");
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
    const { data, error, response } = await this.client.POST("/ai/v1/embed", {
      body: {
        model,
        input,
        truncate: options?.truncate,
      },
      parseAs: "json",
    });
    if (!response.ok) throw inferenceAPIError(response.status, error);
    if (!data) throw new Error("Embed failed: unexpected empty response");
    // The current API contract returns the OpenAI-compatible JSON shape.
    return data as EmbedResponse;
  }

  /**
   * Generate dense embeddings as a plain array of vectors.
   *
   * Current servers return the OpenAI-compatible JSON response. Bounded
   * application/octet-stream responses from legacy servers remain supported.
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
    const response = await fetch(`${this.baseUrl}/ai/v1/embed`, {
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

    if (!response.ok) throw await inferenceAPIErrorResponse(response);

    const contentType = responseMediaType(response);
    if (contentType !== "application/json" && contentType !== "application/octet-stream") {
      await response.body?.cancel();
      throw new Error(`Unexpected embedding response content type ${JSON.stringify(contentType)}`);
    }
    const maxResponseBytes =
      contentType === "application/json"
        ? Math.min(this.maxBinaryResponseBytes, MAX_INFERENCE_JSON_RESPONSE_BYTES)
        : this.maxBinaryResponseBytes;
    const { bytes, truncated } = await readLimitedResponseBytes(response, maxResponseBytes);
    if (truncated) {
      throw new Error(`Embedding response exceeded ${maxResponseBytes} bytes`);
    }

    switch (contentType) {
      case "application/json":
        return denseEmbeddingsFromJSON(bytes);
      case "application/octet-stream":
        return deserializeEmbeddings(bytes.buffer);
    }
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
    const { data, error, response } = await this.client.POST("/ai/v1/chunk", {
      body: {
        input: text,
        config,
      },
      signal: options?.signal,
    });
    if (!response.ok) throw inferenceAPIError(response.status, error);
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
    const { data, error, response } = await this.client.POST("/ai/v1/rerank", {
      body: {
        model,
        query,
        prompts,
      },
    });
    if (!response.ok) throw inferenceAPIError(response.status, error);
    if (!data) throw new Error("Rerank failed: unexpected empty response");
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
    return this.extractRaw({
      model,
      inputs: texts.map((content, index) => ({ id: String(index), content })),
      schema: { structures: structureSchema(schema) },
      options: {
        threshold: options?.threshold,
        flat_ner: options?.flatNer,
        include_confidence: options?.includeConfidence,
        include_spans: options?.includeSpans,
      },
    });
  }

  /**
   * Run the canonical schema-driven extraction API.
   */
  async extractRaw(request: ExtractRequest): Promise<ExtractResponse> {
    const { data, error, response } = await this.client.POST("/ai/v1/extract", {
      body: request,
    });
    if (!response.ok) throw inferenceAPIError(response.status, error);
    if (!data) throw new Error("Extract failed: unexpected empty response");
    return data;
  }

  /**
   * Classify text through the canonical /ai/v1/extract endpoint.
   */
  async classify(
    model: string,
    texts: string[],
    labels: string[],
    options?: {
      multiLabel?: boolean;
      threshold?: number;
      includeConfidence?: boolean;
    }
  ): Promise<ClassificationResult[]> {
    const response = await this.extractRaw({
      model,
      inputs: texts.map((content, index) => ({ id: String(index), content })),
      schema: {
        classifications: [
          {
            name: "classification",
            labels,
            multi_label: options?.multiLabel,
          },
        ],
      },
      options: {
        threshold: options?.threshold,
        include_confidence: options?.includeConfidence ?? true,
      },
    });

    return response.data.map((item, index) => ({
      index,
      classifications: (item.classifications ?? []).map((classification) => ({
        label: classification.label,
        score: classification.score,
      })),
    }));
  }

  /**
   * Extract named entities through the canonical /ai/v1/extract endpoint.
   */
  async recognize(
    model: string,
    texts: string[],
    labels: string[],
    options?: {
      threshold?: number;
      includeConfidence?: boolean;
      includeSpans?: boolean;
    }
  ): Promise<EntityExtractionResult[]> {
    const response = await this.extractRaw({
      model,
      inputs: texts.map((content, index) => ({ id: String(index), content })),
      schema: {
        entities: labels,
      },
      options: {
        threshold: options?.threshold,
        flat_ner: true,
        include_confidence: options?.includeConfidence ?? true,
        include_spans: options?.includeSpans ?? true,
      },
    });

    return response.data.map((item, index) => ({
      index,
      entities: (item.entities ?? []).map((entity) => ({
        text: entity.text,
        label: entity.label,
        score: entity.score,
        start: entity.start,
        end: entity.end,
      })),
    }));
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
    const { data, error, response } = await this.client.POST("/ai/v1/rewrite", {
      body: {
        model,
        inputs,
      },
    });
    if (!response.ok) throw inferenceAPIError(response.status, error);
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
    const { data, error, response } = await this.client.POST("/ai/v1/transcribe", {
      body: {
        audio,
        model: options?.model,
        language: options?.language,
      },
    });
    if (!response.ok) throw inferenceAPIError(response.status, error);
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
    const { data, error, response } = await this.client.GET("/ai/v1/models");
    if (!response.ok) throw inferenceAPIError(response.status, error);
    if (!data) throw new Error("List models failed: unexpected empty response");
    return data;
  }

  /**
   * Get the underlying OpenAPI client for advanced use cases
   */
  getRawClient() {
    return this.client;
  }
}

function inferenceAPIError(status: number, error: InferenceError | unknown): InferenceAPIError {
  if (status === 503 && isTransientCapacityError(error)) {
    return new InferenceCapacityError(error);
  }
  const payload =
    typeof error === "object" && error !== null ? (error as Partial<InferenceError>) : {};
  const code = typeof payload.error === "string" ? payload.error : undefined;
  const message = typeof payload.message === "string" ? payload.message : undefined;
  const retryable = typeof payload.retryable === "boolean" ? payload.retryable : undefined;
  const detail =
    message && message !== code
      ? `${message}${code ? ` (${code})` : ""}`
      : message || code || "Unknown inference error";
  return new InferenceAPIError(status, code, detail, retryable);
}

async function inferenceAPIErrorResponse(response: Response): Promise<InferenceAPIError> {
  const { text, truncated } = await readLimitedResponseText(response, MAX_INFERENCE_ERROR_BYTES);
  let error: unknown = {
    error: text.trim() || response.statusText || `HTTP ${response.status}`,
  };
  try {
    error = JSON.parse(text) as unknown;
  } catch {
    // Report the bounded text body when the peer did not return the documented JSON shape.
  }
  if (truncated) {
    error = {
      error: `${response.statusText || `HTTP ${response.status}`} (response body exceeded ${MAX_INFERENCE_ERROR_BYTES} bytes)`,
    };
  }
  return inferenceAPIError(response.status, error);
}

function responseMediaType(response: Response): string {
  const [mediaType = ""] = (response.headers.get("content-type") ?? "").split(";", 1);
  return mediaType.trim().toLowerCase();
}

function requestAcceptsEventStream(request: Request): boolean {
  return (request.headers.get("accept") ?? "")
    .split(",")
    .some((value) => value.split(";", 1)[0]?.trim().toLowerCase() === "text/event-stream");
}

async function fetchLimitedInferenceResponse(
  request: Request,
  maxBinaryResponseBytes: number
): Promise<Response> {
  const response = await globalThis.fetch(request);
  if (!response.body) return response;

  const mediaType = responseMediaType(response);
  if (response.ok && mediaType === "text/event-stream" && requestAcceptsEventStream(request)) {
    return response;
  }

  const limit = !response.ok
    ? MAX_INFERENCE_ERROR_BYTES
    : mediaType === "application/octet-stream" || mediaType === "application/x-sparse-vectors"
      ? maxBinaryResponseBytes
      : MAX_INFERENCE_JSON_RESPONSE_BYTES;
  const tooLarge = new Error(`Inference response exceeded ${limit} bytes`);
  const contentLength = response.headers.get("content-length");
  if (contentLength !== null && /^\d+$/.test(contentLength) && Number(contentLength) > limit) {
    try {
      await response.body.cancel(tooLarge);
    } catch {
      // Preserve the response-size error when cancellation itself fails.
    }
    throw tooLarge;
  }

  let total = 0;
  const body = response.body.pipeThrough(
    new TransformStream<Uint8Array, Uint8Array>({
      transform(chunk, controller) {
        if (chunk.byteLength > limit - total) throw tooLarge;
        total += chunk.byteLength;
        controller.enqueue(chunk);
      },
    })
  );
  const limited = new Response(body, {
    status: response.status,
    statusText: response.statusText,
    headers: response.headers,
  });
  Object.defineProperties(limited, {
    url: { value: response.url },
    redirected: { value: response.redirected },
    type: { value: response.type },
  });
  return limited;
}

function denseEmbeddingsFromJSON(bytes: Uint8Array): number[][] {
  let value: unknown;
  try {
    value = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes)) as unknown;
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error);
    throw new Error(`Embedding response returned invalid JSON: ${detail}`);
  }

  if (typeof value !== "object" || value === null) {
    throw new Error("Embedding JSON response must be an object");
  }
  const data = (value as { data?: unknown }).data;
  if (!Array.isArray(data)) {
    throw new Error("Embedding JSON response is missing the data array");
  }

  const embeddings: number[][] = [];
  let dimension: number | undefined;
  for (const [index, item] of data.entries()) {
    if (typeof item !== "object" || item === null) {
      throw new Error(`Embedding JSON response item ${index} must be an object`);
    }
    const embedding = (item as { embedding?: unknown }).embedding;
    if (!Array.isArray(embedding)) {
      throw new Error(`Embedding JSON response item ${index} is not a dense vector`);
    }
    if (
      embedding.some((component) => typeof component !== "number" || !Number.isFinite(component))
    ) {
      throw new Error(`Embedding JSON response item ${index} contains a non-finite number`);
    }
    if (embedding.length === 0) {
      throw new Error(`Embedding JSON response item ${index} has zero dimension`);
    }
    if (dimension === undefined) {
      dimension = embedding.length;
    } else if (embedding.length !== dimension) {
      throw new Error(
        `Embedding JSON response item ${index} has dimension ${embedding.length}, expected ${dimension}`
      );
    }
    embeddings.push(embedding as number[]);
  }
  return embeddings;
}

interface SSEFrame {
  event: string;
  data: string;
}

async function* parseSSEFrames(
  body: ReadableStream<Uint8Array>
): AsyncGenerator<SSEFrame, void, void> {
  const reader = body.getReader();
  const decoder = new TextDecoder("utf-8", { fatal: true });
  let buffer = "";
  let event = "";
  let data: string[] = [];
  let dataChars = 0;
  let lineBytes = 0;
  let complete = false;

  try {
    while (true) {
      const result = await reader.read();
      if (!result.done && result.value.byteLength > MAX_GENERATION_SSE_LINE_BYTES) {
        throw new Error(`Generation SSE chunk exceeded ${MAX_GENERATION_SSE_LINE_BYTES} bytes`);
      }
      if (!result.done) {
        for (const byte of result.value) {
          if (byte === 0x0a) {
            lineBytes = 0;
          } else {
            lineBytes += 1;
            if (lineBytes > MAX_GENERATION_SSE_LINE_BYTES) {
              throw new Error(
                `Generation SSE line exceeded ${MAX_GENERATION_SSE_LINE_BYTES} bytes`
              );
            }
          }
        }
      }
      try {
        buffer += result.done ? decoder.decode() : decoder.decode(result.value, { stream: true });
      } catch {
        throw new Error("Generation stream contained invalid UTF-8");
      }
      if (result.done) buffer += "\n\n";

      let newline = buffer.indexOf("\n");
      while (newline !== -1) {
        let line = buffer.slice(0, newline);
        buffer = buffer.slice(newline + 1);
        if (line.endsWith("\r")) line = line.slice(0, -1);

        if (line === "") {
          if (data.length > 0) yield { event, data: data.join("\n") };
          event = "";
          data = [];
          dataChars = 0;
        } else if (!line.startsWith(":")) {
          const colon = line.indexOf(":");
          const field = colon === -1 ? line : line.slice(0, colon);
          let value = colon === -1 ? "" : line.slice(colon + 1);
          if (value.startsWith(" ")) value = value.slice(1);
          if (field === "event") event = value;
          if (field === "data") {
            dataChars += (data.length > 0 ? 1 : 0) + value.length;
            if (dataChars > MAX_GENERATION_SSE_EVENT_CHARS) {
              throw new Error(
                `Generation SSE event exceeded ${MAX_GENERATION_SSE_EVENT_CHARS} characters`
              );
            }
            data.push(value);
          }
        }
        newline = buffer.indexOf("\n");
      }

      if (buffer.length > MAX_GENERATION_SSE_EVENT_CHARS) {
        throw new Error(
          `Generation SSE line exceeded ${MAX_GENERATION_SSE_EVENT_CHARS} characters`
        );
      }
      if (result.done) {
        complete = true;
        return;
      }
    }
  } finally {
    if (!complete) {
      try {
        await reader.cancel();
      } catch {
        // Preserve the parse, consumer, or abort error that ended iteration.
      }
    }
    reader.releaseLock();
  }
}

function structureSchema(schema: Record<string, string[]>) {
  return Object.fromEntries(
    Object.entries(schema).map(([name, fields]) => [
      name,
      {
        fields: Object.fromEntries(fields.map((field) => parseStructureField(field))),
      },
    ])
  );
}

function parseStructureField(field: string): [string, string] {
  const parts = field.split("::");
  const name = parts[0]?.trim();
  if (!name) throw new Error(`Invalid extraction field: ${field}`);
  const kind = parts[1]?.trim();
  return [name, kind === "list" ? "list" : "str"];
}

function normalizeBaseUrl(baseUrl: string): string {
  const trimmed = baseUrl.trim().replace(/\/+$/, "");

  if (trimmed === "" || trimmed === "/") {
    return "";
  }

  if (trimmed.endsWith("/ai/v1")) {
    return trimmed.slice(0, -"/ai/v1".length);
  }

  if (trimmed.endsWith("/api")) {
    return trimmed.slice(0, -4);
  }

  return trimmed;
}
