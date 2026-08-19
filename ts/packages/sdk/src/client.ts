/**
 * Antfly SDK Client
 * Provides a high-level interface for interacting with the Antfly API
 */

import createClient, { type Client } from "openapi-fetch";
import type { paths } from "./public-api.js";
import type {
  AntflyAuth,
  AntflyConfig,
  BackupRequest,
  BatchRequest,
  BatchResult,
  ChatAgentConfig,
  ChatAgentTurnResult,
  ChatMessage,
  ChatStreamCallbacks,
  ClusterRestoreRequest,
  ConnectionsResponse,
  CreateIndexRequest,
  CreatedIndex,
  CreateTableRequest,
  CreateUserRequest,
  DocumentArtifactManifest,
  DocumentArtifactManifestList,
  DocumentArtifactReprocessJob,
  DocumentArtifactReprocessJobStartRequest,
  DocumentArtifactReprocessResponse,
  DocumentArtifactTableReprocessRequest,
  DocumentArtifactTableReprocessResponse,
  EnrichmentConfig,
  IndexStatus,
  LinearMergeRequest,
  LinearMergeResult,
  MultiBatchRequest,
  MultiBatchResult,
  Permission,
  QueryBuilderRequest,
  QueryBuilderResult,
  QueryRequest,
  QueryResponses,
  QueryResult,
  ResourceType,
  RestoreJob,
  RestoreRequest,
  RetrievalAgentRequest,
  RetrievalAgentResult,
  RetrievalAgentStreamCallbacks,
  ScanKeysRequest,
  TableArtifactEnrichmentList,
  TableSchema,
  User,
  WriteOptions,
} from "./types.js";

export interface RestoreOptions {
  /** Stable key used to safely retry creation of the same restore job. */
  idempotencyKey?: string;
}

export interface RestoreJobListOptions {
  limit?: number;
  cursor?: string;
  phase?: RestoreJob["phase"];
  scope?: RestoreJob["scope"];
}

export interface RestoreJobPage {
  jobs: RestoreJob[];
  next_cursor?: string;
}

export interface IndexOperations {
  list(tableName: string): Promise<IndexStatus[]>;
  get(tableName: string, indexName: string): Promise<IndexStatus>;
  create(
    tableName: string,
    indexName: string,
    config: CreateIndexRequest
  ): Promise<CreatedIndex>;
  drop(tableName: string, indexName: string): Promise<true>;
}

export const QUERY_TEMPORARILY_UNAVAILABLE_CODES = [
  "doc_identity_unavailable",
  "read_requires_primary",
  "standby_read_unavailable",
  "storage_read_temporarily_unavailable",
  "index_rebuilding",
  "query_embedding_temporarily_unavailable",
] as const;

export type QueryTemporarilyUnavailableCode = (typeof QUERY_TEMPORARILY_UNAVAILABLE_CODES)[number];

/** A retryable query dependency or read-availability failure. */
export class QueryTemporarilyUnavailableError extends Error {
  readonly status = 503 as const;
  readonly retryable = true as const;

  constructor(
    message: string,
    readonly code: QueryTemporarilyUnavailableCode,
    readonly retryAfterSeconds: number | undefined
  ) {
    super(message);
    this.name = "QueryTemporarilyUnavailableError";
  }
}

/** A retryable storage-admission rejection with an actionable delay. */
export class StorageResourceExhaustedError extends Error {
  readonly status = 429 as const;
  readonly code = "storage_resource_exhausted" as const;
  readonly retryable = true as const;

  constructor(
    message: string,
    readonly retryAfterMs: number,
    readonly retryAfterSeconds: number | undefined
  ) {
    super(message);
    this.name = "StorageResourceExhaustedError";
  }
}

/** @deprecated Catch QueryTemporarilyUnavailableError to handle every retryable query 503. */
export class StorageReadTemporarilyUnavailableError extends QueryTemporarilyUnavailableError {
  declare readonly code: "storage_read_temporarily_unavailable";

  constructor(message: string, retryAfterSeconds: number | undefined) {
    super(message, "storage_read_temporarily_unavailable", retryAfterSeconds);
    this.name = "StorageReadTemporarilyUnavailableError";
  }
}

/** The source artifact changed while a hierarchy traversal cursor was in use. */
export class HierarchyCursorStaleError extends Error {
  readonly status = 409 as const;
  readonly code = "hierarchy_cursor_stale" as const;
  readonly action = "restart_hierarchy_traversal" as const;
  readonly restartWithout = "search_after" as const;
  readonly retryable = false as const;

  constructor(message: string) {
    super(message);
    this.name = "HierarchyCursorStaleError";
  }
}

export const DEFAULT_WRITE_MAX_REQUEST_BYTES = 64 << 20;
export const DEFAULT_WRITE_MAX_RESPONSE_BYTES = 1 << 20;
const MAX_ERROR_RESPONSE_BYTES = 1 << 20;

export function authorizationHeader(auth: AntflyAuth | undefined): string | undefined {
  if (!auth) return undefined;
  if (!("type" in auth)) return `Basic ${btoa(`${auth.username}:${auth.password}`)}`;

  switch (auth.type) {
    case "basic":
      return `Basic ${btoa(`${auth.username}:${auth.password}`)}`;
    case "apiKey":
      return `ApiKey ${btoa(`${auth.keyId}:${auth.keySecret}`)}`;
    case "token":
      return `Bearer ${auth.token}`;
  }
}

type UserOperations = {
  getCurrentUser: () => Promise<CurrentUser | undefined>;
  list: () => Promise<UserSummary[] | undefined>;
  get: (userName: string) => Promise<User | undefined>;
  create: (userName: string, request: CreateUserRequest) => Promise<User | undefined>;
  delete: (userName: string) => Promise<boolean>;
  updatePassword: (userName: string, newPassword: string) => Promise<SuccessMessage | undefined>;
  getPermissions: (userName: string) => Promise<Permission[] | undefined>;
  addPermission: (userName: string, permission: Permission) => Promise<SuccessMessage | undefined>;
  removePermission: (
    userName: string,
    resource: string,
    resourceType: ResourceType
  ) => Promise<boolean>;
};

type CurrentUser = {
  username?: string;
  permissions?: Permission[];
  metadata?: Record<string, unknown> | null;
};

type UserSummary = {
  username?: string;
};

type SuccessMessage = {
  message?: string;
};

type ClusterTopology =
  paths["/db/v1/cluster"]["get"]["responses"][200]["content"]["application/json"];

function apiErrorMessage(error: unknown, fallback = "unknown error"): string {
  if (!error) return fallback;
  if (typeof error === "string") return error.trim() || fallback;
  if (error && typeof error === "object") {
    const fields = error as {
      error?: unknown;
      detail?: unknown;
      message?: unknown;
      title?: unknown;
    };
    for (const value of [fields.error, fields.detail, fields.message, fields.title]) {
      if (typeof value === "string" && value.trim()) return value;
    }
    try {
      return JSON.stringify(error);
    } catch {
      return fallback;
    }
  }
  return String(error);
}

function errorMessage(error: unknown): string {
  return apiErrorMessage(error);
}

function queryError(prefix: string, error: unknown, response: Response | undefined): Error {
  const stale = error && typeof error === "object" ? (error as Record<string, unknown>) : undefined;
  if (
    response?.status === 409 &&
    stale?.error === "hierarchy_cursor_stale" &&
    stale.action === "restart_hierarchy_traversal" &&
    stale.restart_without === "search_after" &&
    stale.retryable === false
  ) {
    const detail =
      typeof stale.message === "string" && stale.message.trim()
        ? stale.message.trim()
        : errorMessage(error);
    return new HierarchyCursorStaleError(`${prefix}: ${detail}`);
  }
  const message = `${prefix}: ${errorMessage(error)}`;
  const code = error && typeof error === "object" ? (error as { code?: unknown }).code : undefined;
  if (
    response?.status === 503 &&
    typeof code === "string" &&
    (QUERY_TEMPORARILY_UNAVAILABLE_CODES as readonly string[]).includes(code) &&
    (error as { retryable?: unknown }).retryable === true
  ) {
    const retryAfter = Number.parseInt(response?.headers.get("Retry-After") ?? "", 10);
    const retryAfterSeconds =
      Number.isSafeInteger(retryAfter) && retryAfter > 0 ? retryAfter : undefined;
    if (code === "storage_read_temporarily_unavailable") {
      return new StorageReadTemporarilyUnavailableError(message, retryAfterSeconds);
    }
    return new QueryTemporarilyUnavailableError(
      message,
      code as QueryTemporarilyUnavailableCode,
      retryAfterSeconds
    );
  }
  return new Error(message);
}

function normalizedWriteOptions(
  options?: WriteOptions
): Required<Pick<WriteOptions, "maxRequestBytes" | "maxResponseBytes">> &
  Pick<WriteOptions, "signal"> {
  return {
    maxRequestBytes:
      options?.maxRequestBytes && options.maxRequestBytes > 0
        ? options.maxRequestBytes
        : DEFAULT_WRITE_MAX_REQUEST_BYTES,
    maxResponseBytes:
      options?.maxResponseBytes && options.maxResponseBytes > 0
        ? options.maxResponseBytes
        : DEFAULT_WRITE_MAX_RESPONSE_BYTES,
    signal: options?.signal,
  };
}

function encodeBoundedJSON(value: unknown, maxBytes: number): string {
  const encoded = JSON.stringify(value);
  if (new TextEncoder().encode(encoded).byteLength > maxBytes) {
    throw new Error(`encoded request exceeded ${maxBytes} bytes`);
  }
  return encoded;
}

export async function readLimitedResponseBytes(
  response: Response,
  maxBytes: number
): Promise<{ bytes: Uint8Array<ArrayBuffer>; truncated: boolean }> {
  if (!response.body) {
    return { bytes: new Uint8Array(0), truncated: false };
  }

  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  let truncated = false;

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      const remaining = maxBytes - total;
      if (remaining <= 0) {
        truncated = true;
        break;
      }
      if (value.byteLength > remaining) {
        chunks.push(value.slice(0, remaining));
        total += remaining;
        truncated = true;
        break;
      }
      chunks.push(value);
      total += value.byteLength;
    }
  } finally {
    if (truncated) {
      try {
        await reader.cancel();
      } catch {
        // Preserve the response limit result when cancellation itself fails.
      }
    }
    reader.releaseLock();
  }

  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return { bytes, truncated };
}

export async function readLimitedResponseText(
  response: Response,
  maxBytes: number
): Promise<{ text: string; truncated: boolean }> {
  const { bytes, truncated } = await readLimitedResponseBytes(response, maxBytes);
  return { text: new TextDecoder().decode(bytes), truncated };
}

function parseJSON<T>(text: string): T {
  return JSON.parse(text) as T;
}

export class AntflyClient {
  private client: Client<paths>;
  private config: AntflyConfig;

  constructor(config: AntflyConfig) {
    this.config = {
      ...config,
      baseUrl: normalizeBaseUrl(config.baseUrl),
    };
    this.client = this.buildClient();
  }

  /**
   * Build the Authorization header value from the auth config.
   * Returns undefined if no auth is configured.
   */
  private getAuthHeader(): string | undefined {
    return authorizationHeader(this.config.auth);
  }

  /**
   * Build the openapi-fetch client with current config.
   */
  private buildClient(): Client<paths> {
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      ...this.config.headers,
    };

    const authHeader = this.getAuthHeader();
    if (authHeader) {
      headers.Authorization = authHeader;
    }

    return createClient<paths>({
      baseUrl: normalizeBaseUrl(this.config.baseUrl),
      headers,
      bodySerializer: (body) => {
        if (typeof body === "string") {
          return body;
        }
        return JSON.stringify(body);
      },
    });
  }

  private requestHeaders(extra?: Record<string, string>): Record<string, string> {
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      ...this.config.headers,
      ...extra,
    };

    const authHeader = this.getAuthHeader();
    if (authHeader) {
      headers.Authorization = authHeader;
    }

    return headers;
  }

  private url(path: string): string {
    return `${normalizeBaseUrl(this.config.baseUrl)}${path}`;
  }

  private async postBoundedJSON<T>(
    path: string,
    body: unknown,
    options: WriteOptions | undefined,
    errorPrefix: string,
    marshalErrorPrefix: string
  ): Promise<{ data?: T; text: string }> {
    const opts = normalizedWriteOptions(options);
    let encodedBody: string;
    try {
      encodedBody = encodeBoundedJSON(body, opts.maxRequestBytes);
    } catch (error) {
      throw new Error(`${marshalErrorPrefix}: ${(error as Error).message}`);
    }

    const response = await fetch(this.url(path), {
      method: "POST",
      headers: this.requestHeaders(),
      body: encodedBody,
      signal: opts.signal,
    });

    if (!response.ok) {
      const { text, truncated } = await readLimitedResponseText(response, MAX_ERROR_RESPONSE_BYTES);
      let message = apiErrorMessage(text);
      try {
        message = apiErrorMessage(parseJSON<unknown>(text), message);
      } catch {
        // Non-JSON error bodies are reported as-is below.
      }
      if (truncated) {
        message = `${message} (response body exceeded ${MAX_ERROR_RESPONSE_BYTES} bytes)`;
      }
      throw new Error(`${errorPrefix}: ${response.status} ${message}`);
    }

    const { text, truncated } = await readLimitedResponseText(response, opts.maxResponseBytes);
    if (truncated) {
      throw new Error(`${errorPrefix} response exceeded ${opts.maxResponseBytes} bytes`);
    }
    if (!text.trim()) {
      return { text };
    }
    return { data: parseJSON<T>(text), text };
  }

  /**
   * Update authentication credentials.
   * Accepts any auth type: basic (username/password), apiKey, or token.
   * For backwards compat, calling setAuth(username, password) still works.
   */
  setAuth(auth: AntflyAuth): void;
  setAuth(username: string, password: string): void;
  setAuth(authOrUsername: AntflyAuth | string, password?: string) {
    if (typeof authOrUsername === "string" && password !== undefined) {
      this.config.auth = { username: authOrUsername, password };
    } else {
      this.config.auth = authOrUsername as AntflyAuth;
    }
    this.client = this.buildClient();
  }

  /**
   * Get cluster status
   */
  async getStatus() {
    const { data, error } = await this.client.GET("/db/v1/status");
    if (error) throw new Error(`Failed to get status: ${errorMessage(error)}`);
    return data;
  }

  /**
   * Get cluster topology and data placement status.
   */
  async getClusterStatus(): Promise<ClusterTopology | undefined> {
    const { data, error } = await this.client.GET("/db/v1/cluster");
    if (error) throw new Error(`Failed to get cluster: ${errorMessage(error)}`);
    return data;
  }

  connections = {
    /**
     * List configured external connections (inference providers, web search,
     * external IO, CDC sources). Pass `include: ["models"]` to
     * live-query each inference provider's model listing API.
     * Returns undefined when the server predates the endpoint.
     */
    list: async (params?: {
      types?: string[];
      include?: "models"[];
      refresh?: boolean;
      signal?: AbortSignal;
    }): Promise<ConnectionsResponse | undefined> => {
      const query: { types?: string; include?: string; refresh?: string } = {};
      if (params?.types?.length) query.types = params.types.join(",");
      if (params?.include?.length) query.include = params.include.join(",");
      if (params?.refresh) query.refresh = "true";
      const { data, error, response } = await this.client.GET("/db/v1/connections", {
        params: { query },
        signal: params?.signal,
      });
      if (response?.status === 404) return undefined;
      if (error) throw new Error(`Failed to list connections: ${errorMessage(error)}`);
      return data;
    },
  };

  /**
   * Private helper for query requests to avoid code duplication
   */
  private async performQuery(
    path: "/db/v1/query" | "/db/v1/tables/{tableName}/query",
    request: QueryRequest,
    tableName?: string
  ): Promise<QueryResponses | undefined> {
    if (path === "/db/v1/tables/{tableName}/query" && tableName) {
      const { data, error, response } = await this.client.POST("/db/v1/tables/{tableName}/query", {
        params: { path: { tableName } },
        body: request,
      });
      if (error) throw queryError("Table query failed", error, response);
      return data;
    } else {
      const { data, error, response } = await this.client.POST("/db/v1/query", {
        body: request,
      });
      if (error) throw queryError("Query failed", error, response);
      return data;
    }
  }

  /**
   * Private helper for multiquery requests to avoid code duplication
   */
  private async performMultiquery(
    path: "/db/v1/query" | "/db/v1/tables/{tableName}/query",
    requests: QueryRequest[],
    tableName?: string
  ): Promise<QueryResponses | undefined> {
    const ndjson = `${requests.map((request) => JSON.stringify(request)).join("\n")}\n`;

    if (path === "/db/v1/tables/{tableName}/query" && tableName) {
      const { data, error, response } = await this.client.POST("/db/v1/tables/{tableName}/query", {
        params: { path: { tableName } },
        body: ndjson,
        headers: {
          "Content-Type": "application/x-ndjson",
        },
      });
      if (error) throw queryError("Table multi-query failed", error, response);
      return data;
    } else {
      const { data, error, response } = await this.client.POST("/db/v1/query", {
        body: ndjson,
        headers: {
          "Content-Type": "application/x-ndjson",
        },
      });
      if (error) throw queryError("Multi-query failed", error, response);
      return data;
    }
  }

  /**
   * Global query operations
   */
  async query(request: QueryRequest): Promise<QueryResult | undefined> {
    const data = await this.performQuery("/db/v1/query", request);
    // The global query returns QueryResponses, extract the first result
    return data?.responses?.[0];
  }

  /**
   * Execute multiple queries in a single request
   */
  async multiquery(requests: QueryRequest[]): Promise<QueryResponses | undefined> {
    return this.performMultiquery("/db/v1/query", requests);
  }

  /**
   * Private helper for Retrieval Agent requests to handle streaming and non-streaming responses
   */
  private async performRetrievalAgent(
    request: RetrievalAgentRequest,
    callbacks?: RetrievalAgentStreamCallbacks
  ): Promise<RetrievalAgentResult | AbortController> {
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      Accept: "text/event-stream, application/json",
    };

    // Add auth header if configured
    const authHeader = this.getAuthHeader();
    if (authHeader) {
      headers.Authorization = authHeader;
    }

    // Merge with any additional headers
    Object.assign(headers, this.config.headers);

    const abortController = new AbortController();
    const response = await fetch(
      `${normalizeBaseUrl(this.config.baseUrl)}/db/v1/agents/retrieval`,
      {
        method: "POST",
        headers,
        body: JSON.stringify(request),
        signal: abortController.signal,
      }
    );

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Retrieval agent request failed: ${response.status} ${errorText}`);
    }

    if (!response.body) {
      throw new Error("Response body is null");
    }

    // Check content type to determine response format
    const contentType = response.headers.get("content-type") || "";
    const isJSON = contentType.includes("application/json");

    // Handle JSON response (non-streaming)
    if (isJSON) {
      const result = (await response.json()) as RetrievalAgentResult;
      return result;
    }

    // Handle SSE streaming response
    if (callbacks) {
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      let currentEvent = "";

      // Start reading the stream in the background
      (async () => {
        try {
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split("\n");
            buffer = lines.pop() || "";

            for (const line of lines) {
              if (!line.trim()) {
                currentEvent = "";
                continue;
              }

              if (line.startsWith("event: ")) {
                currentEvent = line.slice(7).trim();
              } else if (line.startsWith("data: ")) {
                const data = line.slice(6).trim();

                let sseError: Error | undefined;
                try {
                  switch (currentEvent) {
                    case "classification":
                      if (callbacks.onClassification) {
                        callbacks.onClassification(JSON.parse(data));
                      }
                      break;
                    case "reasoning":
                      if (callbacks.onReasoning) {
                        callbacks.onReasoning(JSON.parse(data));
                      }
                      break;
                    case "filter_applied":
                      if (callbacks.onFilterApplied) {
                        callbacks.onFilterApplied(JSON.parse(data));
                      }
                      break;
                    case "search_executed":
                      if (callbacks.onSearchExecuted) {
                        callbacks.onSearchExecuted(JSON.parse(data));
                      }
                      break;
                    case "hit":
                      if (callbacks.onHit) {
                        callbacks.onHit(JSON.parse(data));
                      }
                      break;
                    case "generation":
                      if (callbacks.onGeneration) {
                        callbacks.onGeneration(JSON.parse(data));
                      }
                      break;
                    case "step_started":
                      if (callbacks.onStepStarted) {
                        callbacks.onStepStarted(JSON.parse(data));
                      }
                      break;
                    case "step_progress":
                      if (callbacks.onStepProgress) {
                        callbacks.onStepProgress(JSON.parse(data));
                      }
                      break;
                    case "step_completed":
                      if (callbacks.onStepCompleted) {
                        callbacks.onStepCompleted(JSON.parse(data));
                      }
                      break;
                    case "confidence":
                      if (callbacks.onConfidence) {
                        callbacks.onConfidence(JSON.parse(data));
                      }
                      break;
                    case "followup":
                      if (callbacks.onFollowup) {
                        callbacks.onFollowup(JSON.parse(data));
                      }
                      break;
                    case "eval":
                      if (callbacks.onEvalResult) {
                        callbacks.onEvalResult(JSON.parse(data));
                      }
                      break;
                    case "done":
                      if (callbacks.onDone) {
                        callbacks.onDone(JSON.parse(data));
                      }
                      return;
                    case "error": {
                      const parsed = JSON.parse(data);
                      const message =
                        typeof parsed === "object" && parsed.error ? parsed.error : String(parsed);
                      if (callbacks.onError) {
                        callbacks.onError(message);
                      }
                      sseError = new Error(message);
                      break;
                    }
                  }
                } catch (e) {
                  console.warn("Failed to parse SSE data:", currentEvent, data, e);
                }
                if (sseError) throw sseError;
              }
            }
          }
        } catch (error) {
          if ((error as Error).name !== "AbortError") {
            console.error("Retrieval agent streaming error:", error);
          }
        }
      })();
    }

    return abortController;
  }

  /**
   * Retrieval Agent - Unified retrieval pipeline with optional classification, generation, and eval
   * Supports pipeline mode (structured queries) and agentic mode (tool-calling with LLM)
   * Configure steps.classification, steps.answer, steps.eval to enable additional pipeline stages
   * @param request - Retrieval agent request with query, mode, and optional step configs
   * @param callbacks - Optional callbacks for SSE events (classification, reasoning, hit, answer, citation, confidence, followup_question, eval, done, error)
   * @returns Promise with RetrievalAgentResult (JSON) or AbortController (when streaming)
   */
  async retrievalAgent(
    request: RetrievalAgentRequest,
    callbacks?: RetrievalAgentStreamCallbacks
  ): Promise<RetrievalAgentResult | AbortController> {
    return this.performRetrievalAgent(request, callbacks);
  }

  /**
   * Chat Agent - Multi-turn conversational retrieval with message history management.
   * Wraps the retrieval agent with automatic message accumulation.
   * @param userMessage - The user's message for this turn
   * @param config - Chat configuration (generator, table, indexes, etc.)
   * @param history - Previous conversation messages (pass result.messages from prior turns)
   * @param callbacks - Optional streaming callbacks including chat-specific events
   * @returns For streaming: { abortController, messages } where messages is a Promise.
   *          For non-streaming: { result, messages }
   */
  async chatAgent(
    userMessage: string,
    config: ChatAgentConfig,
    history: ChatMessage[] = [],
    callbacks?: ChatStreamCallbacks
  ): Promise<
    ChatAgentTurnResult | { abortController: AbortController; messages: Promise<ChatMessage[]> }
  > {
    // Build retrieval agent request with conversation history
    const request: RetrievalAgentRequest = {
      query: userMessage,
      queries: [
        {
          table: config.table,
          semantic_search: userMessage,
          indexes: config.semanticIndexes,
          limit: config.limit ?? 10,
        },
      ],
      generator: config.generator,
      messages: [...history, { role: "user", content: userMessage }],
      max_internal_iterations: config.maxInternalIterations ?? 5,
      stream: !!callbacks,
      agent_knowledge: config.agentKnowledge,
    };

    if (config.steps) {
      request.steps = config.steps;
    }

    if (callbacks) {
      // Streaming mode: accumulate answer and emit chat-specific callbacks
      let answerText = "";
      let resolveMessages: (msgs: ChatMessage[]) => void;
      const messagesPromise = new Promise<ChatMessage[]>((resolve) => {
        resolveMessages = resolve;
      });

      const wrappedCallbacks: RetrievalAgentStreamCallbacks = {
        ...callbacks,
        onGeneration: (chunk: string) => {
          answerText += chunk;
          callbacks.onGeneration?.(chunk);
        },
        onDone: (data) => {
          // Build updated messages with assistant response
          const updatedMessages: ChatMessage[] = [
            ...history,
            { role: "user", content: userMessage },
            { role: "assistant", content: answerText },
          ];
          callbacks.onAssistantMessage?.(answerText);
          callbacks.onMessagesUpdated?.(updatedMessages);
          callbacks.onDone?.(data);
          resolveMessages(updatedMessages);
        },
      };

      const abortController = (await this.performRetrievalAgent(
        request,
        wrappedCallbacks
      )) as AbortController;

      return { abortController, messages: messagesPromise };
    }

    // Non-streaming mode
    const result = (await this.performRetrievalAgent(request)) as RetrievalAgentResult;

    // Use server-provided messages or build from response
    const updatedMessages: ChatMessage[] = result.messages?.length
      ? result.messages
      : [
          ...history,
          { role: "user", content: userMessage },
          ...(result.generation
            ? [{ role: "assistant" as const, content: result.generation }]
            : []),
        ];

    return { result, messages: updatedMessages };
  }

  /**
   * Query Builder Agent - Translates natural language into structured search queries
   * Uses an LLM to generate optimized Bleve queries from user intent
   * @param request - Query builder request with intent and optional table/schema context
   * @returns Promise with QueryBuilderResult containing the generated query, explanation, and confidence
   */
  async queryBuilderAgent(request: QueryBuilderRequest): Promise<QueryBuilderResult> {
    const { data, error } = await this.client.POST("/db/v1/agents/query-builder", {
      body: request,
    });
    if (error) throw new Error(`Query builder agent failed: ${error.error}`);
    // biome-ignore lint/style/noNonNullAssertion: data is guaranteed defined after error check
    return data! as unknown as QueryBuilderResult;
  }

  /**
   * Perform a cross-table batch operation atomically.
   */
  async multiBatch(request: MultiBatchRequest): Promise<MultiBatchResult> {
    return this.multiBatchWithOptions(request);
  }

  /**
   * Perform a cross-table batch operation with request and response size bounds.
   */
  async multiBatchWithOptions(
    request: MultiBatchRequest,
    options?: WriteOptions
  ): Promise<MultiBatchResult> {
    const { data } = await this.postBoundedJSON<MultiBatchResult>(
      "/db/v1/batch",
      request,
      options,
      "Multi-batch operation failed",
      "marshalling multi-batch request"
    );
    return data ?? {};
  }

  /**
   * Perform a stateless linear merge against a table.
   */
  async linearMerge(tableName: string, request: LinearMergeRequest): Promise<LinearMergeResult> {
    return this.linearMergeWithOptions(tableName, request);
  }

  /**
   * Perform a stateless linear merge with request and response size bounds.
   */
  async linearMergeWithOptions(
    tableName: string,
    request: LinearMergeRequest,
    options?: WriteOptions
  ): Promise<LinearMergeResult> {
    const { data } = await this.postBoundedJSON<LinearMergeResult>(
      `/db/v1/tables/${encodeURIComponent(tableName)}/merge`,
      request,
      options,
      "Linear merge operation failed",
      "marshalling linear merge request"
    );
    if (!data) throw new Error("Linear merge operation failed: unexpected empty response");
    return data;
  }

  /**
   * Table operations
   */
  tables = {
    /**
     * List all tables
     */
    list: async (params?: { prefix?: string; pattern?: string }) => {
      const { data, error } = await this.client.GET("/db/v1/tables", {
        params: params ? { query: params } : undefined,
      });
      if (error) throw new Error(`Failed to list tables: ${error.error}`);
      return data;
    },

    /**
     * Get table details and status
     */
    get: async (tableName: string) => {
      const { data, error } = await this.client.GET("/db/v1/tables/{tableName}", {
        params: { path: { tableName } },
      });
      if (error) throw new Error(`Failed to get table: ${error.error}`);
      return data;
    },

    /**
     * Create a new table
     */
    create: async (tableName: string, config: CreateTableRequest = {}) => {
      const { data, error } = await this.client.POST("/db/v1/tables/{tableName}", {
        params: { path: { tableName } },
        body: config,
      });
      if (error) {
        throw new Error(`Failed to create table: ${apiErrorMessage(error, "unknown error")}`);
      }
      return data;
    },

    /**
     * Drop a table
     */
    drop: async (tableName: string) => {
      const { error } = await this.client.DELETE("/db/v1/tables/{tableName}", {
        params: { path: { tableName } },
      });
      if (error) throw new Error(`Failed to drop table: ${error.error}`);
      return true;
    },

    /**
     * Update schema for a table
     */
    updateSchema: async (tableName: string, config: TableSchema) => {
      const { data, error } = await this.client.PUT("/db/v1/tables/{tableName}/schema", {
        params: { path: { tableName } },
        body: config,
      });
      if (error) throw new Error(`Failed to update table schema: ${error.error}`);
      return data;
    },

    /**
     * Query a specific table
     */
    query: async (tableName: string, request: QueryRequest) => {
      return this.performQuery("/db/v1/tables/{tableName}/query", request, tableName);
    },

    /**
     * Execute multiple queries on a specific table
     */
    multiquery: async (tableName: string, requests: QueryRequest[]) => {
      return this.performMultiquery("/db/v1/tables/{tableName}/query", requests, tableName);
    },

    /**
     * Perform batch operations on a table
     */
    batch: async (tableName: string, request: BatchRequest): Promise<BatchResult> => {
      return this.tables.batchWithOptions(tableName, request);
    },

    /**
     * Perform batch operations on a table with request and response size bounds.
     */
    batchWithOptions: async (
      tableName: string,
      request: BatchRequest,
      options?: WriteOptions
    ): Promise<BatchResult> => {
      const { data } = await this.postBoundedJSON<BatchResult>(
        `/db/v1/tables/${encodeURIComponent(tableName)}/batch`,
        request,
        options,
        "Batch operation failed",
        "marshalling batch request"
      );
      return (
        data ?? {
          inserted: Object.keys(request.inserts ?? {}).length,
          deleted: request.deletes?.length ?? 0,
          transformed: request.transforms?.length ?? 0,
        }
      );
    },

    /**
     * Backup a table
     */
    backup: async (tableName: string, request: BackupRequest) => {
      const { data, error } = await this.client.POST("/db/v1/tables/{tableName}/backup", {
        params: { path: { tableName } },
        body: request,
      });
      if (error) throw new Error(`Backup failed: ${error.error}`);
      return data;
    },

    /**
     * Restore a table from backup
     */
    restore: async (
      tableName: string,
      request: RestoreRequest,
      options?: RestoreOptions
    ): Promise<RestoreJob> => {
      const { data, error } = await this.client.POST("/db/v1/tables/{tableName}/restore", {
        params: { path: { tableName } },
        ...(options?.idempotencyKey
          ? { headers: { "Idempotency-Key": options.idempotencyKey } }
          : {}),
        body: request,
      });
      if (error) throw new Error(`Restore failed: ${error.error}`);
      if (!data) throw new Error("Restore failed: unexpected empty response");
      return data;
    },

    /**
     * Lookup a specific key in a table
     * @param tableName - Name of the table
     * @param key - Key of the record to lookup
     * @param options - Optional parameters
     * @param options.fields - Comma-separated list of fields to include (e.g., "title,author,metadata.tags")
     */
    lookup: async (tableName: string, key: string, options?: { fields?: string }) => {
      const { data, error } = await this.client.GET("/db/v1/tables/{tableName}/documents/{key}", {
        params: {
          path: { tableName, key },
          query: options?.fields ? { fields: options.fields } : undefined,
        },
      });
      if (error) throw new Error(`Key lookup failed: ${error.error}`);
      return data;
    },

    artifacts: {
      /**
       * List table-level generated artifact enrichment definitions.
       */
      listEnrichments: async (
        tableName: string
      ): Promise<TableArtifactEnrichmentList | undefined> => {
        const { data, error } = await this.client.GET("/db/v1/tables/{tableName}/artifacts", {
          params: {
            path: { tableName },
          },
        });
        if (error) {
          throw new Error(`Failed to list artifact enrichments: ${apiErrorMessage(error)}`);
        }
        return data;
      },

      /**
       * Register or replace a table-level generated artifact enrichment.
       */
      putEnrichment: async (
        tableName: string,
        artifactName: string,
        config: EnrichmentConfig
      ): Promise<unknown> => {
        const { data, error } = await this.client.PUT(
          "/db/v1/tables/{tableName}/artifacts/{artifactName}/enrichment",
          {
            params: {
              path: { tableName, artifactName },
            },
            body: config,
          }
        );
        if (error) {
          throw new Error(`Failed to put artifact enrichment: ${apiErrorMessage(error)}`);
        }
        return data;
      },

      /**
       * Delete a table-level generated artifact enrichment.
       */
      deleteEnrichment: async (tableName: string, artifactName: string): Promise<unknown> => {
        const { data, error } = await this.client.DELETE(
          "/db/v1/tables/{tableName}/artifacts/{artifactName}/enrichment",
          {
            params: {
              path: { tableName, artifactName },
            },
          }
        );
        if (error) {
          throw new Error(`Failed to delete artifact enrichment: ${apiErrorMessage(error)}`);
        }
        return data;
      },

      /**
       * List derived artifact manifests attached to a document.
       */
      list: async (
        tableName: string,
        key: string,
        detail: "summary" | "raw" = "summary"
      ): Promise<DocumentArtifactManifestList | undefined> => {
        const { data, error } = await this.client.GET(
          "/db/v1/tables/{tableName}/documents/{key}/artifacts",
          {
            params: {
              path: { tableName, key },
              query: { detail },
            },
          }
        );
        if (error) {
          throw new Error(`Failed to list document artifacts: ${apiErrorMessage(error)}`);
        }
        return data;
      },

      /**
       * Get a single derived artifact manifest for a document.
       */
      get: async (
        tableName: string,
        key: string,
        artifactName: string,
        detail: "summary" | "raw" = "raw"
      ): Promise<DocumentArtifactManifest | undefined> => {
        const { data, error } = await this.client.GET(
          "/db/v1/tables/{tableName}/documents/{key}/artifacts/{artifactName}",
          {
            params: {
              path: { tableName, key, artifactName },
              query: { detail },
            },
          }
        );
        if (error) {
          throw new Error(`Failed to get document artifact: ${apiErrorMessage(error)}`);
        }
        return data;
      },

      /**
       * Reprocess one derived artifact for one document.
       */
      reprocessDocument: async (
        tableName: string,
        key: string,
        artifactName: string
      ): Promise<DocumentArtifactReprocessResponse | undefined> => {
        const { data, error } = await this.client.POST(
          "/db/v1/tables/{tableName}/documents/{key}/artifacts/{artifactName}/reprocess",
          {
            params: {
              path: { tableName, key, artifactName },
            },
          }
        );
        if (error) {
          throw new Error(`Failed to reprocess document artifact: ${apiErrorMessage(error)}`);
        }
        return data;
      },

      /**
       * Run one bounded table-wide reprocess pass for an artifact.
       */
      reprocessRange: async (
        tableName: string,
        artifactName: string,
        request: DocumentArtifactTableReprocessRequest = {}
      ): Promise<DocumentArtifactTableReprocessResponse | undefined> => {
        const { data, error } = await this.client.POST(
          "/db/v1/tables/{tableName}/artifacts/{artifactName}/reprocess",
          {
            params: {
              path: { tableName, artifactName },
            },
            body: request,
          }
        );
        if (error) {
          throw new Error(`Failed to reprocess table artifact range: ${apiErrorMessage(error)}`);
        }
        return data;
      },

      /**
       * Start a durable table-wide artifact reprocess job.
       */
      startReprocessJob: async (
        tableName: string,
        artifactName: string,
        request: DocumentArtifactReprocessJobStartRequest = {}
      ): Promise<DocumentArtifactReprocessJob | undefined> => {
        const { data, error } = await this.client.POST(
          "/db/v1/tables/{tableName}/artifacts/{artifactName}/reprocess-jobs",
          {
            params: {
              path: { tableName, artifactName },
            },
            body: request,
          }
        );
        if (error) {
          throw new Error(`Failed to start artifact reprocess job: ${apiErrorMessage(error)}`);
        }
        return data;
      },

      getReprocessJob: async (
        tableName: string,
        artifactName: string,
        jobId: string
      ): Promise<DocumentArtifactReprocessJob | undefined> => {
        const { data, error } = await this.client.GET(
          "/db/v1/tables/{tableName}/artifacts/{artifactName}/reprocess-jobs/{jobId}",
          {
            params: {
              path: { tableName, artifactName, jobId },
            },
          }
        );
        if (error) {
          throw new Error(`Failed to load artifact reprocess job: ${apiErrorMessage(error)}`);
        }
        return data;
      },

      advanceReprocessJob: async (
        tableName: string,
        artifactName: string,
        jobId: string
      ): Promise<DocumentArtifactReprocessJob | undefined> => {
        const { data, error } = await this.client.POST(
          "/db/v1/tables/{tableName}/artifacts/{artifactName}/reprocess-jobs/{jobId}/advance",
          {
            params: {
              path: { tableName, artifactName, jobId },
            },
          }
        );
        if (error) {
          throw new Error(`Failed to advance artifact reprocess job: ${apiErrorMessage(error)}`);
        }
        return data;
      },

      cancelReprocessJob: async (
        tableName: string,
        artifactName: string,
        jobId: string
      ): Promise<DocumentArtifactReprocessJob | undefined> => {
        const { data, error } = await this.client.POST(
          "/db/v1/tables/{tableName}/artifacts/{artifactName}/reprocess-jobs/{jobId}/cancel",
          {
            params: {
              path: { tableName, artifactName, jobId },
            },
          }
        );
        if (error) {
          throw new Error(`Failed to cancel artifact reprocess job: ${apiErrorMessage(error)}`);
        }
        return data;
      },
    },

    /**
     * Scan keys in a table within a key range
     * Returns documents as an async iterable, streaming results as NDJSON.
     * @param tableName - Name of the table
     * @param request - Scan request with optional key range, field projection, and filtering
     * @returns AsyncGenerator yielding documents with their keys
     */
    scan: (
      tableName: string,
      request?: ScanKeysRequest
    ): AsyncGenerator<{ _id: string; [key: string]: unknown }> => {
      const config = this.config;
      const authHeader = this.getAuthHeader();

      async function* scanGenerator(): AsyncGenerator<{ _id: string; [key: string]: unknown }> {
        const headers: Record<string, string> = {
          "Content-Type": "application/json",
          Accept: "application/x-ndjson",
        };

        // Add auth header if configured
        if (authHeader) {
          headers.Authorization = authHeader;
        }

        // Merge with any additional headers
        Object.assign(headers, config.headers);

        const response = await fetch(
          `${normalizeBaseUrl(config.baseUrl)}/db/v1/tables/${tableName}/documents`,
          {
            method: "POST",
            headers,
            body: JSON.stringify(request || {}),
          }
        );

        if (!response.ok) {
          const errorText = await response.text();
          throw new Error(`Scan failed: ${response.status} ${errorText}`);
        }

        if (!response.body) {
          throw new Error("Response body is null");
        }

        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;

          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split("\n");
          buffer = lines.pop() || "";

          for (const line of lines) {
            if (line.trim()) {
              yield JSON.parse(line);
            }
          }
        }

        // Handle any remaining content in buffer
        if (buffer.trim()) {
          yield JSON.parse(buffer);
        }
      }

      return scanGenerator();
    },

    /**
     * Scan keys in a table and collect all results into an array
     * Convenience method that consumes the scan AsyncGenerator
     * @param tableName - Name of the table
     * @param request - Scan request with optional key range, field projection, and filtering
     * @returns Promise with array of all matching documents
     */
    scanAll: async (
      tableName: string,
      request?: ScanKeysRequest
    ): Promise<Array<{ _id: string; [key: string]: unknown }>> => {
      const results: Array<{ _id: string; [key: string]: unknown }> = [];
      for await (const doc of this.tables.scan(tableName, request)) {
        results.push(doc);
      }
      return results;
    },
  };

  /** Durable cluster restore job operations. */
  restoreJobs = {
    startCluster: async (
      request: ClusterRestoreRequest,
      options?: RestoreOptions
    ): Promise<RestoreJob> => {
      const { data, error } = await this.client.POST("/db/v1/restore", {
        ...(options?.idempotencyKey
          ? { headers: { "Idempotency-Key": options.idempotencyKey } }
          : {}),
        body: request,
      });
      if (error) throw new Error(`Cluster restore failed: ${error.error}`);
      if (!data) throw new Error("Cluster restore failed: unexpected empty response");
      return data;
    },

    get: async (jobId: string): Promise<RestoreJob> => {
      const { data, error } = await this.client.GET("/db/v1/restore/jobs/{job_id}", {
        params: { path: { job_id: jobId } },
      });
      if (error) throw new Error(`Get restore job failed: ${error.error}`);
      if (!data) throw new Error("Get restore job failed: unexpected empty response");
      return data;
    },

    list: async (options: RestoreJobListOptions = {}): Promise<RestoreJobPage> => {
      const { data, error } = await this.client.GET("/db/v1/restore/jobs", {
        params: { query: options },
      });
      if (error) throw new Error(`List restore jobs failed: ${error.error}`);
      if (!data) throw new Error("List restore jobs failed: unexpected empty response");
      return data;
    },

    listAll: async (options: Omit<RestoreJobListOptions, "cursor"> = {}): Promise<RestoreJob[]> => {
      const jobs: RestoreJob[] = [];
      const seen = new Set<string>();
      let cursor: string | undefined;
      do {
        const page = await this.restoreJobs.list({ ...options, cursor });
        jobs.push(...page.jobs);
        cursor = page.next_cursor;
        if (cursor) {
          if (seen.has(cursor))
            throw new Error(`List restore jobs returned a repeated cursor: ${cursor}`);
          seen.add(cursor);
        }
      } while (cursor);
      return jobs;
    },

    cancel: async (jobId: string): Promise<RestoreJob> => {
      const { data, error } = await this.client.DELETE("/db/v1/restore/jobs/{job_id}", {
        params: { path: { job_id: jobId } },
      });
      if (error) throw new Error(`Cancel restore job failed: ${error.error}`);
      if (!data) throw new Error("Cancel restore job failed: unexpected empty response");
      return data;
    },
  };

  /**
   * Index operations
   */
  indexes: IndexOperations = {
    /**
     * List all indexes for a table
     */
    list: async (tableName: string) => {
      const { data, error } = await this.client.GET("/db/v1/tables/{tableName}/indexes", {
        params: { path: { tableName } },
      });
      if (error) throw new Error(`Failed to list indexes: ${error.error}`);
      if (!data) throw new Error("Failed to list indexes: unexpected empty response");
      return data;
    },

    /**
     * Get index details
     */
    get: async (tableName: string, indexName: string) => {
      const { data, error } = await this.client.GET(
        "/db/v1/tables/{tableName}/indexes/{indexName}",
        {
          params: { path: { tableName, indexName } },
        }
      );
      if (error) throw new Error(`Failed to get index: ${error.error}`);
      if (!data) throw new Error("Failed to get index: unexpected empty response");
      return data;
    },

    /**
     * Create a new index
     */
    create: async (tableName: string, indexName: string, config: CreateIndexRequest) => {
      const { data, error, response } = await this.client.POST(
        "/db/v1/tables/{tableName}/indexes/{indexName}",
        {
          params: { path: { tableName, indexName } },
          body: config,
        }
      );
      if (error) {
        const detail = error as {
          code?: unknown;
          error?: unknown;
          message?: unknown;
          retryable?: unknown;
          retry_after_ms?: unknown;
        };
        if (
          response?.status === 429 &&
          detail.code === "storage_resource_exhausted" &&
          detail.retryable === true
        ) {
          const retryAfterHeader = response.headers.get("Retry-After");
          const parsedRetryAfter = retryAfterHeader && /^[1-9]\d*$/.test(retryAfterHeader)
            ? Number(retryAfterHeader)
            : NaN;
          const retryAfterSeconds = Number.isFinite(parsedRetryAfter) && parsedRetryAfter > 0
            ? parsedRetryAfter
            : undefined;
          const retryAfterMs = typeof detail.retry_after_ms === "number" &&
            Number.isFinite(detail.retry_after_ms) &&
            detail.retry_after_ms > 0
            ? detail.retry_after_ms
            : (retryAfterSeconds ?? 0) * 1000;
          const message = typeof detail.message === "string" && detail.message
            ? detail.message
            : "storage capacity is temporarily exhausted";
          throw new StorageResourceExhaustedError(message, retryAfterMs, retryAfterSeconds);
        }
        throw new Error(`Failed to create index: ${detail.error}`);
      }
      if (!data) throw new Error("Failed to create index: unexpected empty response");
      return data;
    },

    /**
     * Drop an index
     */
    drop: async (tableName: string, indexName: string) => {
      const { error } = await this.client.DELETE("/db/v1/tables/{tableName}/indexes/{indexName}", {
        params: { path: { tableName, indexName } },
      });
      if (error) throw new Error(`Failed to drop index: ${error.error}`);
      return true;
    },
  };

  /**
   * User management operations
   */
  users: UserOperations = {
    /**
     * Get current authenticated user
     */
    getCurrentUser: async () => {
      const { data, error } = await this.client.GET("/auth/v1/me");
      if (error) throw new Error(`Failed to get current user: ${error.error}`);
      return data;
    },

    /**
     * List all users
     */
    list: async () => {
      const { data, error } = await this.client.GET("/auth/v1/users");
      if (error) throw new Error(`Failed to list users: ${error.error}`);
      return data;
    },

    /**
     * Get user details
     */
    get: async (userName: string) => {
      const { data, error } = await this.client.GET("/auth/v1/users/{userName}", {
        params: { path: { userName } },
      });
      if (error) throw new Error(`Failed to get user: ${error.error}`);
      return data;
    },

    /**
     * Create a new user
     */
    create: async (userName: string, request: CreateUserRequest) => {
      const { data, error } = await this.client.POST("/auth/v1/users/{userName}", {
        params: { path: { userName } },
        body: request,
      });
      if (error) throw new Error(`Failed to create user: ${error.error}`);
      return data;
    },

    /**
     * Delete a user
     */
    delete: async (userName: string) => {
      const { error } = await this.client.DELETE("/auth/v1/users/{userName}", {
        params: { path: { userName } },
      });
      if (error) throw new Error(`Failed to delete user: ${error.error}`);
      return true;
    },

    /**
     * Update user password
     */
    updatePassword: async (userName: string, newPassword: string) => {
      const { data, error } = await this.client.PUT("/auth/v1/users/{userName}/password", {
        params: { path: { userName } },
        body: { new_password: newPassword },
      });
      if (error) throw new Error(`Failed to update password: ${error.error}`);
      return data;
    },

    /**
     * Get user permissions
     */
    getPermissions: async (userName: string) => {
      const { data, error } = await this.client.GET("/auth/v1/users/{userName}/permissions", {
        params: { path: { userName } },
      });
      if (error) throw new Error(`Failed to get permissions: ${error.error}`);
      return data;
    },

    /**
     * Add permission to user
     */
    addPermission: async (userName: string, permission: Permission) => {
      const { data, error } = await this.client.POST("/auth/v1/users/{userName}/permissions", {
        params: { path: { userName } },
        body: permission,
      });
      if (error) throw new Error(`Failed to add permission: ${error.error}`);
      return data;
    },

    /**
     * Remove permission from user
     */
    removePermission: async (userName: string, resource: string, resourceType: ResourceType) => {
      const { error } = await this.client.DELETE("/auth/v1/users/{userName}/permissions", {
        params: {
          path: { userName },
          query: { resource, resourceType },
        },
      });
      if (error) throw new Error(`Failed to remove permission: ${error.error}`);
      return true;
    },
  };

  /**
   * Standalone evaluation for testing evaluators without running a query.
   * Evaluates a generated output against ground truth using LLM-as-judge metrics.
   * @param request - Eval request with evaluators, judge config, query, output, and ground truth
   * @returns Evaluation result with scores for each evaluator
   */
  async evaluate(
    request: import("./types.js").EvalRequest
  ): Promise<import("./types.js").EvalResult> {
    const { data, error } = await this.client.POST("/db/v1/eval", {
      body: request,
    });
    if (error) throw new Error(`Evaluation failed: ${error.error}`);
    return data;
  }

  /**
   * Get the underlying OpenAPI client for advanced use cases
   */
  getRawClient() {
    return this.client;
  }
}

export function normalizeBaseUrl(baseUrl: string): string {
  return baseUrl
    .trim()
    .replace(/\/$/, "")
    .replace(/\/db\/v1$/, "")
    .replace(/\/auth\/v1$/, "")
    .replace(/\/ai\/v1$/, "");
}
