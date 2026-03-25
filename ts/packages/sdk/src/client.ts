/**
 * Antfly SDK Client
 * Provides a high-level interface for interacting with the Antfly API
 */

import createClient, { type Client } from "openapi-fetch";
import type { paths } from "./antfly-api.js";
import type {
  AntflyAuth,
  AntflyConfig,
  BackupRequest,
  BatchRequest,
  ChatAgentConfig,
  ChatAgentTurnResult,
  ChatMessage,
  ChatStreamCallbacks,
  CreateTableRequest,
  CreateUserRequest,
  IndexConfig,
  Permission,
  QueryBuilderRequest,
  QueryBuilderResult,
  QueryRequest,
  QueryResponses,
  QueryResult,
  ResourceType,
  RestoreRequest,
  RetrievalAgentRequest,
  RetrievalAgentResult,
  RetrievalAgentStreamCallbacks,
  ScanKeysRequest,
  TableSchema,
} from "./types.js";

export class AntflyClient {
  private client: Client<paths>;
  private config: AntflyConfig;

  constructor(config: AntflyConfig) {
    this.config = config;
    this.client = this.buildClient();
  }

  /**
   * Build the Authorization header value from the auth config.
   * Returns undefined if no auth is configured.
   */
  private getAuthHeader(): string | undefined {
    const auth = this.config.auth;
    if (!auth) return undefined;

    if ("type" in auth) {
      switch (auth.type) {
        case "basic":
          return `Basic ${btoa(`${auth.username}:${auth.password}`)}`;
        case "apiKey":
          return `ApiKey ${btoa(`${auth.keyId}:${auth.keySecret}`)}`;
        case "bearer":
          return `Bearer ${auth.token}`;
      }
    }

    // Backwards compat: { username, password } without 'type' field
    return `Basic ${btoa(`${auth.username}:${auth.password}`)}`;
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
      baseUrl: this.config.baseUrl,
      headers,
      bodySerializer: (body) => {
        if (typeof body === "string") {
          return body;
        }
        return JSON.stringify(body);
      },
    });
  }

  /**
   * Update authentication credentials.
   * Accepts any auth type: basic (username/password), apiKey, or bearer.
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
    const { data, error } = await this.client.GET("/status");
    if (error) throw new Error(`Failed to get status: ${error.error}`);
    return data;
  }

  /**
   * Private helper for query requests to avoid code duplication
   */
  private async performQuery(
    path: "/query" | "/tables/{tableName}/query",
    request: QueryRequest,
    tableName?: string
  ): Promise<QueryResponses | undefined> {
    if (path === "/tables/{tableName}/query" && tableName) {
      const { data, error } = await this.client.POST("/tables/{tableName}/query", {
        params: { path: { tableName } },
        body: request,
      });
      if (error) throw new Error(`Table query failed: ${error.error}`);
      return data;
    } else {
      const { data, error } = await this.client.POST("/query", {
        body: request,
      });
      if (error) throw new Error(`Query failed: ${error.error}`);
      return data;
    }
  }

  /**
   * Private helper for multiquery requests to avoid code duplication
   */
  private async performMultiquery(
    path: "/query" | "/tables/{tableName}/query",
    requests: QueryRequest[],
    tableName?: string
  ): Promise<QueryResponses | undefined> {
    const ndjson = `${requests.map((request) => JSON.stringify(request)).join("\n")}\n`;

    if (path === "/tables/{tableName}/query" && tableName) {
      const { data, error } = await this.client.POST("/tables/{tableName}/query", {
        params: { path: { tableName } },
        body: ndjson,
        headers: {
          "Content-Type": "application/x-ndjson",
        },
      });
      if (error) throw new Error(`Table multi-query failed: ${error.error}`);
      return data;
    } else {
      const { data, error } = await this.client.POST("/query", {
        body: ndjson,
        headers: {
          "Content-Type": "application/x-ndjson",
        },
      });
      if (error) throw new Error(`Multi-query failed: ${error.error}`);
      return data;
    }
  }

  /**
   * Global query operations
   */
  async query(request: QueryRequest): Promise<QueryResult | undefined> {
    const data = await this.performQuery("/query", request);
    // The global query returns QueryResponses, extract the first result
    return data?.responses?.[0];
  }

  /**
   * Execute multiple queries in a single request
   */
  async multiquery(requests: QueryRequest[]): Promise<QueryResponses | undefined> {
    return this.performMultiquery("/query", requests);
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
    const response = await fetch(`${this.config.baseUrl}/agents/retrieval`, {
      method: "POST",
      headers,
      body: JSON.stringify(request),
      signal: abortController.signal,
    });

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
                      const message = typeof parsed === "object" && parsed.error ? parsed.error : String(parsed);
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
    const { data, error } = await this.client.POST("/agents/query-builder", {
      body: request,
    });
    if (error) throw new Error(`Query builder agent failed: ${error.error}`);
    // biome-ignore lint/style/noNonNullAssertion: data is guaranteed defined after error check
    return data!;
  }

  /**
   * Table operations
   */
  tables = {
    /**
     * List all tables
     */
    list: async (params?: { prefix?: string; pattern?: string }) => {
      const { data, error } = await this.client.GET("/tables", {
        params: params ? { query: params } : undefined,
      });
      if (error) throw new Error(`Failed to list tables: ${error.error}`);
      return data;
    },

    /**
     * Get table details and status
     */
    get: async (tableName: string) => {
      const { data, error } = await this.client.GET("/tables/{tableName}", {
        params: { path: { tableName } },
      });
      if (error) throw new Error(`Failed to get table: ${error.error}`);
      return data;
    },

    /**
     * Create a new table
     */
    create: async (tableName: string, config: CreateTableRequest = {}) => {
      const { data, error } = await this.client.POST("/tables/{tableName}", {
        params: { path: { tableName } },
        body: config,
      });
      if (error) throw new Error(`Failed to create table: ${error.error}`);
      return data;
    },

    /**
     * Drop a table
     */
    drop: async (tableName: string) => {
      const { error } = await this.client.DELETE("/tables/{tableName}", {
        params: { path: { tableName } },
      });
      if (error) throw new Error(`Failed to drop table: ${error.error}`);
      return true;
    },

    /**
     * Update schema for a table
     */
    updateSchema: async (tableName: string, config: TableSchema) => {
      const { data, error } = await this.client.PUT("/tables/{tableName}/schema", {
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
      return this.performQuery("/tables/{tableName}/query", request, tableName);
    },

    /**
     * Execute multiple queries on a specific table
     */
    multiquery: async (tableName: string, requests: QueryRequest[]) => {
      return this.performMultiquery("/tables/{tableName}/query", requests, tableName);
    },

    /**
     * Perform batch operations on a table
     */
    batch: async (tableName: string, request: BatchRequest) => {
      const { data, error } = await this.client.POST("/tables/{tableName}/batch", {
        params: { path: { tableName } },
        // @ts-expect-error Our BatchRequest type allows any object shape for inserts
        body: request,
      });
      if (error) throw new Error(`Batch operation failed: ${error.error}`);
      return data;
    },

    /**
     * Backup a table
     */
    backup: async (tableName: string, request: BackupRequest) => {
      const { data, error } = await this.client.POST("/tables/{tableName}/backup", {
        params: { path: { tableName } },
        body: request,
      });
      if (error) throw new Error(`Backup failed: ${error.error}`);
      return data;
    },

    /**
     * Restore a table from backup
     */
    restore: async (tableName: string, request: RestoreRequest) => {
      const { data, error } = await this.client.POST("/tables/{tableName}/restore", {
        params: { path: { tableName } },
        body: request,
      });
      if (error) throw new Error(`Restore failed: ${error.error}`);
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
      const { data, error } = await this.client.GET("/tables/{tableName}/lookup/{key}", {
        params: {
          path: { tableName, key },
          query: options?.fields ? { fields: options.fields } : undefined,
        },
      });
      if (error) throw new Error(`Key lookup failed: ${error.error}`);
      return data;
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
    ): AsyncGenerator<{ _key: string; [key: string]: unknown }> => {
      const config = this.config;
      const authHeader = this.getAuthHeader();

      async function* scanGenerator(): AsyncGenerator<{ _key: string; [key: string]: unknown }> {
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

        const response = await fetch(`${config.baseUrl}/tables/${tableName}/lookup`, {
          method: "POST",
          headers,
          body: JSON.stringify(request || {}),
        });

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
    ): Promise<Array<{ _key: string; [key: string]: unknown }>> => {
      const results: Array<{ _key: string; [key: string]: unknown }> = [];
      for await (const doc of this.tables.scan(tableName, request)) {
        results.push(doc);
      }
      return results;
    },
  };

  /**
   * Index operations
   */
  indexes = {
    /**
     * List all indexes for a table
     */
    list: async (tableName: string) => {
      const { data, error } = await this.client.GET("/tables/{tableName}/indexes", {
        params: { path: { tableName } },
      });
      if (error) throw new Error(`Failed to list indexes: ${error.error}`);
      return data;
    },

    /**
     * Get index details
     */
    get: async (tableName: string, indexName: string) => {
      const { data, error } = await this.client.GET("/tables/{tableName}/indexes/{indexName}", {
        params: { path: { tableName, indexName } },
      });
      if (error) throw new Error(`Failed to get index: ${error.error}`);
      return data;
    },

    /**
     * Create a new index
     */
    create: async (tableName: string, config: IndexConfig) => {
      const { error } = await this.client.POST("/tables/{tableName}/indexes/{indexName}", {
        params: { path: { tableName, indexName: config.name } },
        body: config,
      });
      if (error) throw new Error(`Failed to create index: ${error.error}`);
      return true;
    },

    /**
     * Drop an index
     */
    drop: async (tableName: string, indexName: string) => {
      const { error } = await this.client.DELETE("/tables/{tableName}/indexes/{indexName}", {
        params: { path: { tableName, indexName } },
      });
      if (error) throw new Error(`Failed to drop index: ${error.error}`);
      return true;
    },
  };

  /**
   * User management operations
   */
  users = {
    /**
     * Get current authenticated user
     */
    getCurrentUser: async () => {
      const { data, error } = await this.client.GET("/users/me");
      if (error) throw new Error(`Failed to get current user: ${error.error}`);
      return data;
    },

    /**
     * List all users
     */
    list: async () => {
      const { data, error } = await this.client.GET("/users");
      if (error) throw new Error(`Failed to list users: ${error.error}`);
      return data;
    },

    /**
     * Get user details
     */
    get: async (userName: string) => {
      const { data, error } = await this.client.GET("/users/{userName}", {
        params: { path: { userName } },
      });
      if (error) throw new Error(`Failed to get user: ${error.error}`);
      return data;
    },

    /**
     * Create a new user
     */
    create: async (userName: string, request: CreateUserRequest) => {
      const { data, error } = await this.client.POST("/users/{userName}", {
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
      const { error } = await this.client.DELETE("/users/{userName}", {
        params: { path: { userName } },
      });
      if (error) throw new Error(`Failed to delete user: ${error.error}`);
      return true;
    },

    /**
     * Update user password
     */
    updatePassword: async (userName: string, newPassword: string) => {
      const { data, error } = await this.client.PUT("/users/{userName}/password", {
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
      const { data, error } = await this.client.GET("/users/{userName}/permissions", {
        params: { path: { userName } },
      });
      if (error) throw new Error(`Failed to get permissions: ${error.error}`);
      return data;
    },

    /**
     * Add permission to user
     */
    addPermission: async (userName: string, permission: Permission) => {
      const { data, error } = await this.client.POST("/users/{userName}/permissions", {
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
      const { error } = await this.client.DELETE("/users/{userName}/permissions", {
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
    const { data, error } = await this.client.POST("/eval", {
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
