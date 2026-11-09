/** Client for the optional antfly-source companion, at its own base URL. */
export interface SourceConnection {
  provider: "github" | "google_workspace" | "gcs" | "s3" | "local" | "slack" | "discord";
  secret_refs?: Partial<
    Record<
      "token" | "credentials" | "access_key_id" | "secret_access_key" | "session_token",
      string
    >
  >;
  services?: string[];
  token_env?: string;
  credentials_env?: string;
  access_key_env?: string;
  secret_key_env?: string;
  session_token_env?: string;
  s3_endpoint?: string;
  s3_insecure?: boolean;
  local_root?: string;
}

export interface SourceDefinition {
  id: string;
  alias?: string;
  connection: string;
  table: string;
  config: Record<string, unknown>;
  permissions: { public: boolean; readers: string[] };
  permission_ttl_seconds?: number;
  publication?: "native" | "text" | "media";
  search_version?: 1 | 2;
  streams?: SearchStream[];
  artifacts?: ArtifactBinding[];
  processing?: ProcessingDeclaration;
  knowledge?: KnowledgeDeclaration;
  identity?: { namespace?: string };
}

/** Durable migration lineage returned by the companion; clients cannot mutate it directly. */
export interface LocalMigrationReceipt {
  version: 1;
  source_id: string;
  table: string;
  namespace: string;
  from_connection: string;
  from_prefix: string;
  from_binding: string;
  to_connection: string;
  to_bucket: string;
  to_prefix: string;
  to_binding: string;
}

export interface SourceProfile {
  version: 1;
  endpoint: string;
  data_plane_token_env?: string;
  catalog_table?: string;
  principals?: string[];
  connections: Record<string, SourceConnection>;
  sources: SourceDefinition[];
  readonly local_migrations?: Record<string, LocalMigrationReceipt>;
}

export interface SourceSyncJob {
  version: 1;
  id: string;
  state: "running" | "completed" | "failed" | "cancelled" | "interrupted";
  started_at: number;
  finished_at?: number;
  code?: string;
  operation?: "sync" | "purge";
  purged_source_count?: number;
  failure?: { source_id?: string; phase: string; code: string; retryable: boolean };
}

export class SourceServiceError extends Error {
  constructor(
    public readonly status: number,
    public readonly code: string
  ) {
    super(`Source service returned ${status}: ${code}`);
    this.name = "SourceServiceError";
  }
}

class SourceTransport {
  private readonly baseUrl: string;
  readonly #token: string;
  private readonly fetcher: typeof fetch;

  constructor(options: { baseUrl: string; token: string; fetch?: typeof fetch }) {
    const url = new URL(options.baseUrl);
    const loopback = ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname);
    if (
      url.username ||
      url.password ||
      url.search ||
      url.hash ||
      (url.protocol !== "https:" && !(url.protocol === "http:" && loopback))
    )
      throw new Error("Source service requires HTTPS or loopback HTTP without URL credentials");
    if (!options.token || /[\r\n]/.test(options.token))
      throw new Error("Invalid source service token");
    this.baseUrl = url.toString().replace(/\/$/, "");
    this.#token = options.token;
    this.fetcher = options.fetch ?? globalThis.fetch;
  }

  protected async request<T>(
    method: string,
    path: string,
    body?: unknown,
    signal?: AbortSignal,
    headers: Record<string, string> = {}
  ): Promise<T> {
    const response = await this.fetcher(`${this.baseUrl}${path}`, {
      method,
      headers: {
        ...headers,
        Authorization: `Bearer ${this.#token}`,
        "Content-Type": "application/json",
      },
      body: body === undefined ? undefined : JSON.stringify(body),
      redirect: "error",
      signal: signal ?? AbortSignal.timeout(30_000),
    });
    if (!response.ok) {
      let code = "request_failed";
      try {
        const value = (await response.json()) as { code?: unknown };
        if (typeof value.code === "string") code = value.code;
      } catch {
        /* Non-JSON proxy error. */
      }
      throw new SourceServiceError(response.status, code);
    }
    return response.json() as Promise<T>;
  }
}

/** Standalone-only credential and legacy profile operations. */
export class StandaloneSourcesClient extends SourceTransport {
  putSecret(
    key: string,
    value: string,
    signal?: AbortSignal
  ): Promise<{ version: 1; state: string }> {
    return this.request("PUT", `/v1/secrets/${encodeURIComponent(key)}`, { value }, signal);
  }
  revokeSecret(key: string, signal?: AbortSignal): Promise<{ version: 1; state: string }> {
    return this.request("DELETE", `/v1/secrets/${encodeURIComponent(key)}`, undefined, signal);
  }
  profile(signal?: AbortSignal): Promise<SourceProfile> {
    return this.request("GET", "/v1/profile", undefined, signal);
  }
  putConnection(
    name: string,
    connection: SourceConnection,
    signal?: AbortSignal
  ): Promise<SourceConnection> {
    return this.request("PUT", `/v1/connections/${encodeURIComponent(name)}`, connection, signal);
  }
  revokeConnection(name: string, signal?: AbortSignal): Promise<{ version: 1; state: string }> {
    return this.request("DELETE", `/v1/connections/${encodeURIComponent(name)}`, undefined, signal);
  }
  putSource(
    source: SourceDefinition,
    signal?: AbortSignal
  ): Promise<{ version: 1; state: string; rekeys: boolean }> {
    return this.request("PUT", `/v1/sources/${encodeURIComponent(source.id)}`, source, signal);
  }
  removeSource(id: string, signal?: AbortSignal): Promise<{ version: 1; state: string }> {
    return this.request("DELETE", `/v1/sources/${encodeURIComponent(id)}`, undefined, signal);
  }
  /** Remove the definition and queue durable row deletion. Call sync() to apply it. */
  purgeSource(id: string, signal?: AbortSignal): Promise<{ version: 1; state: string }> {
    return this.request(
      "DELETE",
      `/v1/sources/${encodeURIComponent(id)}?purge=true`,
      undefined,
      signal
    );
  }

  sync(
    options: {
      sourceId?: string;
      restart?: boolean;
      requestKey?: string;
      signal?: AbortSignal;
    } = {}
  ): Promise<SourceSyncJob> {
    return this.request(
      "POST",
      "/v1/sync",
      {
        source_id: options.sourceId,
        restart: options.restart ?? false,
        request_key: options.requestKey,
      },
      options.signal
    );
  }
  /** Apply durable purge intents independently of ingestion credentials. */
  runPurges(signal?: AbortSignal): Promise<SourceSyncJob> {
    return this.request("POST", "/v1/sync", { purge_only: true }, signal);
  }
  currentJob(signal?: AbortSignal): Promise<SourceSyncJob> {
    return this.request("GET", "/v1/jobs", undefined, signal);
  }
  job(id: string, signal?: AbortSignal): Promise<SourceSyncJob> {
    return this.request("GET", `/v1/jobs/${encodeURIComponent(id)}`, undefined, signal);
  }
  cancelJob(id: string, signal?: AbortSignal): Promise<SourceSyncJob> {
    return this.request("DELETE", `/v1/jobs/${encodeURIComponent(id)}`, undefined, signal);
  }
}

export type {
  ArtifactBinding,
  Capabilities,
  EntityBinding,
  GraphBinding,
  KnowledgeDeclaration,
  KnowledgeRequest,
  KnowledgeStatus,
  Mutation,
  ProcessingDeclaration,
  ProcessingJobRequest,
  ProcessingPreset,
  ProcessingPreview,
  RetrievalBindings,
  Run,
  RunInput,
  RunPage,
  SearchStatus,
  SearchStream,
  Source,
  SourcePage,
  TextStream,
  VectorSpace,
} from "./sources-types.js";

import type {
  ArtifactBinding,
  Capabilities,
  EntityBinding,
  GraphBinding,
  KnowledgeDeclaration,
  KnowledgeRequest,
  KnowledgeStatus,
  Mutation,
  ProcessingDeclaration,
  ProcessingJobRequest,
  ProcessingPreset,
  ProcessingPreview,
  RetrievalBindings,
  Run,
  RunInput,
  RunPage,
  SearchStatus,
  SearchStream,
  Source,
  SourcePage,
  TextStream,
  VectorSpace,
} from "./sources-types.js";

/** Common management API. baseUrl includes the runtime's /sources/v1 mount. */
export class SourcesClient extends SourceTransport {
  capabilities(signal?: AbortSignal): Promise<Capabilities> {
    return this.request("GET", "/capabilities", undefined, signal);
  }
  sources(
    options: { limit?: number; cursor?: string; signal?: AbortSignal } = {}
  ): Promise<SourcePage> {
    const query = new URLSearchParams();
    if (options.limit !== undefined) query.set("limit", String(options.limit));
    if (options.cursor) query.set("cursor", options.cursor);
    return this.request("GET", `/sources?${query}`, undefined, options.signal);
  }
  source(id: string, signal?: AbortSignal): Promise<Source> {
    return this.request("GET", `/sources/${encodeURIComponent(id)}`, undefined, signal);
  }
  previewProcessing(
    id: string,
    input: ProcessingPreset,
    signal?: AbortSignal
  ): Promise<ProcessingPreview> {
    return this.request(
      "POST",
      `/sources/${encodeURIComponent(id)}/processing-preview`,
      input,
      signal
    );
  }
  queryKnowledge(
    id: string,
    input: KnowledgeRequest,
    signal?: AbortSignal
  ): Promise<Record<string, unknown>> {
    return this.request(
      "POST",
      `/sources/${encodeURIComponent(id)}/knowledge-query`,
      input,
      signal
    );
  }
  processingJob(
    id: string,
    input: ProcessingJobRequest,
    signal?: AbortSignal
  ): Promise<Record<string, unknown>> {
    return this.request(
      "POST",
      `/sources/${encodeURIComponent(id)}/processing-jobs`,
      input,
      signal
    );
  }
  putSource(source: Source, signal?: AbortSignal): Promise<Source> {
    const { search_status: _observed, knowledge_status: _knowledge, ...desired } = source;
    return this.request("PUT", `/sources/${encodeURIComponent(source.id)}`, desired, signal);
  }
  removeSource(
    id: string,
    options: { purge?: boolean; signal?: AbortSignal } = {}
  ): Promise<Mutation> {
    return this.request(
      "DELETE",
      `/sources/${encodeURIComponent(id)}?purge=${options.purge ?? false}`,
      undefined,
      options.signal
    );
  }
  runs(
    id: string,
    options: { limit?: number; cursor?: string; signal?: AbortSignal } = {}
  ): Promise<RunPage> {
    const query = new URLSearchParams();
    if (options.limit !== undefined) query.set("limit", String(options.limit));
    if (options.cursor) query.set("cursor", options.cursor);
    return this.request(
      "GET",
      `/sources/${encodeURIComponent(id)}/runs?${query}`,
      undefined,
      options.signal
    );
  }
  run(sourceId: string, runId: string, signal?: AbortSignal): Promise<Run> {
    return this.request(
      "GET",
      `/sources/${encodeURIComponent(sourceId)}/runs/${encodeURIComponent(runId)}`,
      undefined,
      signal
    );
  }
  startRun(
    sourceId: string,
    key: string,
    input: RunInput = {},
    signal?: AbortSignal
  ): Promise<Run> {
    return this.request("POST", `/sources/${encodeURIComponent(sourceId)}/runs`, input, signal, {
      "Idempotency-Key": key,
    });
  }
  cancelRun(sourceId: string, runId: string, signal?: AbortSignal): Promise<Run> {
    return this.request(
      "POST",
      `/sources/${encodeURIComponent(sourceId)}/runs/${encodeURIComponent(runId)}/cancel`,
      {},
      signal
    );
  }
}
