/** Client for the optional antfly-source companion, at its own base URL. */
export interface SourceConnection {
  provider: "github" | "google_workspace" | "gcs" | "s3" | "slack" | "discord";
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
}

export interface SourceDefinition {
  id: string;
  alias?: string;
  connection: string;
  table: string;
  config: Record<string, unknown>;
  permissions: { public: boolean; readers: string[] };
  permission_ttl_seconds?: number;
  publication?: "text" | "media";
}

export interface SourceProfile {
  version: 1;
  endpoint: string;
  data_plane_token_env?: string;
  catalog_table?: string;
  principals?: string[];
  connections: Record<string, SourceConnection>;
  sources: SourceDefinition[];
}

export interface SourceSyncJob {
  version: 1;
  id: string;
  state: "running" | "completed" | "failed" | "cancelled" | "interrupted";
  started_at: number;
  finished_at?: number;
  code?: string;
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

export class SourcesClient {
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

  private async request<T>(
    method: string,
    path: string,
    body?: unknown,
    signal?: AbortSignal
  ): Promise<T> {
    const response = await this.fetcher(`${this.baseUrl}${path}`, {
      method,
      headers: { Authorization: `Bearer ${this.#token}`, "Content-Type": "application/json" },
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
  ): Promise<{ version: 1; state: string }> {
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

  sync(options: { restart?: boolean; signal?: AbortSignal } = {}): Promise<SourceSyncJob> {
    return this.request("POST", "/v1/sync", { restart: options.restart ?? false }, options.signal);
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
