/**
 * Unit tests for the Antfly SDK client using Vitest
 */
import { beforeEach, describe, expect, it, vi } from "vitest";
import type {
  ClusterStatus,
  CreateTableRequest,
  GlobalQueryRequest,
  QueryRequest,
  TableQueryRequest,
  TableStatus,
} from "../src/types.js";

// Mock openapi-fetch at the top level
const mockGet = vi.fn();
const mockPost = vi.fn();
const mockPut = vi.fn();
const mockDelete = vi.fn();

vi.mock("openapi-fetch", () => ({
  default: vi.fn(() => ({
    GET: mockGet,
    POST: mockPost,
    PUT: mockPut,
    DELETE: mockDelete,
    OPTIONS: vi.fn(),
    HEAD: vi.fn(),
    PATCH: vi.fn(),
    TRACE: vi.fn(),
    request: vi.fn(),
    use: vi.fn(),
    eject: vi.fn(),
  })),
}));

// Import client after mocking
const {
  AntflyClient,
  HierarchyCursorStaleError,
  IndexMutationTemporarilyUnavailableError,
  QueryTemporarilyUnavailableError,
  StorageResourceExhaustedError,
  StorageReadTemporarilyUnavailableError,
  normalizeBaseUrl,
  readLimitedResponseBytes,
  readLimitedResponseText,
} = await import("../src/client.js");
const { default: createClient } = await import("openapi-fetch");

describe("bounded response readers", () => {
  it("treats a null response body as empty without invoking unbounded fallbacks", async () => {
    const arrayBuffer = vi.fn(() => Promise.reject(new Error("must not be called")));
    const text = vi.fn(() => Promise.reject(new Error("must not be called")));
    const response = { body: null, arrayBuffer, text } as unknown as Response;

    await expect(readLimitedResponseBytes(response, 16)).resolves.toEqual({
      bytes: new Uint8Array(0),
      truncated: false,
    });
    await expect(readLimitedResponseText(response, 16)).resolves.toEqual({
      text: "",
      truncated: false,
    });
    expect(arrayBuffer).not.toHaveBeenCalled();
    expect(text).not.toHaveBeenCalled();
  });
});

describe("AntflyClient", () => {
  let client: AntflyClient;

  beforeEach(() => {
    // Clear all mocks before each test
    vi.clearAllMocks();

    // Create the client instance
    client = new AntflyClient({
      baseUrl: "http://localhost:8080",
      auth: {
        username: "test",
        password: "test",
      },
    });
  });

  describe("constructor", () => {
    it("should initialize with config", () => {
      expect(client).toBeInstanceOf(AntflyClient);
    });

    it("should have access to raw client", () => {
      expect(client.getRawClient()).toBeDefined();
    });

    it("should normalize local and CloudAF base URLs", () => {
      expect(normalizeBaseUrl("http://localhost:8080")).toBe("http://localhost:8080");
      expect(normalizeBaseUrl("http://localhost:8080/")).toBe("http://localhost:8080");
      expect(normalizeBaseUrl("http://localhost:8080/db/v1")).toBe("http://localhost:8080");
      expect(normalizeBaseUrl("http://localhost:8080/auth/v1")).toBe("http://localhost:8080");
      expect(normalizeBaseUrl("http://localhost:8080/ai/v1")).toBe("http://localhost:8080");
      expect(normalizeBaseUrl("https://platform.antfly.io/cloud/v1/instance")).toBe(
        "https://platform.antfly.io/cloud/v1/instance"
      );
      expect(normalizeBaseUrl("https://platform.antfly.io/cloud/v1/instance/db/v1")).toBe(
        "https://platform.antfly.io/cloud/v1/instance"
      );
    });

    it("should configure token auth", () => {
      new AntflyClient({
        baseUrl: "https://platform.antfly.io/cloud/v1/instance",
        auth: { type: "token", token: "antflydb_test" },
      });

      expect(createClient).toHaveBeenLastCalledWith(
        expect.objectContaining({
          baseUrl: "https://platform.antfly.io/cloud/v1/instance",
          headers: expect.objectContaining({
            Authorization: "Bearer antflydb_test",
          }),
        })
      );
    });
  });

  describe("status", () => {
    it("returns the typed deployment capability contract", async () => {
      const status: ClusterStatus = {
        health: "healthy",
        deployment_mode: "standalone",
        index_capabilities: { artifact_sources: true, artifact_sources_state: "available" },
      };
      mockGet.mockResolvedValueOnce({ data: status, error: undefined });

      await expect(client.getStatus()).resolves.toEqual(status);
      expect(mockGet).toHaveBeenCalledWith("/db/v1/status");
    });

    it("rejects an empty successful status response", async () => {
      mockGet.mockResolvedValueOnce({ data: undefined, error: undefined });

      await expect(client.getStatus()).rejects.toThrow("response body was empty");
    });
  });

  describe("query", () => {
    it("should execute global query", async () => {
      const mockResponse = {
        responses: [
          {
            hits: {
              total: { value: 1, relation: "exact" },
              hits: [{ _id: "test", _score: 1.0, _source: { name: "test" } }],
            },
            took: 10,
            status: 200,
          },
        ],
      };

      mockPost.mockResolvedValueOnce({
        data: mockResponse,
        error: undefined,
      });

      const request: GlobalQueryRequest = {
        table: "test",
        limit: 10,
      };

      const result = await client.query(request);
      expect(result).toEqual(mockResponse.responses[0]);
      expect(mockPost).toHaveBeenCalledWith("/db/v1/query", {
        body: request,
      });
    });

    it("forwards global query cancellation", async () => {
      mockPost.mockResolvedValueOnce({
        data: { responses: [] },
        error: undefined,
      });
      const controller = new AbortController();
      const request: GlobalQueryRequest = { table: "products", limit: 3 };

      await client.query(request, { signal: controller.signal });

      expect(mockPost).toHaveBeenCalledWith("/db/v1/query", {
        body: request,
        signal: controller.signal,
      });
    });

    it("rejects a missing global table before transport", async () => {
      const invalid = { limit: 3 } as unknown as GlobalQueryRequest;

      await expect(client.query(invalid)).rejects.toThrow(
        "Global query request.table must be a non-empty string"
      );
      expect(mockPost).not.toHaveBeenCalled();
    });

    it("should handle query with Bleve full_text_search", async () => {
      const mockResponse = {
        responses: [
          {
            hits: {
              total: { value: 2, relation: "exact" },
              hits: [
                { _id: "1", _score: 1.5, _source: { name: "laptop" } },
                { _id: "2", _score: 1.2, _source: { name: "notebook" } },
              ],
            },
            took: 15,
            status: 200,
          },
        ],
      };

      mockPost.mockResolvedValueOnce({
        data: mockResponse,
        error: undefined,
      });

      const request: GlobalQueryRequest = {
        table: "products",
        full_text_index: "product_text",
        full_text_search: {
          match: "laptop",
          field: "name",
        },
        limit: 10,
      };

      const result = await client.query(request);
      expect(result?.hits?.total).toEqual({ value: 2, relation: "exact" });
      expect(mockPost).toHaveBeenCalledWith("/db/v1/query", {
        body: request,
      });
    });
  });

  describe("tables", () => {
    it("should list tables", async () => {
      const mockTables: TableStatus[] = [
        {
          name: "table1",
          indexes: {},
          shards: {},
          storage_status: { disk_usage: 1024, empty: false },
        },
        {
          name: "table2",
          indexes: {},
          shards: {},
          storage_status: { disk_usage: 2048, empty: true },
        },
      ];

      mockGet.mockResolvedValueOnce({
        data: mockTables,
        error: undefined,
      });

      const tables = await client.tables.list();
      expect(tables).toEqual(mockTables);
      expect(mockGet).toHaveBeenCalledWith("/db/v1/tables", {
        params: undefined,
      });
    });

    it("formats table metadata Problem Details errors", async () => {
      mockGet.mockResolvedValueOnce({
        data: undefined,
        error: {
          type: "about:blank",
          title: "Bad Gateway",
          status: 502,
          detail: "upstream table metadata request failed",
        },
        response: new Response(undefined, { status: 502 }),
      });

      await expect(client.tables.get("products")).rejects.toThrow(
        "Failed to get table: upstream table metadata request failed"
      );
    });

    it("should create a table", async () => {
      const mockTable = { name: "new_table", indexes: {}, shards: {} };

      mockPost.mockResolvedValueOnce({
        data: mockTable,
        error: undefined,
      });

      const config: CreateTableRequest = {
        num_shards: 3,
        schema: {
          version: 0,
          key: "id",
          default_type: "document",
        },
      };

      const result = await client.tables.create("new_table", config);
      expect(result).toEqual(mockTable);
      expect(mockPost).toHaveBeenCalledWith("/db/v1/tables/{tableName}", {
        params: { path: { tableName: "new_table" } },
        body: config,
      });
    });

    it("rejects invalid inline indexes before creating a table", async () => {
      const config = {
        indexes: {
          semantic: {
            type: "embeddings",
            source_artifact_name: "document_chunks_v1",
          },
        },
      } as CreateTableRequest;

      await expect(client.tables.create("new_table", config)).rejects.toThrow(
        'Invalid index "semantic": Embedding source_artifact_name requires a non-empty embedding_name.'
      );
      expect(mockPost).not.toHaveBeenCalled();
    });

    it("should query a specific table", async () => {
      const mockResponse = {
        responses: [
          {
            hits: {
              total: { value: 1, relation: "exact" },
              hits: [{ _id: "prod1", _score: 1.0, _source: { name: "Product 1" } }],
            },
            took: 20,
            status: 200,
          },
        ],
      };

      mockPost.mockResolvedValueOnce({
        data: mockResponse,
        error: undefined,
      });

      const request: TableQueryRequest = {
        full_text_search: {
          query: "laptop",
        },
        limit: 10,
      };

      const result = await client.tables.query("products", request);
      expect(result).toEqual(mockResponse);
      expect(mockPost).toHaveBeenCalledWith("/db/v1/tables/{tableName}/query", {
        params: { path: { tableName: "products" } },
        body: request,
      });
    });

    it("forwards table query cancellation", async () => {
      mockPost.mockResolvedValueOnce({
        data: { responses: [] },
        error: undefined,
      });
      const controller = new AbortController();
      const request: TableQueryRequest = { limit: 3 };

      await client.tables.query("products", request, { signal: controller.signal });

      expect(mockPost).toHaveBeenCalledWith("/db/v1/tables/{tableName}/query", {
        params: { path: { tableName: "products" } },
        body: request,
        signal: controller.signal,
      });
    });

    it("rejects a competing body table on table-scoped queries before transport", async () => {
      const ambiguous = { table: "other", limit: 3 } as QueryRequest;

      await expect(
        client.tables.query("products", ambiguous as unknown as TableQueryRequest)
      ).rejects.toThrow(
        'request.table must be omitted; the route already selects table "products"'
      );
      await expect(
        client.tables.multiquery("products", [ambiguous as unknown as TableQueryRequest])
      ).rejects.toThrow(
        'requests[0].table must be omitted; the route already selects table "products"'
      );
      expect(mockPost).not.toHaveBeenCalled();
    });

    it("formats table query Problem Details errors", async () => {
      mockPost.mockResolvedValueOnce({
        data: undefined,
        error: {
          type: "about:blank",
          title: "Bad Gateway",
          status: 502,
          detail: "upstream query response ended unexpectedly",
        },
        response: new Response(undefined, { status: 502 }),
      });

      await expect(client.tables.query("products", { limit: 3 })).rejects.toThrow(
        "Table query failed: upstream query response ended unexpectedly"
      );
    });

    it("should return the durable table restore job", async () => {
      const restoreJob = {
        job_id: "9223372036854775807",
        attempt_id: 0,
        scope: "table" as const,
        table_name: "products",
        backup_id: "nightly",
        phase: "queued" as const,
        cancel_requested: false,
        published_table_count: 0,
        completed_table_count: 0,
        total_table_count: 1,
        created_at_ms: 1,
        updated_at_ms: 1,
      };
      mockPost.mockResolvedValueOnce({ data: restoreJob, error: undefined });

      const result = await client.tables.restore(
        "products",
        {
          backup_id: "nightly",
          location: "s3://backups/nightly",
          connection: "archive",
        },
        { idempotencyKey: "products-nightly" }
      );

      expect(result).toEqual(restoreJob);
      expect(mockPost).toHaveBeenCalledWith("/db/v1/tables/{tableName}/restore", {
        params: { path: { tableName: "products" } },
        headers: { "Idempotency-Key": "products-nightly" },
        body: {
          backup_id: "nightly",
          location: "s3://backups/nightly",
          connection: "archive",
        },
      });
    });

    it("should manage durable cluster restore jobs with opaque ids", async () => {
      const restoreJob = {
        job_id: "9223372036854775807",
        attempt_id: 1,
        scope: "cluster" as const,
        backup_id: "nightly",
        phase: "running" as const,
        cancel_requested: false,
        published_table_count: 12,
        completed_table_count: 11,
        created_at_ms: 1,
        updated_at_ms: 2,
      };
      mockPost.mockResolvedValueOnce({ data: restoreJob, error: undefined });
      mockGet.mockResolvedValueOnce({ data: restoreJob, error: undefined });
      mockDelete.mockResolvedValueOnce({
        data: { ...restoreJob, cancel_requested: true },
        error: undefined,
      });

      await expect(
        client.restoreJobs.startCluster(
          {
            backup_id: "nightly",
            location: "s3://backups/nightly",
            connection: "archive",
          },
          { idempotencyKey: "cluster-nightly" }
        )
      ).resolves.toEqual(restoreJob);
      expect(mockPost).toHaveBeenCalledWith("/db/v1/restore", {
        headers: { "Idempotency-Key": "cluster-nightly" },
        body: {
          backup_id: "nightly",
          location: "s3://backups/nightly",
          connection: "archive",
        },
      });

      await expect(client.restoreJobs.get(restoreJob.job_id)).resolves.toEqual(restoreJob);
      expect(mockGet).toHaveBeenCalledWith("/db/v1/restore/jobs/{job_id}", {
        params: { path: { job_id: restoreJob.job_id } },
      });

      await expect(client.restoreJobs.cancel(restoreJob.job_id)).resolves.toEqual({
        ...restoreJob,
        cancel_requested: true,
      });
      expect(mockDelete).toHaveBeenCalledWith("/db/v1/restore/jobs/{job_id}", {
        params: { path: { job_id: restoreJob.job_id } },
      });
    });

    it("should paginate durable restore jobs across empty filtered pages", async () => {
      const restoreJob = {
        job_id: "42",
        attempt_id: 1,
        scope: "table" as const,
        table_name: "docs",
        backup_id: "nightly",
        phase: "running" as const,
        cancel_requested: false,
        published_table_count: 0,
        completed_table_count: 0,
        created_at_ms: 1,
        updated_at_ms: 2,
      };
      mockGet
        .mockResolvedValueOnce({ data: { jobs: [], next_cursor: "100" }, error: undefined })
        .mockResolvedValueOnce({ data: { jobs: [restoreJob] }, error: undefined });

      await expect(
        client.restoreJobs.listAll({ phase: "running", scope: "table" })
      ).resolves.toEqual([restoreJob]);
      expect(mockGet).toHaveBeenNthCalledWith(1, "/db/v1/restore/jobs", {
        params: { query: { phase: "running", scope: "table", cursor: undefined } },
      });
      expect(mockGet).toHaveBeenNthCalledWith(2, "/db/v1/restore/jobs", {
        params: { query: { phase: "running", scope: "table", cursor: "100" } },
      });
    });

    it("should reject an invalid empty table restore response", async () => {
      mockPost.mockResolvedValueOnce({ data: undefined, error: undefined });

      await expect(
        client.tables.restore("products", {
          backup_id: "nightly",
          location: "s3://backups/nightly",
          connection: "archive",
        })
      ).rejects.toThrow("Restore failed: unexpected empty response");
    });

    it("should perform bounded batch writes with fetch", async () => {
      const mockFetch = vi
        .spyOn(globalThis, "fetch")
        .mockResolvedValueOnce(new Response(JSON.stringify({ inserted: 1 }), { status: 201 }));

      const result = await client.tables.batchWithOptions(
        "products",
        {
          inserts: {
            "prod:1": { title: "Notebook" },
          },
        },
        {
          maxRequestBytes: 1024,
          maxResponseBytes: 1024,
        }
      );

      expect(result).toEqual({ inserted: 1 });
      const requestBody = mockFetch.mock.calls[0]?.[1]?.body;
      expect(typeof requestBody).toBe("string");
      expect(new TextEncoder().encode(requestBody as string).byteLength).toBeGreaterThan(0);
      expect(mockFetch).toHaveBeenCalledWith(
        "http://localhost:8080/db/v1/tables/products/batch",
        expect.objectContaining({
          method: "POST",
          body: JSON.stringify({
            inserts: {
              "prod:1": { title: "Notebook" },
            },
          }),
        })
      );

      mockFetch.mockRestore();
    });

    it("should reject oversized encoded batch requests", async () => {
      const mockFetch = vi.spyOn(globalThis, "fetch");

      await expect(
        client.tables.batchWithOptions(
          "products",
          {
            inserts: {
              "prod:1": { title: "Notebook" },
            },
          },
          { maxRequestBytes: 8 }
        )
      ).rejects.toThrow("marshalling batch request: encoded request exceeded 8 bytes");

      expect(mockFetch).not.toHaveBeenCalled();
      mockFetch.mockRestore();
    });

    it("should reject oversized batch success responses", async () => {
      const mockFetch = vi
        .spyOn(globalThis, "fetch")
        .mockResolvedValueOnce(new Response("x".repeat(17), { status: 201 }));

      await expect(
        client.tables.batchWithOptions(
          "products",
          { inserts: { "prod:1": { title: "Notebook" } } },
          { maxRequestBytes: 1024, maxResponseBytes: 16 }
        )
      ).rejects.toThrow("Batch operation failed response exceeded 16 bytes");

      mockFetch.mockRestore();
    });

    it("should perform bounded linear merge requests", async () => {
      const mockResponse = {
        status: "success",
        upserted: 1,
        skipped: 0,
        deleted: 0,
        next_cursor: "prod:1",
      };
      const mockFetch = vi
        .spyOn(globalThis, "fetch")
        .mockResolvedValueOnce(new Response(JSON.stringify(mockResponse), { status: 200 }));

      const result = await client.linearMergeWithOptions(
        "products",
        {
          records: {
            "prod:1": { title: "Notebook" },
          },
        },
        { maxRequestBytes: 1024, maxResponseBytes: 1024 }
      );

      expect(result).toEqual(mockResponse);
      const requestBody = mockFetch.mock.calls[0]?.[1]?.body;
      expect(typeof requestBody).toBe("string");
      expect(new TextEncoder().encode(requestBody as string).byteLength).toBeGreaterThan(0);
      expect(mockFetch).toHaveBeenCalledWith(
        "http://localhost:8080/db/v1/tables/products/merge",
        expect.objectContaining({
          method: "POST",
          body: JSON.stringify({
            records: {
              "prod:1": { title: "Notebook" },
            },
          }),
        })
      );

      mockFetch.mockRestore();
    });

    it("should reject oversized linear merge requests before sending", async () => {
      const mockFetch = vi.spyOn(globalThis, "fetch");

      await expect(
        client.linearMergeWithOptions(
          "products",
          {
            records: {
              "prod:1": { title: "x".repeat(128) },
            },
          },
          { maxRequestBytes: 64 }
        )
      ).rejects.toThrow("marshalling linear merge request: encoded request exceeded 64 bytes");

      expect(mockFetch).not.toHaveBeenCalled();
      mockFetch.mockRestore();
    });

    it("should reject oversized multi-batch requests before sending", async () => {
      const mockFetch = vi.spyOn(globalThis, "fetch");

      await expect(
        client.multiBatchWithOptions(
          {
            tables: {
              products: {
                inserts: {
                  "prod:1": { title: "x".repeat(128) },
                },
              },
            },
          },
          { maxRequestBytes: 64 }
        )
      ).rejects.toThrow("marshalling multi-batch request: encoded request exceeded 64 bytes");

      expect(mockFetch).not.toHaveBeenCalled();
      mockFetch.mockRestore();
    });

    it("should lookup a key without field projection", async () => {
      const mockDocument = {
        _key: "user:123",
        name: "John Doe",
        email: "john@example.com",
        metadata: { role: "admin" },
      };

      mockGet.mockResolvedValueOnce({
        data: mockDocument,
        error: undefined,
      });

      const result = await client.tables.lookup("users", "user:123");
      expect(result).toEqual(mockDocument);
      expect(mockGet).toHaveBeenCalledWith("/db/v1/tables/{tableName}/documents/{key}", {
        params: {
          path: { tableName: "users", key: "user:123" },
          query: undefined,
        },
      });
    });

    it("should lookup a key with field projection", async () => {
      const mockDocument = {
        _key: "user:123",
        name: "John Doe",
        email: "john@example.com",
      };

      mockGet.mockResolvedValueOnce({
        data: mockDocument,
        error: undefined,
      });

      const result = await client.tables.lookup("users", "user:123", {
        fields: "name,email",
      });
      expect(result).toEqual(mockDocument);
      expect(mockGet).toHaveBeenCalledWith("/db/v1/tables/{tableName}/documents/{key}", {
        params: {
          path: { tableName: "users", key: "user:123" },
          query: { fields: "name,email" },
        },
      });
    });

    it("should throw error when lookup fails", async () => {
      mockGet.mockResolvedValueOnce({
        data: undefined,
        error: { error: "Key not found" },
      });

      await expect(client.tables.lookup("users", "nonexistent")).rejects.toThrow(
        "Key lookup failed: Key not found"
      );
    });
  });

  describe("tables.scan", () => {
    /**
     * Helper to create a mock NDJSON ReadableStream
     */
    function createNDJSONStream(
      documents: Array<Record<string, unknown>>
    ): ReadableStream<Uint8Array> {
      const encoder = new TextEncoder();
      let docIndex = 0;

      return new ReadableStream({
        pull(controller) {
          if (docIndex < documents.length) {
            const line = `${JSON.stringify(documents[docIndex])}\n`;
            controller.enqueue(encoder.encode(line));
            docIndex++;
          } else {
            controller.close();
          }
        },
      });
    }

    /**
     * Helper to create a mock Response with NDJSON content type
     */
    function createNDJSONResponse(documents: Array<Record<string, unknown>>): Response {
      return new Response(createNDJSONStream(documents), {
        status: 200,
        headers: { "content-type": "application/x-ndjson" },
      });
    }

    it("should scan keys and stream results", async () => {
      const mockDocuments = [
        { _id: "user:1", name: "Alice" },
        { _id: "user:2", name: "Bob" },
        { _id: "user:3", name: "Charlie" },
      ];

      const mockFetch = vi
        .spyOn(globalThis, "fetch")
        .mockResolvedValueOnce(createNDJSONResponse(mockDocuments));

      const results: Array<{ _id: string; [key: string]: unknown }> = [];
      for await (const doc of client.tables.scan("users")) {
        results.push(doc);
      }

      expect(results).toHaveLength(3);
      expect(results[0]).toEqual({ _id: "user:1", name: "Alice" });
      expect(results[1]).toEqual({ _id: "user:2", name: "Bob" });
      expect(results[2]).toEqual({ _id: "user:3", name: "Charlie" });

      expect(mockFetch).toHaveBeenCalledWith(
        "http://localhost:8080/db/v1/tables/users/documents",
        expect.objectContaining({
          method: "POST",
          body: "{}",
        })
      );

      mockFetch.mockRestore();
    });

    it("should scan keys with range and field parameters", async () => {
      const mockDocuments = [
        { _id: "user:100", name: "User 100" },
        { _id: "user:101", name: "User 101" },
      ];

      const mockFetch = vi
        .spyOn(globalThis, "fetch")
        .mockResolvedValueOnce(createNDJSONResponse(mockDocuments));

      const results: Array<{ _id: string; [key: string]: unknown }> = [];
      for await (const doc of client.tables.scan("users", {
        from: "user:100",
        to: "user:200",
        fields: ["name"],
        limit: 10,
      })) {
        results.push(doc);
      }

      expect(results).toHaveLength(2);

      expect(mockFetch).toHaveBeenCalledWith(
        "http://localhost:8080/db/v1/tables/users/documents",
        expect.objectContaining({
          method: "POST",
          body: JSON.stringify({
            from: "user:100",
            to: "user:200",
            fields: ["name"],
            limit: 10,
          }),
        })
      );

      mockFetch.mockRestore();
    });

    it("should throw error when scan fails", async () => {
      const mockFetch = vi
        .spyOn(globalThis, "fetch")
        .mockResolvedValueOnce(new Response("Table not found", { status: 404 }));

      const generator = client.tables.scan("nonexistent");
      await expect(generator.next()).rejects.toThrow("Scan failed: 404 Table not found");

      mockFetch.mockRestore();
    });

    it("should collect all results with scanAll", async () => {
      const mockDocuments = [
        { _id: "prod:1", title: "Product 1", price: 10 },
        { _id: "prod:2", title: "Product 2", price: 20 },
      ];

      const mockFetch = vi
        .spyOn(globalThis, "fetch")
        .mockResolvedValueOnce(createNDJSONResponse(mockDocuments));

      const results = await client.tables.scanAll("products", {
        fields: ["title", "price"],
      });

      expect(results).toHaveLength(2);
      expect(results[0]).toEqual({ _id: "prod:1", title: "Product 1", price: 10 });
      expect(results[1]).toEqual({ _id: "prod:2", title: "Product 2", price: 20 });

      mockFetch.mockRestore();
    });
  });

  describe("indexes", () => {
    it("uses path-owned identity and returns the normalized created config", async () => {
      const created = {
        name: "thumbnail",
        type: "embeddings" as const,
        dimension: 512,
      };
      mockPost.mockResolvedValueOnce({ data: created, error: undefined });

      const result = await client.indexes.create("wikipedia", "thumbnail", {
        type: "embeddings",
        dimension: 512,
      });

      expect(result).toEqual(created);
      expect(mockPost).toHaveBeenCalledWith("/db/v1/tables/{tableName}/indexes/{indexName}", {
        params: { path: { tableName: "wikipedia", indexName: "thumbnail" } },
        body: { type: "embeddings", dimension: 512 },
      });
    });

    it("rejects an empty create response", async () => {
      mockPost.mockResolvedValueOnce({ data: undefined, error: undefined });
      await expect(
        client.indexes.create("wikipedia", "thumbnail", {
          type: "embeddings",
          dimension: 512,
        })
      ).rejects.toThrow("unexpected empty response");
    });

    it("rejects invalid index field relationships before transport", async () => {
      await expect(
        client.indexes.create("wikipedia", "thumbnail", {
          type: "embeddings",
          source_artifact_name: "thumbnail_chunks_v1",
        })
      ).rejects.toThrow("requires a non-empty embedding_name");
      expect(mockPost).not.toHaveBeenCalled();
    });

    it("preserves storage admission retry metadata", async () => {
      mockPost.mockResolvedValueOnce({
        data: undefined,
        error: {
          code: "storage_resource_exhausted",
          error: "storage_resource_exhausted",
          message: "storage capacity is temporarily exhausted",
          retryable: true,
          retry_after_ms: 1250,
        },
        response: {
          status: 429,
          headers: new Headers({ "Retry-After": "2" }),
        },
      });

      try {
        await client.indexes.create("wikipedia", "thumbnail", {
          type: "embeddings",
          dimension: 512,
        });
        expect.fail("expected storage admission failure");
      } catch (error) {
        expect(error).toBeInstanceOf(StorageResourceExhaustedError);
        const exhausted = error as InstanceType<typeof StorageResourceExhaustedError>;
        expect(exhausted.status).toBe(429);
        expect(exhausted.code).toBe("storage_resource_exhausted");
        expect(exhausted.retryable).toBe(true);
        expect(exhausted.retryAfterMs).toBe(1250);
        expect(exhausted.retryAfterSeconds).toBe(2);
      }
    });

    it("falls back to Retry-After for an invalid body delay", async () => {
      mockPost.mockResolvedValueOnce({
        data: undefined,
        error: {
          code: "storage_resource_exhausted",
          error: "storage_resource_exhausted",
          retryable: true,
          retry_after_ms: Number.POSITIVE_INFINITY,
        },
        response: {
          status: 429,
          headers: new Headers({ "Retry-After": "3" }),
        },
      });

      await expect(
        client.indexes.create("wikipedia", "thumbnail", {
          type: "embeddings",
          dimension: 512,
        })
      ).rejects.toMatchObject({ retryAfterMs: 3000, retryAfterSeconds: 3 });
    });

    it("preserves temporary index mutation retry metadata", async () => {
      mockPost.mockResolvedValueOnce({
        data: undefined,
        error: {
          error: "index_probe_unavailable",
          message: "model probe is temporarily unavailable",
          retryable: true,
        },
        response: {
          status: 503,
          headers: new Headers({ "Retry-After": "4" }),
        },
      });

      try {
        await client.indexes.create("wikipedia", "thumbnail", {
          type: "embeddings",
          dimension: 512,
        });
        expect.fail("expected temporary index mutation failure");
      } catch (error) {
        expect(error).toBeInstanceOf(IndexMutationTemporarilyUnavailableError);
        expect(error).toMatchObject({
          status: 503,
          code: "index_probe_unavailable",
          retryable: true,
          retryAfterSeconds: 4,
        });
      }
    });
  });

  describe("setAuth", () => {
    it("should update authentication credentials", () => {
      client.setAuth("newuser", "newpass");
      expect(client.getRawClient()).toBeDefined();
      // In a real implementation, you'd verify the auth header was updated
    });
  });

  describe("error handling", () => {
    it("should throw error when query fails", async () => {
      const mockError = {
        error: "Table not found",
      };

      mockPost.mockResolvedValueOnce({
        data: undefined,
        error: mockError as unknown,
      });

      const request: QueryRequest = {
        table: "nonexistent",
        limit: 10,
      };

      await expect(client.query(request)).rejects.toThrow("Query failed: Table not found");
    });

    it("preserves retry guidance when query storage is temporarily unavailable", async () => {
      mockPost.mockResolvedValueOnce({
        data: undefined,
        error: {
          code: "storage_read_temporarily_unavailable",
          message: "storage read temporarily unavailable",
          retryable: true,
        },
        response: new Response(undefined, {
          status: 503,
          headers: { "Retry-After": "3" },
        }),
      });

      const promise = client.query({ table: "products", limit: 10 });
      await expect(promise).rejects.toMatchObject({
        name: "StorageReadTemporarilyUnavailableError",
        status: 503,
        code: "storage_read_temporarily_unavailable",
        retryable: true,
        retryAfterSeconds: 3,
      });
      await promise.catch((error: unknown) => {
        expect(error).toBeInstanceOf(StorageReadTemporarilyUnavailableError);
      });
    });

    it("preserves hierarchy traversal restart guidance for stale cursors", async () => {
      mockPost.mockResolvedValueOnce({
        data: undefined,
        error: {
          status: 409,
          error: "hierarchy_cursor_stale",
          message: "the source artifact changed during traversal",
          action: "restart_hierarchy_traversal",
          restart_without: "search_after",
          retryable: false,
        },
        response: new Response(undefined, { status: 409 }),
      });

      const promise = client.query({ table: "products", limit: 10 });
      await expect(promise).rejects.toMatchObject({
        name: "HierarchyCursorStaleError",
        message: "Query failed: the source artifact changed during traversal",
        status: 409,
        code: "hierarchy_cursor_stale",
        action: "restart_hierarchy_traversal",
        restartWithout: "search_after",
        retryable: false,
      });
      await promise.catch((error: unknown) => {
        expect(error).toBeInstanceOf(HierarchyCursorStaleError);
      });
    });

    it.each([
      ["doc_identity_unavailable", "doc identity unavailable"],
      ["read_requires_primary", "read requires primary"],
      ["standby_read_unavailable", "standby read unavailable"],
      ["index_rebuilding", "required index is rebuilding"],
      ["query_embedding_temporarily_unavailable", "query embedding temporarily unavailable"],
    ] as const)("classifies retryable query availability response %s", async (code, message) => {
      mockPost.mockResolvedValueOnce({
        data: undefined,
        error: {
          code,
          message,
          retryable: true,
        },
        response: new Response(undefined, {
          status: 503,
          headers: { "Retry-After": "2" },
        }),
      });

      const promise = client.query({ table: "products", limit: 10 });
      await expect(promise).rejects.toMatchObject({
        name: "QueryTemporarilyUnavailableError",
        status: 503,
        code,
        retryable: true,
        retryAfterSeconds: 2,
      });
      await promise.catch((error: unknown) => {
        expect(error).toBeInstanceOf(QueryTemporarilyUnavailableError);
        expect(error).not.toBeInstanceOf(StorageReadTemporarilyUnavailableError);
      });
    });
  });

  describe("SSE parsing", () => {
    /**
     * Helper to create a mock ReadableStream from SSE events
     */
    function createSSEStream(
      events: Array<{ event: string; data: string }>
    ): ReadableStream<Uint8Array> {
      const encoder = new TextEncoder();
      let eventIndex = 0;

      return new ReadableStream({
        pull(controller) {
          if (eventIndex < events.length) {
            const { event, data } = events[eventIndex];
            const sseData = `event: ${event}\ndata: ${data}\n\n`;
            controller.enqueue(encoder.encode(sseData));
            eventIndex++;
          } else {
            controller.close();
          }
        },
      });
    }

    /**
     * Helper to create a mock Response with SSE content type
     */
    function createSSEResponse(events: Array<{ event: string; data: string }>): Response {
      return new Response(createSSEStream(events), {
        status: 200,
        headers: { "content-type": "text/event-stream" },
      });
    }

    describe("Retrieval Agent SSE parsing", () => {
      it("should JSON-parse reasoning events to preserve newlines", async () => {
        const reasoningWithNewlines =
          "Step 1: First thing\nStep 2: Second thing\nStep 3: Third thing";
        const events = [
          { event: "reasoning", data: JSON.stringify(reasoningWithNewlines) },
          { event: "done", data: JSON.stringify({ success: true }) },
        ];

        const mockFetch = vi
          .spyOn(globalThis, "fetch")
          .mockResolvedValueOnce(createSSEResponse(events));

        const receivedReasoning: string[] = [];
        let doneReceived = false;
        await client.retrievalAgent(
          { table: "test", query: "test query" },
          {
            onReasoning: (text) => receivedReasoning.push(text),
            onDone: () => {
              doneReceived = true;
            },
          }
        );

        // Wait for stream to complete
        await new Promise((resolve) => setTimeout(resolve, 50));

        expect(receivedReasoning).toHaveLength(1);
        expect(receivedReasoning[0]).toBe(reasoningWithNewlines);
        expect(receivedReasoning[0]).toContain("\n");
        expect(doneReceived).toBe(true);

        mockFetch.mockRestore();
      });

      it("should JSON-parse answer events to preserve newlines", async () => {
        const answerWithNewlines = "Here is the answer:\n\n1. First point\n2. Second point";
        const events = [
          { event: "generation", data: JSON.stringify(answerWithNewlines) },
          { event: "done", data: JSON.stringify({ success: true }) },
        ];

        const mockFetch = vi
          .spyOn(globalThis, "fetch")
          .mockResolvedValueOnce(createSSEResponse(events));

        const receivedAnswers: string[] = [];
        let doneReceived = false;
        await client.retrievalAgent(
          { table: "test", query: "test query" },
          {
            onGeneration: (text) => receivedAnswers.push(text),
            onDone: () => {
              doneReceived = true;
            },
          }
        );

        await new Promise((resolve) => setTimeout(resolve, 50));

        expect(receivedAnswers).toHaveLength(1);
        expect(receivedAnswers[0]).toBe(answerWithNewlines);
        expect(receivedAnswers[0]).toContain("\n");
        expect(doneReceived).toBe(true);

        mockFetch.mockRestore();
      });

      it("should JSON-parse followup events to preserve newlines", async () => {
        const followupWithNewlines = "Would you like to know more about:\n- Option A\n- Option B";
        const events = [
          { event: "followup", data: JSON.stringify(followupWithNewlines) },
          { event: "done", data: JSON.stringify({ success: true }) },
        ];

        const mockFetch = vi
          .spyOn(globalThis, "fetch")
          .mockResolvedValueOnce(createSSEResponse(events));

        const receivedFollowups: string[] = [];
        let doneReceived = false;
        await client.retrievalAgent(
          { table: "test", query: "test query" },
          {
            onFollowup: (text) => receivedFollowups.push(text),
            onDone: () => {
              doneReceived = true;
            },
          }
        );

        await new Promise((resolve) => setTimeout(resolve, 50));

        expect(receivedFollowups).toHaveLength(1);
        expect(receivedFollowups[0]).toBe(followupWithNewlines);
        expect(receivedFollowups[0]).toContain("\n");
        expect(doneReceived).toBe(true);

        mockFetch.mockRestore();
      });

      it("should expose tool mode and confidence from the authoritative done payload", async () => {
        const events = [
          {
            event: "tool_mode",
            data: JSON.stringify({ mode: "native", tools_count: 3 }),
          },
          {
            event: "done",
            data: JSON.stringify({ generation_confidence: 0.8, context_relevance: 0.9 }),
          },
        ];
        const mockFetch = vi
          .spyOn(globalThis, "fetch")
          .mockResolvedValueOnce(createSSEResponse(events));
        const toolModes: Array<{ mode: string; tools_count?: number }> = [];
        const confidence: Array<{
          generation_confidence: number;
          context_relevance: number;
        }> = [];

        await client.retrievalAgent(
          { table: "test", query: "test query" },
          {
            onToolMode: (data) => toolModes.push(data),
            onConfidence: (data) => confidence.push(data),
          }
        );
        await new Promise((resolve) => setTimeout(resolve, 50));

        expect(toolModes).toEqual([{ mode: "native", tools_count: 3 }]);
        expect(confidence).toEqual([{ generation_confidence: 0.8, context_relevance: 0.9 }]);
        mockFetch.mockRestore();
      });
    });

    describe("Retrieval Agent SSE parsing (multi-paragraph)", () => {
      it("should JSON-parse answer events to preserve newlines", async () => {
        const answerWithNewlines = "The response is:\n\nParagraph one.\n\nParagraph two.";
        const events = [
          { event: "generation", data: JSON.stringify(answerWithNewlines) },
          { event: "done", data: JSON.stringify({ success: true }) },
        ];

        const mockFetch = vi
          .spyOn(globalThis, "fetch")
          .mockResolvedValueOnce(createSSEResponse(events));

        const receivedAnswers: string[] = [];
        let doneReceived = false;
        await client.retrievalAgent(
          { table: "test", query: "test query" },
          {
            onGeneration: (text) => receivedAnswers.push(text),
            onDone: () => {
              doneReceived = true;
            },
          }
        );

        await new Promise((resolve) => setTimeout(resolve, 50));

        expect(receivedAnswers).toHaveLength(1);
        expect(receivedAnswers[0]).toBe(answerWithNewlines);
        expect(receivedAnswers[0]).toContain("\n");
        expect(doneReceived).toBe(true);

        mockFetch.mockRestore();
      });
    });
  });
});
