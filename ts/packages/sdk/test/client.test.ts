/**
 * Unit tests for the Antfly SDK client using Vitest
 */
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { CreateTableRequest, QueryRequest, TableStatus } from "../src/types.js";

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
const { AntflyClient } = await import("../src/client.js");

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
  });

  describe("query", () => {
    it("should execute global query", async () => {
      const mockResponse = {
        responses: [
          {
            hits: {
              total: 1,
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

      const request: QueryRequest = {
        table: "test",
        limit: 10,
      };

      const result = await client.query(request);
      expect(result).toEqual(mockResponse.responses[0]);
      expect(mockPost).toHaveBeenCalledWith("/query", {
        body: request,
      });
    });

    it("should handle query with Bleve full_text_search", async () => {
      const mockResponse = {
        responses: [
          {
            hits: {
              total: 2,
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

      const request: QueryRequest = {
        table: "products",
        full_text_search: {
          match: "laptop",
          field: "name",
        },
        limit: 10,
      };

      const result = await client.query(request);
      expect(result?.hits?.total).toBe(2);
      expect(mockPost).toHaveBeenCalledWith("/query", {
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
      expect(mockGet).toHaveBeenCalledWith("/tables", {
        params: undefined,
      });
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
      expect(mockPost).toHaveBeenCalledWith("/tables/{tableName}", {
        params: { path: { tableName: "new_table" } },
        body: config,
      });
    });

    it("should query a specific table", async () => {
      const mockResponse = {
        responses: [
          {
            hits: {
              total: 1,
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

      const request: QueryRequest = {
        full_text_search: {
          query: "laptop",
        },
        limit: 10,
      };

      const result = await client.tables.query("products", request);
      expect(result).toEqual(mockResponse);
      expect(mockPost).toHaveBeenCalledWith("/tables/{tableName}/query", {
        params: { path: { tableName: "products" } },
        body: request,
      });
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
      expect(mockGet).toHaveBeenCalledWith("/tables/{tableName}/lookup/{key}", {
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
      expect(mockGet).toHaveBeenCalledWith("/tables/{tableName}/lookup/{key}", {
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
        { _key: "user:1", name: "Alice" },
        { _key: "user:2", name: "Bob" },
        { _key: "user:3", name: "Charlie" },
      ];

      const mockFetch = vi
        .spyOn(globalThis, "fetch")
        .mockResolvedValueOnce(createNDJSONResponse(mockDocuments));

      const results: Array<{ _key: string; [key: string]: unknown }> = [];
      for await (const doc of client.tables.scan("users")) {
        results.push(doc);
      }

      expect(results).toHaveLength(3);
      expect(results[0]).toEqual({ _key: "user:1", name: "Alice" });
      expect(results[1]).toEqual({ _key: "user:2", name: "Bob" });
      expect(results[2]).toEqual({ _key: "user:3", name: "Charlie" });

      expect(mockFetch).toHaveBeenCalledWith(
        "http://localhost:8080/tables/users/lookup",
        expect.objectContaining({
          method: "POST",
          body: "{}",
        })
      );

      mockFetch.mockRestore();
    });

    it("should scan keys with range and field parameters", async () => {
      const mockDocuments = [
        { _key: "user:100", name: "User 100" },
        { _key: "user:101", name: "User 101" },
      ];

      const mockFetch = vi
        .spyOn(globalThis, "fetch")
        .mockResolvedValueOnce(createNDJSONResponse(mockDocuments));

      const results: Array<{ _key: string; [key: string]: unknown }> = [];
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
        "http://localhost:8080/tables/users/lookup",
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
        { _key: "prod:1", title: "Product 1", price: 10 },
        { _key: "prod:2", title: "Product 2", price: 20 },
      ];

      const mockFetch = vi
        .spyOn(globalThis, "fetch")
        .mockResolvedValueOnce(createNDJSONResponse(mockDocuments));

      const results = await client.tables.scanAll("products", {
        fields: ["title", "price"],
      });

      expect(results).toHaveLength(2);
      expect(results[0]).toEqual({ _key: "prod:1", title: "Product 1", price: 10 });
      expect(results[1]).toEqual({ _key: "prod:2", title: "Product 2", price: 20 });

      mockFetch.mockRestore();
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
