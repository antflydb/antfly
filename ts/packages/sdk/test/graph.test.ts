// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

import http from "node:http";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AntflyClient, GraphExecutionError } from "../src/client.js";

const result = {
  wire_version: 1,
  columns: ["n"],
  rows: [[{ type: "integer", value: "9223372036854775807" }]],
  plan: [],
  stats: {},
  graph_incarnation: "4",
  graph_revision: "1",
  complete: true,
};
afterEach(() => vi.unstubAllGlobals());
describe("graph query transport", () => {
  it("rejects graph redirects before replaying a mutation", async () => {
    let deliveries = 0;
    const server = http.createServer((_request, response) => {
      deliveries++;
      response.writeHead(307, { Location: "/replayed" });
      response.end();
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    try {
      const address = server.address();
      if (!address || typeof address === "string") throw new Error("missing local address");
      const client = new AntflyClient({ baseUrl: `http://127.0.0.1:${address.port}` });
      for (const operation of [
        () => client.graphs.execute("g", { query: "CREATE(n:D)" }),
        () => client.graphs.create("g", { labels: [] }),
        () => client.graphs.update("g", "1", { labels: [] }),
        () => client.graphs.drop("g"),
        () =>
          client.api.POST("/db/v1/graphs/{graphName}/query", {
            params: { path: { graphName: "g" } },
            body: { query: "CREATE(n:D)" },
          }),
      ]) {
        const before = deliveries;
        await expect(operation()).rejects.toThrow();
        expect(deliveries - before).toBe(1);
      }
    } finally {
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });
  it("conditionally updates a graph without retrying a stale revision", async () => {
    const fetch = vi.fn(
      async (_request: Request) =>
        new Response(JSON.stringify({ error: "CatalogGenerationChanged" }), {
          status: 409,
          headers: { "Content-Type": "application/json" },
        })
    );
    vi.stubGlobal("fetch", fetch);
    const client = new AntflyClient({ baseUrl: "https://antfly.test" });
    const definition = { labels: [{ name: "Document", table: "docs" }], relationships: [] };
    await expect(
      client.graphs.update("knowledge", "9223372036854775807", definition)
    ).rejects.toThrow("reload the catalog");
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(fetch.mock.calls[0][0].method).toBe("PUT");
    expect(await fetch.mock.calls[0][0].json()).toEqual({
      expected_revision: "9223372036854775807",
      definition,
    });
  });
  it("loads mapped schemas through the explicitly scoped catalog route", async () => {
    const fetch = vi.fn(
      async (_request: Request) =>
        new Response(JSON.stringify({ name: "docs", indexes: {}, shards: {}, schema: {} }), {
          headers: { "Content-Type": "application/json" },
        })
    );
    vi.stubGlobal("fetch", fetch);
    const client = new AntflyClient({ baseUrl: "https://antfly.test" });
    await client.tables.get("docs", { database: "sales", namespace: "reports" });
    expect(fetch.mock.calls[0][0].url).toBe(
      "https://antfly.test/db/v1/databases/sales/namespaces/reports/tables/docs"
    );
  });
  it("discovers graph indexes in an explicitly scoped mapped table", async () => {
    const fetch = vi.fn(
      async (_request: Request) =>
        new Response("[]", { headers: { "Content-Type": "application/json" } })
    );
    vi.stubGlobal("fetch", fetch);
    const client = new AntflyClient({ baseUrl: "https://antfly.test" });
    await client.indexes.list("docs", { database: "sales", namespace: "reports" });
    expect(fetch.mock.calls[0][0].url).toBe(
      "https://antfly.test/db/v1/databases/sales/namespaces/reports/tables/docs/indexes"
    );
  });
  it("encodes bigint parameters and forwards cancellation without rounding results", async () => {
    const fetch = vi.fn(
      async (_request: Request) =>
        new Response(JSON.stringify(result), { headers: { "Content-Type": "application/json" } })
    );
    vi.stubGlobal("fetch", fetch);
    const client = new AntflyClient({ baseUrl: "https://antfly.test" });
    const controller = new AbortController();
    const response = await client.graphs.execute(
      "knowledge",
      { query: "RETURN $n AS n", parameters: { n: (1n << 63n) - 1n } },
      { signal: controller.signal }
    );
    expect(response.rows[0][0]).toEqual({ type: "integer", value: "9223372036854775807" });
    const request = fetch.mock.calls[0][0];
    expect(request.url).toBe("https://antfly.test/db/v1/graphs/knowledge/query");
    expect(await request.json()).toEqual({
      query: "RETURN $n AS n",
      typed_parameters: {
        type: "map",
        value: { n: { type: "integer", value: "9223372036854775807" } },
      },
    });
    controller.abort();
    expect(request.signal.aborted).toBe(true);
  });
  it("retains structured UTF-8 diagnostics", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response(
            JSON.stringify({
              code: "UndefinedGraphVariable",
              message: "Unknown variable nope",
              span: { start: 15, end: 19 },
            }),
            { status: 400, headers: { "Content-Type": "application/json" } }
          )
      )
    );
    const client = new AntflyClient({ baseUrl: "https://antfly.test" });
    const error = await client.graphs
      .execute("knowledge", { query: "RETURN '😀', nope" })
      .catch((failure: unknown) => failure);
    expect(error).toBeInstanceOf(GraphExecutionError);
    expect(error).toMatchObject({
      status: 400,
      diagnostic: {
        code: "UndefinedGraphVariable",
        message: "Unknown variable nope",
        span: { start: 15, end: 19 },
      },
    });
  });
  it("preserves server-confirmed write outcomes on failed result delivery", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response(
            JSON.stringify({
              code: "OutOfMemory",
              message: "Result encoding failed",
              write_outcome: "committed",
            }),
            { status: 429, headers: { "Content-Type": "application/json" } }
          )
      )
    );
    const client = new AntflyClient({ baseUrl: "https://antfly.test" });
    const error = await client.graphs
      .execute("knowledge", { query: "CREATE (n:Document) RETURN n" })
      .catch((failure: unknown) => failure);
    expect(error).toMatchObject({ status: 429, diagnostic: { write_outcome: "committed" } });
  });

  it("rejects mutually exclusive parameter contracts before sending a request", async () => {
    const fetch = vi.fn();
    vi.stubGlobal("fetch", fetch);
    const client = new AntflyClient({ baseUrl: "https://antfly.test" });
    await expect(
      client.graphs.execute("knowledge", {
        query: "RETURN 1",
        parameters: {},
        typed_parameters: { type: "map", value: {} },
      })
    ).rejects.toThrow("Choose parameters");
    expect(fetch).not.toHaveBeenCalled();
  });
  it("carries a client write ID and inspects the durable receipt without replay", async () => {
    const id = "0123456789abcdef0123456789abcdef";
    const fetch = vi.fn(
      async (request: Request) =>
        new Response(
          JSON.stringify(
            request.method === "POST"
              ? { ...result, complete: false, commit: { transaction_id: id, status: "unknown" } }
              : { transaction_id: id, disposition: "committed" }
          ),
          { headers: { "Content-Type": "application/json" } }
        )
    );
    vi.stubGlobal("fetch", fetch);
    const client = new AntflyClient({ baseUrl: "https://antfly.test" });
    const response = await client.graphs.execute("knowledge", {
      query: "CREATE (n:Document) RETURN n",
      write_request_id: id,
    });
    expect(response.commit?.status).toBe("unknown");
    expect((await fetch.mock.calls[0][0].json()).write_request_id).toBe(id);
    const status = await client.graphs.writeStatus(id);
    expect(status.disposition).toBe("committed");
    expect(fetch.mock.calls[1][0].url).toBe(`https://antfly.test/db/v1/transactions/${id}/status`);
    expect(fetch).toHaveBeenCalledTimes(2);
    await expect(client.graphs.writeStatus("bad")).rejects.toThrow(
      "Invalid graph write transaction ID"
    );
    expect(fetch).toHaveBeenCalledTimes(2);
  });
});

describe("graph catalog mutation visibility", () => {
  it.each([
    "create",
    "update",
    "drop",
  ])("preserves pending %s without replay", async (operation) => {
    const fetch = vi.fn(
      async () =>
        new Response(JSON.stringify({ status: "committed_visibility_pending" }), {
          status: 202,
          headers: { "Content-Type": "application/json" },
        })
    );
    vi.stubGlobal("fetch", fetch);
    const client = new AntflyClient({ baseUrl: "https://antfly.test" });
    const definition = { labels: [{ name: "Document", table: "docs" }] };
    const outcome =
      operation === "create"
        ? await client.graphs.create("knowledge", definition)
        : operation === "update"
          ? await client.graphs.update("knowledge", "1", definition)
          : await client.graphs.drop("knowledge");
    expect(outcome).toEqual({ status: "committed_visibility_pending" });
    expect(fetch).toHaveBeenCalledTimes(1);
  });
  it("returns no pending outcome when graph deletion is visible", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response(null, { status: 204 }))
    );
    const client = new AntflyClient({ baseUrl: "https://antfly.test" });
    expect(await client.graphs.drop("knowledge")).toBeUndefined();
  });
});
