import { AntflyClient } from "@antfly/sdk";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createGraphTool } from "../src/tools/graph.js";

afterEach(() => vi.unstubAllGlobals());

function toolOptions() {
  return { toolCallId: "t1", messages: [] } as never;
}

// biome-ignore lint/suspicious/noExplicitAny: parsed JSON test fixtures are intentionally untyped.
async function bodyOf(request: Request | undefined): Promise<any> {
  if (!request) throw new Error("Missing captured request");
  return JSON.parse(await request.text());
}

describe("createGraphTool", () => {
  it("forces the fixed table and index, and clamps traverse bounds", async () => {
    let captured: Request | undefined;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (request: Request) => {
        captured = request.clone();
        return new Response(
          JSON.stringify({
            responses: [
              {
                graph_results: {
                  neighbors: {
                    kind: "nodes",
                    nodes: [],
                    stats: { returned_items: 0, truncated: false },
                  },
                },
              },
            ],
          }),
          { headers: { "Content-Type": "application/json" } }
        );
      })
    );
    const client = new AntflyClient({ baseUrl: "http://graph.test" });
    const tools = createGraphTool({ client, table: "kg", index: "g", maxResults: 10, maxDepth: 2 });
    const result = await tools.antfly_graph_query.execute?.(
      {
        operations: {
          neighbors: { traverse: { start: { keys: ["n1"] }, max_depth: 999, limit: 999 } },
        },
      },
      toolOptions()
    );
    expect(captured?.url).toContain("/db/v1/tables/kg/query");
    const body = await bodyOf(captured);
    expect(body.graph_queries.neighbors.index).toBe("g");
    expect(body.graph_queries.neighbors.traverse.max_depth).toBe(2);
    expect(body.graph_queries.neighbors.traverse.limit).toBe(10);
    expect(result).toEqual({
      neighbors: { kind: "nodes", nodes: [], stats: { returned_items: 0, truncated: false } },
    });
  });

  it("rejects an index override, since it is fixed in code", async () => {
    const client = new AntflyClient({ baseUrl: "http://graph.test" });
    const tools = createGraphTool({ client, table: "kg", index: "g" });
    await expect(
      tools.antfly_graph_query.execute?.(
        { operations: { bad: { traverse: { start: { keys: ["n1"] } }, index: "other" } } },
        toolOptions()
      )
    ).rejects.toThrow(/fixed index/);
  });

  it("rejects an operation body with no recognized kind", async () => {
    const client = new AntflyClient({ baseUrl: "http://graph.test" });
    const tools = createGraphTool({ client, table: "kg" });
    await expect(
      tools.antfly_graph_query.execute?.({ operations: { bad: {} } }, toolOptions())
    ).rejects.toThrow(/exactly one of/);
  });
});

it.each([
  [
    { bindings: ["n"], limit: 9999 },
    { bindings: ["n"], limit: 5 },
  ],
  [{ bindings: ["n"] }, { bindings: ["n"], limit: 5 }],
  [{ aggregates: { count: { count: "*" } } }, { aggregates: { count: { count: "*" } } }],
])("keeps MATCH return at its canonical level and bounds bindings", async (projection, expected) => {
  const query = vi.fn().mockResolvedValue({ responses: [{ graph_results: {} }] });
  const tools = createGraphTool({
    client: { tables: { query } } as never,
    table: "docs",
    maxResults: 5,
  });
  const match = { anchor: "n", nodes: { n: {} }, edges: [] };
  await tools.antfly_graph_query.execute?.(
    { operations: { q: { match, return: projection } } },
    toolOptions()
  );
  expect(query.mock.calls[0][1].graph_queries.q).toEqual({
    index: "graph",
    match,
    return: expected,
  });
  expect(match).not.toHaveProperty("return");
});

it("preserves valid graph names that coincide with Object prototype properties", async () => {
  const query = vi.fn().mockResolvedValue({ responses: [{ graph_results: {} }] });
  const tools = createGraphTool({ client: { tables: { query } } as never, table: "docs" });
  const operations = JSON.parse(
    '{"__proto__":{"traverse":{"start":{"keys":["a"]}}},"constructor":{"traverse":{"start":{"keys":["b"]}}}}'
  );
  await tools.antfly_graph_query.execute?.({ operations }, toolOptions());
  const sent = JSON.parse(JSON.stringify(query.mock.calls[0][1]));
  expect(Object.keys(sent.graph_queries)).toEqual(["__proto__", "constructor"]);
  const operation = Object.getOwnPropertyDescriptor(sent.graph_queries, "__proto__")?.value;
  expect(operation.index).toBe("graph");
  expect(operation.traverse.max_depth).toBe(6);
  expect(Object.getPrototypeOf(operations)).toBe(Object.prototype);
});
