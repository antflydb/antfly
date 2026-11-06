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
