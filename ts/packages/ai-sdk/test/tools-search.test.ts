import { AntflyClient } from "@antfly/sdk";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createSearchTool } from "../src/tools/search.js";

afterEach(() => vi.unstubAllGlobals());

function toolOptions() {
  return { toolCallId: "t1", messages: [] } as never;
}

// biome-ignore lint/suspicious/noExplicitAny: parsed JSON test fixtures are intentionally untyped.
async function bodyOf(request: Request | undefined): Promise<any> {
  if (!request) throw new Error("Missing captured request");
  return JSON.parse(await request.text());
}

describe("createSearchTool", () => {
  it("scopes the query to the fixed table and adds semantic_search only when indexes are configured", async () => {
    let captured: Request | undefined;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (request: Request) => {
        captured = request.clone();
        return new Response('{"hits":{"hits":[]}}', {
          headers: { "Content-Type": "application/json" },
        });
      })
    );
    const client = new AntflyClient({ baseUrl: "http://search.test" });
    const tools = createSearchTool({ client, table: "docs", indexes: ["body_embedding"] });
    await tools.antfly_search.execute?.({ query: "laptop" }, toolOptions());
    expect(captured?.url).toContain("/db/v1/tables/docs/query");
    const body = await bodyOf(captured);
    expect(body.full_text_search).toEqual({ query: "laptop" });
    expect(body.semantic_search).toBe("laptop");
    expect(body.indexes).toEqual(["body_embedding"]);
  });

  it("omits semantic_search when no index is configured", async () => {
    let captured: Request | undefined;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (request: Request) => {
        captured = request.clone();
        return new Response('{"hits":{"hits":[]}}', {
          headers: { "Content-Type": "application/json" },
        });
      })
    );
    const client = new AntflyClient({ baseUrl: "http://search.test" });
    const tools = createSearchTool({ client, table: "docs" });
    await tools.antfly_search.execute?.({ query: "laptop" }, toolOptions());
    const body = await bodyOf(captured);
    expect(body.semantic_search).toBeUndefined();
    expect(body.indexes).toBeUndefined();
  });

  it("treats maxResults as a ceiling on the model's limit", async () => {
    const bodies: { limit?: number }[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const request =
          input instanceof Request && init === undefined ? input : new Request(input, init);
        bodies.push(await request.json());
        return new Response(JSON.stringify({ responses: [{ status: 200, hits: { hits: [] } }] }), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      })
    );
    const { antfly_search } = createSearchTool({
      client: new AntflyClient({ baseUrl: "http://x" }),
      table: "docs",
      maxResults: 5,
    });
    const options = { toolCallId: "t1", messages: [] } as never;
    await antfly_search.execute?.({ query: "q", limit: 100 }, options);
    await antfly_search.execute?.({ query: "q", limit: 2 }, options);
    expect(bodies.map((body) => body.limit)).toEqual([5, 2]);
  });
});
