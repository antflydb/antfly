import { AntflyClient } from "@antfly/sdk";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createSqlTool } from "../src/tools/sql.js";

afterEach(() => vi.unstubAllGlobals());

function toolOptions() {
  return { toolCallId: "t1", messages: [] } as never;
}

/** `AntflyClient.executeSQL` calls plain `fetch(url, init)` (two arguments),
 * not a single `Request` object; normalize so the mock can inspect it. */
function asRequest(input: RequestInfo | URL, init?: RequestInit): Request {
  return input instanceof Request && init === undefined ? input : new Request(input, init);
}

// biome-ignore lint/suspicious/noExplicitAny: parsed JSON test fixtures are intentionally untyped.
async function bodyOf(request: Request | undefined): Promise<any> {
  if (!request) throw new Error("Missing captured request");
  return JSON.parse(await request.text());
}

describe("createSqlTool", () => {
  it("executes a SELECT statement against the fixed database/namespace", async () => {
    let captured: Request | undefined;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        captured = asRequest(input, init);
        return new Response(
          JSON.stringify({ columns: [], rows: [], rows_affected: 0, command_tag: "SELECT 0" }),
          { headers: { "Content-Type": "application/json" } }
        );
      })
    );
    const client = new AntflyClient({ baseUrl: "http://sql.test" });
    const tools = createSqlTool({ client, database: "analytics", namespace: "public" });
    await tools.antfly_sql.execute?.({ statement: "SELECT 1" }, toolOptions());
    expect(captured?.url).toContain("/db/v1/sql");
    const body = await bodyOf(captured);
    expect(body.statement).toBe("SELECT 1");
    expect(body.database).toBe("analytics");
    expect(body.namespace).toBe("public");
  });

  it("rejects a write statement by default without calling the server", async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    const client = new AntflyClient({ baseUrl: "http://sql.test" });
    const tools = createSqlTool({ client });
    await expect(
      tools.antfly_sql.execute?.({ statement: "DELETE FROM users" }, toolOptions())
    ).rejects.toThrow(/read-only/);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("allows a write statement once explicitly opted into in code", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response(
            JSON.stringify({ columns: [], rows: [], rows_affected: 1, command_tag: "DELETE 1" }),
            { headers: { "Content-Type": "application/json" } }
          )
      )
    );
    const client = new AntflyClient({ baseUrl: "http://sql.test" });
    const tools = createSqlTool({ client, readOnly: false });
    const result = await tools.antfly_sql.execute?.(
      { statement: "DELETE FROM users WHERE id = $1", parameters: ["1"] },
      toolOptions()
    );
    expect((result as { rows_affected: number }).rows_affected).toBe(1);
  });

  it("treats a leading SQL comment as not defeating the read-only check", async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    const client = new AntflyClient({ baseUrl: "http://sql.test" });
    const tools = createSqlTool({ client });
    await expect(
      tools.antfly_sql.execute?.(
        { statement: "/* comment */ -- another\nDELETE FROM users" },
        toolOptions()
      )
    ).rejects.toThrow(/read-only/);
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
