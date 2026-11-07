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
  it.each([
    "WITH gone AS (DELETE FROM t RETURNING id) SELECT * FROM gone",
    "SELECT 1; DROP TABLE t",
    "SELECT * INTO copy_of_t FROM t",
    "EXPLAIN ANALYZE DELETE FROM t",
    "/* select */ DELETE FROM t",
    "SELECT 1 /* ; */; INSERT INTO t VALUES (1)",
  ])("refuses a write dressed as a read: %s", async (statement) => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    const { antfly_sql } = createSqlTool({ client: new AntflyClient({ baseUrl: "http://x" }) });
    await expect(antfly_sql.execute?.({ statement }, toolOptions())).rejects.toThrow("read-only");
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it.each([
    "SELECT 'DROP TABLE t; DELETE' AS note FROM t;",
    'SELECT "update" FROM t -- delete',
    "WITH a AS (SELECT 1) SELECT * FROM a",
  ])("allows a read that only mentions a keyword in text: %s", async (statement) => {
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response(JSON.stringify({ columns: [], rows: [] }), {
            status: 200,
            headers: { "content-type": "application/json" },
          })
      )
    );
    const { antfly_sql } = createSqlTool({ client: new AntflyClient({ baseUrl: "http://x" }) });
    await expect(antfly_sql.execute?.({ statement }, toolOptions())).resolves.toBeDefined();
  });

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

it.each([
  `select 1 ${"/*a".repeat(33330)}`,
  `select '${"a".repeat(90000)}`,
  `select $tag9$${"a".repeat(90000)}`,
  "select 1 /* outer /* inner */",
  `select $${"a".repeat(1000)}$${`$${"a".repeat(999)}b`.repeat(90)}`,
])("rejects malformed bounded SQL without executing it", async (statement) => {
  const executeSQL = vi.fn();
  const tool = createSqlTool({ client: { executeSQL } as never }).antfly_sql;
  await expect(tool.execute?.({ statement }, toolOptions())).rejects.toThrow("read-only");
  expect(executeSQL).not.toHaveBeenCalled();
});
it.each([
  "select $$delete; drop$$;",
  "select $tag9$delete; drop$tag9$;",
  "select 1 /* outer /* delete */ inner */; -- tail",
  "select $1, 'it''s delete', \"a\"\"drop\";",
])("allows properly delimited SQL text", async (statement) => {
  const executeSQL = vi.fn(async () => ({}));
  const tool = createSqlTool({ client: { executeSQL } as never }).antfly_sql;
  await tool.execute?.({ statement }, toolOptions());
  expect(executeSQL).toHaveBeenCalledOnce();
});
