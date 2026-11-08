import { AntflyClient } from "@antfly/sdk";
import { afterEach, expect, it, vi } from "vitest";
import { createGraphTool } from "../src/tools/graph.js";
import { createMemoryTools } from "../src/tools/memory.js";
import { createSearchTool } from "../src/tools/search.js";
import { createSqlTool } from "../src/tools/sql.js";

afterEach(() => vi.unstubAllGlobals());
const client = () => new AntflyClient({ baseUrl: "http://tools.test" });
const cases = [
  {
    name: "search",
    tool: () => createSearchTool({ client: client(), table: "docs" }).antfly_search,
    input: { query: "hi" },
  },
  {
    name: "graph",
    tool: () => createGraphTool({ client: client(), table: "docs" }).antfly_graph_query,
    input: { operations: { neighbors: { traverse: { start: { keys: ["n"] } } } } },
  },
  {
    name: "sql",
    tool: () => createSqlTool({ client: client() }).antfly_sql,
    input: { statement: "SELECT 1" },
  },
  {
    name: "recall",
    tool: () => createMemoryTools({ client: client(), space: "s" }).antfly_memory_recall,
    input: { query: "hi" },
  },
  {
    name: "remember",
    tool: () => createMemoryTools({ client: client(), space: "s" }).antfly_memory_remember,
    input: { subject: "s", predicate: "p", value: "v", statement: "s p v" },
  },
  {
    name: "forget",
    tool: () => createMemoryTools({ client: client(), space: "s" }).antfly_memory_forget,
    input: { evidenceIds: ["e"] },
  },
];
it.each(cases)("$name does not dispatch when already cancelled", async ({ tool, input }) => {
  const fetch = vi.fn();
  vi.stubGlobal("fetch", fetch);
  const abortSignal = AbortSignal.abort(new Error("cancelled"));
  await expect(
    Promise.resolve(
      tool().execute?.(input, { toolCallId: "t", messages: [], context: undefined, abortSignal })
    )
  ).rejects.toThrow("cancelled");
  expect(fetch).not.toHaveBeenCalled();
});
it.each(cases)("$name aborts its in-flight transport", async ({ tool, input }) => {
  let captured: AbortSignal | undefined;
  vi.stubGlobal(
    "fetch",
    vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      const request = input instanceof Request ? input : new Request(input, init);
      captured = request.signal;
      return new Promise<Response>((_resolve, reject) => {
        if (request.signal.aborted) reject(request.signal.reason);
        else
          request.signal.addEventListener("abort", () => reject(request.signal.reason), {
            once: true,
          });
      });
    })
  );
  const caller = new AbortController();
  const pending = Promise.resolve(
    tool().execute?.(input, {
      toolCallId: "t",
      messages: [],
      context: undefined,
      abortSignal: caller.signal,
    })
  );
  const rejected = expect(pending).rejects.toBeDefined();
  await vi.waitFor(() => expect(captured).toBeDefined());
  caller.abort(new Error("cancelled"));
  await rejected;
  expect(captured?.aborted).toBe(true);
});
