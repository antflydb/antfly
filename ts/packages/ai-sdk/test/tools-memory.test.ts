import { AntflyClient } from "@antfly/sdk";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createMemoryTools } from "../src/tools/memory.js";

afterEach(() => vi.unstubAllGlobals());

function toolOptions() {
  return { toolCallId: "t1", messages: [] } as never;
}

// biome-ignore lint/suspicious/noExplicitAny: parsed JSON test fixtures are intentionally untyped.
async function bodyOf(request: Request | undefined): Promise<any> {
  if (!request) throw new Error("Missing captured request");
  return JSON.parse(await request.text());
}

describe("createMemoryTools", () => {
  it("scopes recall to the configured memory space without exposing it as an argument", async () => {
    let captured: Request | undefined;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (request: Request) => {
        captured = request.clone();
        return new Response('{"operations":[]}', {
          headers: { "Content-Type": "application/json" },
        });
      })
    );
    const client = new AntflyClient({ baseUrl: "http://memory.test" });
    const tools = createMemoryTools({ client, space: "assistant", database: "projects" });
    expect(Object.keys(tools)).toEqual([
      "antfly_memory_recall",
      "antfly_memory_remember",
      "antfly_memory_forget",
    ]);
    const recall = tools.antfly_memory_recall;
    await recall.execute?.({ query: "what did we decide" }, toolOptions());
    expect(captured?.url).toContain("/memory/v1/spaces/assistant/recall");
    expect(captured?.headers.get("X-Antfly-Database")).toBe("projects");
    const body = await bodyOf(captured);
    expect(body.query).toBe("what did we decide");
    expect(body.space).toBeUndefined();
  });

  it("remembers a fact as linked evidence and an assertion, with an idempotency key", async () => {
    let captured: Request | undefined;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (request: Request) => {
        captured = request.clone();
        return new Response('{"id":"op1"}', { headers: { "Content-Type": "application/json" } });
      })
    );
    const client = new AntflyClient({ baseUrl: "http://memory.test" });
    const tools = createMemoryTools({ client, space: "assistant" });
    await tools.antfly_memory_remember.execute?.(
      {
        subject: "Mira Chen",
        predicate: "owns",
        value: "operations",
        statement: "Mira Chen owns operations.",
      },
      toolOptions()
    );
    expect(captured?.url).toContain("/memory/v1/spaces/assistant/observations");
    expect(captured?.headers.get("Idempotency-Key")).toBeTruthy();
    const body = await bodyOf(captured);
    expect(body.evidence).toHaveLength(1);
    expect(body.evidence[0].text).toBe("Mira Chen owns operations.");
    expect(body.assertions).toHaveLength(1);
    expect(body.assertions[0]).toMatchObject({
      subject: "Mira Chen",
      predicate: "owns",
      value: "operations",
      statement: "Mira Chen owns operations.",
    });
    expect(body.assertions[0].evidence[0].id).toBe(body.evidence[0].id);
  });

  it("marks forget as needing approval, since it is destructive", () => {
    const client = new AntflyClient({ baseUrl: "http://memory.test" });
    const tools = createMemoryTools({ client, space: "assistant" });
    expect(tools.antfly_memory_forget.needsApproval).toBe(true);
  });

  it("forgets by evidence ID", async () => {
    let captured: Request | undefined;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (request: Request) => {
        captured = request.clone();
        return new Response('{"id":"op2"}', { headers: { "Content-Type": "application/json" } });
      })
    );
    const client = new AntflyClient({ baseUrl: "http://memory.test" });
    const tools = createMemoryTools({ client, space: "assistant" });
    await tools.antfly_memory_forget.execute?.({ evidenceIds: ["ev1"] }, toolOptions());
    expect(captured?.url).toContain("/memory/v1/spaces/assistant/forget");
    const body = await bodyOf(captured);
    expect(body.evidence_ids).toEqual(["ev1"]);
  });
});

it.each([
  ["antfly_memory_recall", { query: "fact" }],
  ["antfly_memory_remember", { subject: "a", predicate: "b", value: "c", statement: "abc" }],
  ["antfly_memory_forget", { evidenceIds: ["e"] }],
] as const)("serializes exact integers throughout %s results", async (name, input) => {
  const data = {
    revision: 18446744073709551615n,
    nested: [{ safe: 42, exact: 9007199254740993n }],
  };
  const POST = vi.fn(async () => ({ data, response: new Response() }));
  const tool = createMemoryTools({ client: { memoryApi: { POST } } as never, space: "s" })[name];
  const result = await tool.execute?.(input, toolOptions());
  expect(JSON.parse(JSON.stringify(result))).toEqual({
    revision: "18446744073709551615",
    nested: [{ safe: 42, exact: "9007199254740993" }],
  });
  expect(data.revision).toBe(18446744073709551615n);
});
it("preserves exact integers in memory errors", async () => {
  const POST = vi.fn(async () => ({
    error: { revision: 18446744073709551615n },
    response: new Response(null, { status: 409 }),
  }));
  const tool = createMemoryTools({
    client: { memoryApi: { POST } } as never,
    space: "s",
  }).antfly_memory_recall;
  await expect(tool.execute?.({ query: "fact" }, toolOptions())).rejects.toThrow(
    '"revision":"18446744073709551615"'
  );
});
