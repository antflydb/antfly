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
  const POST = vi.fn(async (_path: string, _options: unknown) => ({
    data,
    response: new Response(),
  }));
  const tool = createMemoryTools({ client: { memoryApi: { POST } } as never, space: "s" })[name];
  const result = await tool.execute?.(input, toolOptions());
  expect(JSON.parse(JSON.stringify(result))).toEqual({
    revision: "18446744073709551615",
    nested: [{ safe: 42, exact: "9007199254740993" }],
  });
  expect(data.revision).toBe(18446744073709551615n);
});
it("preserves exact integers in memory errors", async () => {
  const POST = vi.fn(async (_path: string, _options: unknown) => ({
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

it("opts into exactly the native eight-tool agent profile with host-owned scope", async () => {
  const { z } = await import("zod");
  const client = new AntflyClient({ baseUrl: "http://memory.test" });
  const tools = createMemoryTools({ client, space: "s", profile: "agent", runId: "run-unique" });
  expect(Object.keys(tools).sort()).toEqual([
    "antfly_memory_brief",
    "antfly_memory_correct",
    "antfly_memory_explain",
    "antfly_memory_forget",
    "antfly_memory_get",
    "antfly_memory_outcome",
    "antfly_memory_recall",
    "antfly_memory_remember",
  ]);
  for (const value of Object.values(tools)) {
    const schema = z.toJSONSchema(value.inputSchema as import("zod").ZodObject);
    for (const field of ["space", "database", "namespace", "branch", "runId", "idempotencyKey"]) {
      expect(schema.properties).not.toHaveProperty(field);
    }
  }
});

it("reconstructs stable mutation bodies and IDs from the host run and tool call", async () => {
  const requests: Request[] = [];
  vi.stubGlobal("fetch", async (request: Request) => {
    requests.push(request.clone());
    return new Response('{"id":"op","evidence_ids":["canonical"]}', {
      headers: { "Content-Type": "application/json" },
    });
  });
  const client = new AntflyClient({ baseUrl: "http://memory.test" });
  const options = { client, space: "s", runId: "global-request-uuid", branch: "feature" };
  const input = { subject: "s", predicate: "p", value: "v", statement: "A fact" };
  await createMemoryTools(options).antfly_memory_remember.execute?.(input, toolOptions());
  await createMemoryTools(options).antfly_memory_remember.execute?.(input, toolOptions());
  await createMemoryTools({ ...options, runId: "another-run" }).antfly_memory_remember.execute?.(
    input,
    toolOptions()
  );
  const first = requests[0];
  const retry = requests[1];
  const another = requests[2];
  if (!first || !retry || !another) throw new Error("Missing requests");
  expect(first.headers.get("Idempotency-Key")).toBe(retry.headers.get("Idempotency-Key"));
  expect(first.headers.get("Idempotency-Key")).not.toBe(another.headers.get("Idempotency-Key"));
  const body = await bodyOf(first);
  expect(body).toEqual(await bodyOf(retry));
  expect(body.scoped_evidence_ids).toBe(true);
  expect(body.assertions[0].id).not.toBe((await bodyOf(another)).assertions[0].id);
});

it("clamps retrieval budgets and preserves exact correction revisions", async () => {
  const requests: Request[] = [];
  vi.stubGlobal("fetch", async (request: Request) => {
    requests.push(request.clone());
    return new Response('{"revision":18446744073709551615}', {
      headers: { "Content-Type": "application/json" },
    });
  });
  const client = new AntflyClient({ baseUrl: "http://memory.test" });
  const tools = createMemoryTools({
    client,
    space: "s",
    profile: "agent",
    runId: "run",
    database: "db",
    namespace: "ns",
    branch: "b",
    recallLimit: 3,
    recallTokenBudget: 512,
    briefLimit: 2,
    briefTokenBudget: 512,
    utilityWeight: 0.5,
    explainMaxBytes: 1024,
  });
  const result = await tools.antfly_memory_recall.execute?.(
    { query: "q", limit: 200, tokenBudget: 65536 },
    toolOptions()
  );
  expect(result).toEqual({ revision: "18446744073709551615" });
  await tools.antfly_memory_brief.execute?.({ limit: 64, tokenBudget: 65536 }, toolOptions());
  await tools.antfly_memory_get.execute?.({ id: "a" }, toolOptions());
  await tools.antfly_memory_explain.execute?.({ id: "a" }, toolOptions());
  await tools.antfly_memory_correct.execute?.(
    {
      id: "a",
      expectedRevision: "18446744073709551615",
      lifecycle: "active",
      stability: "settled",
      reason: "Verified",
      evidenceIds: ["e"],
    },
    toolOptions()
  );
  await tools.antfly_memory_outcome.execute?.(
    { taskId: "task", success: false, usedMemoryIds: ["a"] },
    toolOptions()
  );
  expect(await bodyOf(requests[0])).toMatchObject({
    limit: 3,
    token_budget: 512,
    utility_weight: 0.5,
  });
  expect(await bodyOf(requests[1])).toEqual({ limit: 2, token_budget: 512 });
  expect(requests[2]?.method).toBe("GET");
  expect(await bodyOf(requests[3])).toMatchObject({ max_bytes: 1024 });
  expect(await requests[4]?.text()).toContain('"expected_revision":18446744073709551615');
  expect(await bodyOf(requests[5])).toMatchObject({ success: false, used_memory_ids: ["a"] });
  for (const request of requests) {
    expect(request.headers.get("X-Antfly-Database")).toBe("db");
    expect(request.headers.get("X-Antfly-Namespace")).toBe("ns");
    expect(request.headers.get("X-Antfly-Memory-Branch")).toBe("b");
  }
});

it("loads a fresh session brief and records only explicit consumed IDs and real outcomes", async () => {
  const { createMemorySession } = await import("../src/tools/memory.js");
  const requests: Request[] = [];
  vi.stubGlobal("fetch", async (request: Request) => {
    requests.push(request.clone());
    const body = request.url.endsWith("/brief")
      ? '{"items":[{"id":"a"}],"revision":9007199254740993}'
      : '{"id":"op"}';
    return new Response(body, { headers: { "Content-Type": "application/json" } });
  });
  const client = new AntflyClient({ baseUrl: "http://memory.test" });
  const session = createMemorySession({
    client,
    space: "s",
    runId: "global-run",
    briefTokenBudget: 512,
  });
  expect(Object.keys(session.tools)).toHaveLength(8);
  expect((await session.loadBrief()).revision).toBe("9007199254740993");
  await session.loadBrief();
  expect(requests).toHaveLength(2);
  await session.recordAccess(["b", "a", "a"], { eventId: "context-1" });
  await session.recordAccess(["a", "b"], { eventId: "context-1" });
  expect(await bodyOf(requests[2])).toEqual({ memory_ids: ["a", "b"] });
  expect(requests[2]?.headers.get("Idempotency-Key")).toBe(
    requests[3]?.headers.get("Idempotency-Key")
  );
  expect(await session.recordAccess([], { eventId: "empty" })).toBeNull();
  expect(requests).toHaveLength(4);
  await session.recordOutcome({
    eventId: "result",
    taskId: "task",
    success: false,
    usedMemoryIds: ["a"],
    detail: "Application verification failed",
  });
  expect(await bodyOf(requests[4])).toMatchObject({
    task_id: "task",
    success: false,
    used_memory_ids: ["a"],
  });
});

it.each([
  ["brief", {}],
  ["get", { id: "a" }],
  ["explain", { id: "a" }],
  ["correct", { id: "a", expectedRevision: 1, lifecycle: "active", reason: "Verified" }],
  ["outcome", { taskId: "task", success: true, usedMemoryIds: ["a"] }],
] as const)("forwards cancellation and JSON-safe integers for agent %s", async (name, input) => {
  const signal = new AbortController().signal;
  const POST = vi.fn(async (_path: string, _options: unknown) => ({
    data: { revision: 9007199254740993n },
    response: new Response(),
  }));
  const GET = vi.fn(async (_path: string, _options: unknown) => ({
    data: { revision: 9007199254740993n },
    response: new Response(),
  }));
  const tool = createMemoryTools({
    client: { memoryApi: { POST, GET } } as never,
    space: "s",
    profile: "agent",
  })[`antfly_memory_${name}`];
  expect(
    await tool.execute?.(input, { toolCallId: "t", messages: [], abortSignal: signal } as never)
  ).toEqual({ revision: "9007199254740993" });
  expect((POST.mock.calls[0] ?? GET.mock.calls[0])?.[1]).toMatchObject({ signal });
  POST.mockClear();
  GET.mockClear();
  await expect(
    tool.execute?.(input, {
      toolCallId: "t",
      messages: [],
      abortSignal: AbortSignal.abort(),
    } as never)
  ).rejects.toThrow();
  expect(POST).not.toHaveBeenCalled();
  expect(GET).not.toHaveBeenCalled();
});

it("validates host budgets and session identity before issuing requests", async () => {
  const { createMemorySession } = await import("../src/tools/memory.js");
  const client = new AntflyClient({ baseUrl: "http://memory.test" });
  expect(() => createMemoryTools({ client, space: "s", recallLimit: -1 })).toThrow(RangeError);
  expect(() => createMemoryTools({ client, space: "s", utilityWeight: Number.NaN })).toThrow(
    RangeError
  );
  expect(() => createMemorySession({ client, space: "s", runId: "" })).toThrow();
  expect(() => createMemoryTools({ client, space: "s", explainMaxBytes: 1023 })).toThrow(
    RangeError
  );
  expect(() => createMemoryTools({ client, space: "s", explainMaxBytes: 1024 })).not.toThrow();
});

it("remembers managed evidence without submitting external assertions and retries exactly", async () => {
  const { z } = await import("zod");
  const requests: Request[] = [];
  vi.stubGlobal("fetch", async (request: Request) => {
    requests.push(request.clone());
    return new Response('{"id":"op","phase":"pending"}', {
      headers: { "Content-Type": "application/json" },
    });
  });
  const client = new AntflyClient({ baseUrl: "http://memory.test" });
  const options = {
    client,
    space: "s",
    profile: "agent",
    rememberMode: "managed",
    runId: "managed-run",
  } as const;
  const input = { statement: "A fact worth extracting", sourceUrl: "https://example.com/note" };
  const tool = createMemoryTools(options).antfly_memory_remember;
  expect((tool.inputSchema as import("zod").ZodType).parse(input)).toEqual(input);
  expect(z.toJSONSchema(tool.inputSchema as import("zod").ZodObject).properties).not.toHaveProperty(
    "rememberMode"
  );
  await tool.execute?.(input, toolOptions());
  await createMemoryTools(options).antfly_memory_remember.execute?.(input, toolOptions());
  const first = requests[0];
  const retry = requests[1];
  if (!first || !retry) throw new Error("Missing requests");
  expect(first.headers.get("Idempotency-Key")).toBe(retry.headers.get("Idempotency-Key"));
  const body = await bodyOf(first);
  expect(body).toEqual(await bodyOf(retry));
  expect(body.assertions).toBeUndefined();
  expect(body.evidence[0]).toMatchObject({
    text: input.statement,
    source_url: input.sourceUrl,
    managed: true,
  });
  await expect(
    tool.execute?.(input, {
      toolCallId: "t1",
      messages: [],
      abortSignal: AbortSignal.abort(),
    } as never)
  ).rejects.toThrow();
  expect(requests).toHaveLength(2);
});
