// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import { readFileSync } from "node:fs";
import { afterEach, expect, it, vi } from "vitest";
import { AntflyClient } from "../src/client.js";
import { parseMemoryJSON, stringifyMemoryJSON } from "../src/memory.js";

afterEach(() => vi.unstubAllGlobals());

it.each([
  false,
  true,
])("round-trips uint64 references with Content-Length=%s and current authentication", async (length) => {
  const sent: Request[] = [];
  const body =
    '{"nodes":[{"resource":{"database":"default","namespace":"public","table":"business","table_id":18446744073709551615,"kind":"record","id":"approval","revision":9007199254740993},"record":{},"links":[]}],"complete":true,"truncated":false,"missing_evidence":false}';
  vi.stubGlobal("fetch", async (request: Request) => {
    sent.push(request.clone());
    return new Response(body, {
      headers: {
        "Content-Type": "application/json",
        ...(length ? { "Content-Length": String(new TextEncoder().encode(body).length) } : {}),
      },
    });
  });
  const client = new AntflyClient({
    baseUrl: "http://memory.test",
    auth: { username: "alice", password: "secret" },
    headers: { "X-Antfly-Database": "projects" },
  });
  const first = await client.memory.explain("team", "claim");
  const resource = first.data?.nodes[0]?.resource;
  if (!resource) throw new Error("Missing resource reference");
  expect(resource.revision).toBe(9007199254740993n);
  expect(resource.table_id).toBe(18446744073709551615n);
  client.setAuth({ type: "token", token: "refreshed" });
  await client.memory.explainResource("team", { resource });
  const second = sent[1];
  if (!second) throw new Error("Missing forwarded request");
  const forwarded = await second.text();
  expect(forwarded).toContain('"revision":9007199254740993');
  expect(forwarded).toContain('"table_id":18446744073709551615');
  expect(second.headers.get("Authorization")).toBe("Bearer refreshed");
  expect(second.headers.get("X-Antfly-Database")).toBe("projects");
});

it("preserves full-width conflict metadata and supports explicit raw responses", async () => {
  vi.stubGlobal(
    "fetch",
    vi
      .fn()
      .mockResolvedValueOnce(new Response('{"revision":9007199254740993}', { status: 409 }))
      .mockResolvedValueOnce(new Response('{"revision":9007199254740993}'))
  );
  const client = new AntflyClient({ baseUrl: "http://memory.test" });
  expect((await client.memory.explain("team", "claim")).error).toEqual({
    revision: 9007199254740993n,
  });
  const raw = await client.memoryApi.GET("/memory/v1/spaces/{space}", {
    params: { path: { space: "team" } },
    parseAs: "text",
  });
  expect(raw.data).toBe('{"revision":9007199254740993}');
});

it("keeps safe numeric values and numeric strings distinct and rejects rounded requests", () => {
  expect(
    parseMemoryJSON('{"small":7,"float":0.25,"id":"9007199254740993","big":9007199254740993}')
  ).toEqual({ small: 7, float: 0.25, id: "9007199254740993", big: 9007199254740993n });
  expect(stringifyMemoryJSON({ revision: 9007199254740993n })).toBe(
    '{"revision":9007199254740993}'
  );
  expect(() => stringifyMemoryJSON({ revision: Number("9007199254740993") })).toThrow(
    /already lost precision/
  );
});

it("decodes native discovery and proposal fixtures through the typed memory transport", async () => {
  const fixture = (name: string) =>
    readFileSync(
      new URL(
        `../../../../zig/pkg/antfly-embedded/src/memory/fixtures/wire/${name}.json`,
        import.meta.url
      ),
      "utf8"
    );
  vi.stubGlobal("fetch", async (request: Request) => {
    const path = new URL(request.url).pathname;
    const name = path.endsWith("/candidates")
      ? "candidates"
      : path.endsWith("/entity-proposals")
        ? "proposals"
        : "proposal";
    return new Response(fixture(name), { headers: { "Content-Type": "application/json" } });
  });
  const client = new AntflyClient({ baseUrl: "http://memory.test" });
  const { data: candidates } = await client.memoryApi.POST(
    "/memory/v1/spaces/{space}/entities/{id}/candidates",
    {
      params: { path: { space: "wire", id: "a" } },
      body: {},
    }
  );
  expect(candidates?.seed.entity.id).toBe("a");
  expect(candidates?.items[0]?.entity.entity.id).toBe("b");
  expect(candidates?.items[0]?.view.canonical_id).toBe("b");
  expect(candidates?.revision).toBe(3);
  expect(candidates?.next_cursor).toBeNull();
  const { data: proposals } = await client.memoryApi.GET(
    "/memory/v1/spaces/{space}/entity-proposals",
    {
      params: { path: { space: "wire" } },
    }
  );
  const proposalId = proposals?.items[0]?.id;
  if (!proposalId) throw new Error("Missing native proposal");
  const { data: details } = await client.memoryApi.GET(
    "/memory/v1/spaces/{space}/entity-proposals/{id}",
    {
      params: { path: { space: "wire", id: proposalId } },
    }
  );
  expect(details?.proposal.id).toBe(proposalId);
  expect(details?.inputs.actor).toBe("codex/consolidation");
  expect(details?.inputs.entities[0]?.revision).toBe(3);
  expect(details?.reason).toBeNull();
  expect(details?.stale_configuration).toBe(false);
});

function captureMemoryRequests(response = "{}") {
  const sent: Request[] = [];
  vi.stubGlobal("fetch", async (request: Request) => {
    sent.push(request.clone());
    return new Response(response, { headers: { "Content-Type": "application/json" } });
  });
  return sent;
}

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

it("generates idempotency keys for mutations and omits them for reads", async () => {
  const sent = captureMemoryRequests();
  const client = new AntflyClient({ baseUrl: "http://memory.test" });
  const outcome = { id: "o1", task_id: "t1", success: true, used_memory_ids: ["m1"] };
  await client.memory.outcome("team", outcome);
  await client.memory.outcome("team", outcome);
  await client.memory.pin("team", "m1");
  await client.memory.maintain("team");
  await client.memory.brief("team");
  await client.memory.consolidate("team");
  await client.memory.recall("team", { query: "deploys" });
  const keys = sent.map((request) => request.headers.get("Idempotency-Key"));
  for (const key of keys.slice(0, 4)) expect(key).toMatch(UUID);
  expect(new Set(keys.slice(0, 4)).size).toBe(4);
  expect(keys.slice(4)).toEqual([null, null, null]);
});

it("passes explicit and legacy positional idempotency keys through", async () => {
  const sent = captureMemoryRequests();
  const client = new AntflyClient({ baseUrl: "http://memory.test" });
  const observation = { evidence: [{ id: "e1", text: "hello" }] };
  await client.memory.observe("team", observation, "legacy-key");
  await client.memory.observe("team", observation, { idempotencyKey: "explicit-key" });
  await client.memory.create({ space: "team" }, "create-key");
  await client.memory.advance("team", "op1", "advance-key");
  expect(sent.map((request) => request.headers.get("Idempotency-Key"))).toEqual([
    "legacy-key",
    "explicit-key",
    "create-key",
    "advance-key",
  ]);
});

it("sends scope headers only when provided", async () => {
  const sent = captureMemoryRequests();
  const client = new AntflyClient({ baseUrl: "http://memory.test" });
  await client.memory.brief("team", {}, { database: "projects", namespace: "eng", branch: "exp" });
  await client.memory.pin("team", "m1", { database: "projects", idempotencyKey: "k" });
  await client.memory.inspect("team");
  const [brief, pin, inspect] = sent;
  if (!brief || !pin || !inspect) throw new Error("Missing requests");
  expect(brief.headers.get("X-Antfly-Database")).toBe("projects");
  expect(brief.headers.get("X-Antfly-Namespace")).toBe("eng");
  expect(brief.headers.get("X-Antfly-Memory-Branch")).toBe("exp");
  expect(pin.headers.get("X-Antfly-Database")).toBe("projects");
  expect(pin.headers.get("X-Antfly-Namespace")).toBeNull();
  expect(pin.headers.get("Idempotency-Key")).toBe("k");
  for (const name of ["X-Antfly-Database", "X-Antfly-Namespace", "X-Antfly-Memory-Branch"]) {
    expect(inspect.headers.get(name)).toBeNull();
  }
});

it("routes helpers to their native memory endpoints", async () => {
  const sent = captureMemoryRequests();
  const client = new AntflyClient({ baseUrl: "http://memory.test" });
  await client.memory.put("team", { version: 1 });
  await client.memory.configure("team", { expected_revision: 3, config: {} });
  await client.memory.brief("team", { token_budget: 500, task: "deploy" });
  await client.memory.consolidate("team", { limit: 10 });
  await client.memory.pin("team", "m1", { reason: "core" });
  await client.memory.unpin("team", "m1");
  await client.memory.outcome("team", {
    id: "o1",
    task_id: "t1",
    success: false,
    used_memory_ids: [],
  });
  await client.memory.list("team", { subject: "svc", limit: 5 });
  await client.memory.history("team", "m1", { limit: 2 });
  await client.memory.entities("team", { limit: 3 });
  await client.memory.entity("team", "ent");
  await client.memory.evidence("team", "e1");
  await client.memory.episodes("team");
  await client.memory.episode("team", "ep1");
  await client.memory.changes("team", { start: "latest" });
  await client.memory.maintain("team");
  const routes = sent.map((request) => {
    const url = new URL(request.url);
    return `${request.method} ${url.pathname}${url.search}`;
  });
  expect(routes).toEqual([
    "PUT /memory/v1/spaces/team",
    "PATCH /memory/v1/spaces/team",
    "POST /memory/v1/spaces/team/brief",
    "POST /memory/v1/spaces/team/consolidation",
    "POST /memory/v1/spaces/team/pins",
    "POST /memory/v1/spaces/team/pins",
    "POST /memory/v1/spaces/team/outcomes",
    "GET /memory/v1/spaces/team/memories?subject=svc&limit=5",
    "GET /memory/v1/spaces/team/memories/m1/history?limit=2",
    "GET /memory/v1/spaces/team/entities?limit=3",
    "GET /memory/v1/spaces/team/entities/ent",
    "GET /memory/v1/spaces/team/evidence/e1",
    "GET /memory/v1/spaces/team/episodes",
    "GET /memory/v1/spaces/team/episodes/ep1",
    "POST /memory/v1/spaces/team/changes",
    "POST /memory/v1/spaces/team/maintenance",
  ]);
  const bodies = await Promise.all(sent.map((request) => request.text()));
  expect(JSON.parse(bodies[2] ?? "")).toEqual({ token_budget: 500, task: "deploy" });
  expect(JSON.parse(bodies[4] ?? "")).toEqual({ id: "m1", pinned: true, reason: "core" });
  expect(JSON.parse(bodies[5] ?? "")).toEqual({ id: "m1", pinned: false });
});

it("remembers text as one evidence item cited by evidence-less assertions", async () => {
  const sent = captureMemoryRequests();
  const client = new AntflyClient({ baseUrl: "http://memory.test" });
  const assertion = {
    id: "a1",
    subject: "deploys",
    predicate: "happen_on",
    value: "tuesday",
    statement: "Deploys happen on Tuesday.",
  };
  const generated = await client.memory.remember("team", {
    text: "We deploy on Tuesdays.",
    sourceId: "slack",
    evidence: { author: "alice" },
    assertions: [assertion],
  });
  await client.memory.remember("team", { text: "Second", id: "e2" }, { idempotencyKey: "k2" });
  const [first, second] = sent;
  if (!first || !second) throw new Error("Missing requests");
  expect(new URL(first.url).pathname).toBe("/memory/v1/spaces/team/observations");
  expect(first.headers.get("Idempotency-Key")).toMatch(UUID);
  expect(generated.evidenceId).toMatch(UUID);
  expect(JSON.parse(await first.text())).toEqual({
    evidence: [
      {
        author: "alice",
        id: generated.evidenceId,
        text: "We deploy on Tuesdays.",
        source_id: "slack",
      },
    ],
    assertions: [{ ...assertion, evidence: [{ id: generated.evidenceId }] }],
  });
  expect(second.headers.get("Idempotency-Key")).toBe("k2");
  expect(JSON.parse(await second.text())).toEqual({ evidence: [{ id: "e2", text: "Second" }] });
});
