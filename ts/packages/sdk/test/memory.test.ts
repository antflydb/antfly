// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import { readFileSync } from "node:fs";
import { afterEach, expect, it, vi } from "vitest";
import { AntflyClient } from "../src/client.js";
import { RequestAdmission } from "../src/request-admission.js";
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

it("shares ordinary admission with native memory requests and closes both transports", async () => {
  const fetch = vi.fn(async () => new Response("{}"));
  vi.stubGlobal("fetch", fetch);
  const admission = new RequestAdmission({ maxConcurrent: 1, maxQueued: 0 });
  const client = new AntflyClient({ baseUrl: "http://memory.test", requestAdmission: admission });
  const held = await admission.fetch("http://memory.test/held");
  await expect(
    client.memoryApi.GET("/memory/v1/spaces/{space}", {
      params: { path: { space: "team" } },
    })
  ).rejects.toMatchObject({ reason: "queue_full", requestSent: false });
  expect(fetch).toHaveBeenCalledTimes(1);
  await held.text();
  await client.memoryApi.GET("/memory/v1/spaces/{space}", {
    params: { path: { space: "team" } },
  });
  expect(fetch).toHaveBeenCalledTimes(2);
  client.close();
  await expect(client.memory.explain("team", "claim")).rejects.toMatchObject({
    reason: "closed",
    requestSent: false,
  });
  expect(fetch).toHaveBeenCalledTimes(2);
});
