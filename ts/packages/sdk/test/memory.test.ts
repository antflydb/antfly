// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

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
