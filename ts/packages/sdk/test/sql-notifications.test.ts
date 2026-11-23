// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { afterEach, expect, it, vi } from "vitest";
import { AntflyClient } from "../src/client.js";

afterEach(() => vi.unstubAllGlobals());

it("polls and acknowledges a bounded page without evaluating or retrying SQL", async () => {
  const cursor = "00000000000000010001";
  const page = {
    notifications: [{ channel: "events", payload: "updated", sender_pid: 42 }],
    cursor,
    pending: false,
  };
  const fetch = vi.fn().mockResolvedValue(new Response(JSON.stringify(page)));
  vi.stubGlobal("fetch", fetch);
  const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
  expect(
    await client.pollSQLNotifications("a".repeat(32), { acknowledge: cursor, limit: 1 })
  ).toEqual(page);
  expect(fetch).toHaveBeenCalledTimes(1);
  expect(fetch.mock.calls[0][0]).toBe(
    `http://localhost:8080/db/v1/sql/connections/${"a".repeat(32)}/notifications`
  );
  expect(JSON.parse(fetch.mock.calls[0][1].body)).toEqual({ acknowledge: cursor, limit: 1 });
  expect(fetch.mock.calls[0][1].redirect).toBe("error");
});

it.each([
  { limit: 0 },
  { limit: 129 },
  { acknowledge: "bad" },
])("rejects invalid notification controls before dispatch", async (request) => {
  const fetch = vi.fn();
  vi.stubGlobal("fetch", fetch);
  const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
  await expect(client.pollSQLNotifications("a".repeat(32), request)).rejects.toThrow();
  expect(fetch).not.toHaveBeenCalled();
});

it.each([
  {
    notifications: [{ channel: "events", payload: "x", sender_pid: true }],
    cursor: "0".repeat(20),
    pending: false,
  },
  { notifications: [{ channel: "events", payload: "x", sender_pid: 42 }], pending: false },
  {
    notifications: [{ channel: "events", payload: "x".repeat(8000), sender_pid: 42 }],
    cursor: "0".repeat(20),
    pending: false,
  },
])("rejects malformed notification pages", async (page) => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify(page))));
  const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
  await expect(client.pollSQLNotifications("a".repeat(32))).rejects.toThrow();
});
