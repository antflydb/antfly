// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { test } from "node:test";
import { createPhaseTimings, profileClientOperations, waitForWorkerResult } from "./worker-progress.mjs";

const child = () => Object.assign(new EventEmitter(), { pid: 123 });

test("progress is not a terminal result and listeners are removed on completion", async () => {
  const worker = child();
  const result = waitForWorkerResult(worker, "migrate");
  worker.emit("message", { type: "progress", timings: { active: [{ phase: "CREATE" }] } });
  let completed = false;
  result.then(() => { completed = true; });
  await Promise.resolve();
  assert.equal(completed, false);
  worker.emit("message", { error: null, applied: 1 });
  assert.deepEqual(await result, { error: null, applied: 1 });
  for (const event of ["message", "exit", "error"]) assert.equal(worker.listenerCount(event), 0);
});

test("a timed-out worker retains its active operation", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const worker = child();
  const result = waitForWorkerResult(worker, "migrate", { timeoutMs: 1000 });
  const rejected = assert.rejects(result, /did not report.*CLAIM DELETE/);
  worker.emit("message", { type: "progress", timings: { active: [{ phase: "CLAIM DELETE", duration_ms: 900 }] } });
  t.mock.timers.tick(1000);
  await rejected;
  assert.equal(worker.listenerCount("message"), 0);
});

test("an early worker exit includes its last progress", async () => {
  const worker = child();
  const result = waitForWorkerResult(worker, "migrate");
  const rejected = assert.rejects(result, /code=17.*CREATE/);
  worker.emit("message", { type: "progress", timings: { active: [{ phase: "CREATE" }] } });
  worker.emit("exit", 17, null);
  await rejected;
});

test("progress is periodic, rate limited and bounded under IPC backpressure", async (t) => {
  t.mock.timers.enable({ apis: ["setInterval"] });
  let clock = 0;
  let acknowledge;
  const messages = [];
  const profile = createPhaseTimings({ now: () => clock, connected: () => true,
    send: (message, callback) => { messages.push(message); acknowledge = callback; } });
  try {
    for (let i = 0; i < 100; i++) await profile.track("SELECT", async () => i);
    assert.equal(messages.length, 1);
    assert.equal(messages[0].timings.active[0].phase, "SELECT");
    let finish;
    const active = profile.track("CREATE", () => new Promise((resolve) => { finish = resolve; }));
    clock = 5000;
    t.mock.timers.tick(5000);
    assert.equal(messages.length, 1, "unacknowledged progress must not accumulate");
    acknowledge();
    clock = 6000;
    t.mock.timers.tick(1000);
    assert.equal(messages.length, 2);
    assert.equal(messages[1].timings.active[0].phase, "CREATE");
    assert.equal(messages[1].timings.active[0].duration_ms, 6000);
    finish();
    await active;
    assert.equal(profile.snapshot().active_count, 0);
    assert.equal(profile.snapshot().slowest.length, 8);
    assert.equal(profile.snapshot().queries.SELECT.calls, 100);
  } finally { profile.close(); }
});

test("diagnostic transport failure does not change query failures or results", async () => {
  const profile = createPhaseTimings({ connected: () => true, send: () => { throw new Error("closed IPC"); } });
  try {
    assert.equal(await profile.track("SELECT", async () => 7), 7);
    const failure = new Error("query failure");
    await assert.rejects(profile.track("CREATE", async () => { throw failure; }), (error) => error === failure);
  } finally { profile.close(); }
});


test("HTTP progress remains active after headers until the SDK consumes the body", async (t) => {
  const { AntflyClient } = await import("../../ts/packages/sdk/dist/index.js");
  t.mock.timers.enable({ apis: ["setInterval"] });
  let controller;
  let clock = 0;
  let headers;
  const headersReceived = new Promise((resolve) => { headers = resolve; });
  t.mock.method(globalThis, "fetch", async () => {
    const response = new Response(new ReadableStream({ start(value) { controller = value; } }),
      { headers: { "Content-Type": "application/json" } });
    headers();
    return response;
  });
  const messages = [];
  const profile = createPhaseTimings({ now: () => clock, connected: () => true,
    send: (message, callback) => { messages.push(message); callback(); } });
  const client = new AntflyClient({ baseUrl: "http://body-profile.test" });
  const restore = profileClientOperations(client.api, profile);
  try {
    let settled = false;
    const result = client.api.GET("/db/v1/databases").then((value) => { settled = true; return value; });
    await headersReceived;
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(settled, false);
    assert.equal(profile.snapshot().active_count, 1);
    clock = 1000;
    t.mock.timers.tick(1000);
    assert.equal(messages.at(-1).timings.active[0].phase, "HTTP");
    assert.equal(messages.at(-1).timings.active[0].duration_ms, 1000);
    controller.enqueue(new TextEncoder().encode('[{"name":"default"}]'));
    controller.close();
    assert.deepEqual((await result).data, [{ name: "default" }]);
    assert.equal(profile.snapshot().active_count, 0);
    assert.equal(profile.snapshot().queries.HTTP.calls, 1);
    assert.equal(profile.snapshot().queries.HTTP.milliseconds, 1000);
  } finally { restore(); profile.close(); }
});

test("body-read errors retain their identity and finish the HTTP operation", async (t) => {
  const { AntflyClient } = await import("../../ts/packages/sdk/dist/index.js");
  let controller;
  t.mock.method(globalThis, "fetch", async () => new Response(
    new ReadableStream({ start(value) { controller = value; } }),
    { headers: { "Content-Type": "application/json" } },
  ));
  const profile = createPhaseTimings();
  const client = new AntflyClient({ baseUrl: "http://body-profile.test" });
  const original = client.api.GET;
  const restore = profileClientOperations(client.api, profile);
  try {
    const failure = new Error("body interrupted");
    const result = client.api.GET("/db/v1/databases");
    const rejected = assert.rejects(result, (error) => error === failure);
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(profile.snapshot().active_count, 1);
    controller.error(failure);
    await rejected;
    assert.equal(profile.snapshot().active_count, 0);
    assert.equal(profile.snapshot().queries.HTTP.calls, 1);
  } finally { restore(); profile.close(); }
  assert.equal(client.api.GET, original);
});
