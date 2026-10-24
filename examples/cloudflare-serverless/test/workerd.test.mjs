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

import { test } from "node:test";
import assert from "node:assert/strict";
import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { mkdtemp, rm, readdir } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
test("workerd persists R2 pages and coordinated commits across restart", async () => {
  const persist = await mkdtemp(join(tmpdir(), "antfly-workerd-"));
  const output = fileURLToPath(new URL("../dist/worker/", import.meta.url));
  const wasm = (await readdir(output)).filter(name => name.endsWith(".wasm"));
  const options = {
    modulesRoot: output,
    modules: [{ type: "ESModule", path: join(output, "harness.mjs"), contents: `
      import worker from "./worker.js";
      export { CommitCoordinator } from "./worker.js";
      export default { async fetch(request, env, ctx) {
        const path = new URL(request.url).pathname;
        const sent = [];
        const localEnv = { ...env, MAINTENANCE: { async send(value) { sent.push(value); } } };
        if (path === "/test/queue") {
          let ack = false, retry = false;
          await worker.queue({messages: [{body: {incarnation: env.ANTFLY_INCARNATION},
            ack() { ack = true; }, retry() { retry = true; }}]}, localEnv);
          return Response.json({ack, retry, sent});
        }
        if (path === "/test/scheduled") {
          await worker.scheduled({}, localEnv);
          return Response.json(sent);
        }
        if (path === "/test/ai") {
          const aiEnv = {...env, AI_MODEL: "@cf/test/task", AI_GATEWAY: "test-gateway",
            AI: {async run(...args) { return {args}; }} };
          return worker.fetch(new Request("https://example.test/ai", request), aiEnv, ctx);
        }
        if (path === "/test/enqueue-failure") {
          localEnv.MAINTENANCE.send = async () => { throw new Error("queue unavailable"); };
          return worker.fetch(new Request("https://example.test/api", request), localEnv, ctx);
        }
        return worker.fetch(request, env, ctx);
      }};
    ` }, { type: "ESModule", path: join(output, "worker.js") }, ...wasm.map(name => ({ type: "CompiledWasm", path: join(output, name) }))],
    compatibilityDate: "2026-10-23",
    bindings: { API_TOKEN: "local-test", ANTFLY_INCARNATION: "test-incarnation" },
    resourcePersistencePath: join(persist, "resources"),
    isolatedResourcePersistencePath: join(persist, "isolated"),
    r2Buckets: ["DATABASE"],
    durableObjects: { COMMITS: { className: "CommitCoordinator", useSQLite: true } },

  };
  let mf;
  const call = async body => {
    const response = await mf.dispatchFetch("https://example.test/api", {
      method: "POST", headers: { Authorization: "Bearer local-test" }, body: JSON.stringify(body),
    });
    const result = await response.json();
    assert.equal(response.status, 200, JSON.stringify(result));
    return result;
  };
  try {
    mf = new Miniflare(convertV4MiniflareOptions(options));
    assert.equal((await mf.dispatchFetch("https://example.test/api", { method: "POST", body: "{}" })).status, 401);
    const mutation = { op: "mutate", operation_id: "one", timestamp_ns: "123", mutations: [
      { kind: "upsert", doc_id: "a", body: JSON.stringify({ text: "durable cloudflare" }) },
    ] };
    const committed = await call(mutation);
    await mf.dispose();
    mf = new Miniflare(convertV4MiniflareOptions(options));
    assert.deepEqual(await call(mutation), committed);
    const result = await call({ op: "lookup", doc_id: "a", required_visibility: committed.visibility });
    assert.equal(JSON.parse(result.body).text, "durable cloudflare");
    assert.equal((await call({ op: "latest" })).visibility, committed.visibility);
    const queued = await mf.dispatchFetch("https://example.test/test/queue");
    assert.deepEqual(await queued.json(), { ack: true, retry: false, sent: [] });
    assert.equal((await call({ op: "latest" })).visibility, committed.visibility);
    assert.deepEqual((await call({ op: "search", text: "cloudflare" })).hits.map(x => x.doc_id), ["a"]);
    await mf.dispose();
    mf = new Miniflare(convertV4MiniflareOptions(options));
    assert.equal((await call({ op: "latest" })).visibility, committed.visibility);
    assert.equal(JSON.parse((await call({ op: "lookup", doc_id: "a", required_visibility: committed.visibility })).body).text, "durable cloudflare");
    const scheduled = await mf.dispatchFetch("https://example.test/test/scheduled");
    assert.deepEqual(await scheduled.json(), [{ incarnation: "test-incarnation" }]);
    const ai = await mf.dispatchFetch("https://example.test/test/ai", { method: "POST",
      headers: { Authorization: "Bearer local-test" }, body: JSON.stringify({ text: "task input" }) });
    assert.deepEqual(await ai.json(), { args: ["@cf/test/task", { text: "task input" }, { gateway: { id: "test-gateway" } }] });
    const failedQueue = await mf.dispatchFetch("https://example.test/test/enqueue-failure", { method: "POST",
      headers: { Authorization: "Bearer local-test" }, body: JSON.stringify({ ...mutation, operation_id: "two" }) });
    const accepted = await failedQueue.json();
    assert.equal(accepted.committed, true);
    assert.equal(accepted.maintenance_pending, true);
    assert.equal((await call({ op: "lookup", doc_id: "a", required_visibility: accepted.visibility })).body, result.body);
    const bucket = await mf.getR2Bucket("DATABASE");
    const listing = await bucket.list();
    assert.ok(listing.objects.some(x => x.key.includes("/pages/")));
    assert.ok(listing.objects.every(x => !x.key.endsWith("/HEAD")));
    await call({ op: "mutate", operation_id: "queued-batch", mutations: Array.from({ length: 300 }, (_, i) => ({
      kind: "upsert", doc_id: `queued/${String(i).padStart(4, "0")}`, body: JSON.stringify({ text: "queued document" }),
    })) });
    const partial = await mf.dispatchFetch("https://example.test/test/queue");
    assert.deepEqual(await partial.json(), { ack: true, retry: false, sent: [{ incarnation: "test-incarnation" }] });
    const finished = await mf.dispatchFetch("https://example.test/test/queue");
    assert.deepEqual(await finished.json(), { ack: true, retry: false, sent: [] });
    assert.equal((await call({ op: "search", text: "queued", limit: 1 })).hits[0].doc_id, "queued/0000");
  } finally {
    if (mf) await mf.dispose();
    await rm(persist, { recursive: true, force: true });
  }
});
