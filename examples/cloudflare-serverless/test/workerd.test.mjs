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
test("workerd persists R2 pages and coordinated commits across restart", { timeout: 180000 }, async () => {
  const persist = await mkdtemp(join(tmpdir(), "antfly-workerd-"));
  const output = fileURLToPath(new URL("../dist/worker/", import.meta.url));
  const wasm = (await readdir(output)).filter(name => name.endsWith(".wasm"));
  const options = {
    modulesRoot: output,
    modules: [{ type: "ESModule", path: join(output, "harness.mjs"), contents: `
      import worker, { CommitCoordinator as BaseCoordinator } from "./worker.js";
      export class CommitCoordinator extends BaseCoordinator {
        async testGuardedProgress() {
          const key = "antfly/progress/guard-fixture/HEAD", target = "antfly/progress/guard-fixture/REINDEX";
          const first = await this.objectCall({op: "put", key}, new Uint8Array([1]));
          const next = await this.objectCall({op: "put", key}, new Uint8Array([2]));
          const stale = await this.objectCall({op: "put", key: target, guard_key: key, guard_etag: first.json.etag}, new Uint8Array([3]));
          const valid = await this.objectCall({op: "put", key: target, guard_key: key, guard_etag: next.json.etag}, new Uint8Array([4]));
          const foreign = await this.objectCall({op: "put", key: target, guard_key: "antfly/progress/other/HEAD", guard_etag: next.json.etag}, new Uint8Array([5]));
          return {stale: stale.status, valid: valid.status, foreign: foreign.status};
        }
        testJournalScale() {
          const j = this.journal, claim = j.claim();
          if (claim.busy) throw new Error("unexpected busy journal");
          const token = claim.token;
          try {
            if (claim.state.phase === "prepare") {
              j.prepared(token, {roots: [{kind: "manifest", version: "0"}], floor: "1", retired_before: "1", pin_prefix: "fixture/", artifact_prefix: "artifacts/", manifest_prefix: "manifests/"}); j.pins(token);
            }
            if (j.state().phase === "mark_artifacts") for (let i = 0; i < 32; i++) {
              const next = j.next(token);
              if (!next) { j.marked(token); break; }
              let children = [], key;
              if (next.task.kind === "manifest") {
                const offset = Number(next.task.version); key = "frontier/" + offset;
                children = Array.from({length: 128}, (_, n) => ({kind: "blob", artifact: {artifact_id: "a".repeat(175), checksum: "b".repeat(64), byte_len: offset+n+1, metadata_version: 0}}));
                if (offset + 128 < 2048) children.push({kind: "manifest", version: String(offset+128)});
              } else key = "body/" + next.task.artifact.byte_len;
              j.traced(token, next.id, {key, children});
            }
            const rows = this.ctx.storage.sql.exec("SELECT COUNT(*) AS marks FROM collection_marks").toArray()[0];
            return {done: j.state().phase === "compact_wal", ...rows};
          } finally { j.release(token); }
        }
      }
      let embeddingCalls = 0;
      export default { async fetch(request, env, ctx) {
        const path = new URL(request.url).pathname;
        const sent = [];
        const localEnv = { ...env, MAINTENANCE: { async send(value) { sent.push(value); } } };
        if (path === "/test/compaction-limit") return worker.fetch(new Request("https://example.test/api", request), {...localEnv, COMPACTION_MAX_DOCUMENTS: "0"}, ctx);
        if (path === "/test/sql-pressure") return worker.fetch(new Request("https://example.test/api", request), {...localEnv, COMMITS: {getByName() {return {async collection() {throw new Error("out of memory: SQLITE_NOMEM");}};}}}, ctx);
        if (path === "/test/reindex") return worker.fetch(new Request("https://example.test/api", request), {...localEnv, ANTFLY_INCARNATION: "test-reindex", PUBLICATION_RECORDS: "8"}, ctx);
        if (path === "/test/guard") return Response.json(await env.COMMITS.getByName("test-guard").testGuardedProgress());
        if (path === "/test/journal-scale") return Response.json(await env.COMMITS.getByName("test-journal-scale").testJournalScale());
        if (path === "/test/checkpoint-gc") {
          const namespace = "test-checkpoint-gc", stub = env.COMMITS.getByName(namespace);
          const today = Math.floor(Date.now() / 86400000), days = [today - 3, today];
          const domain = Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(namespace))), b => b.toString(16).padStart(2, "0")).join("");
          const keys = [];
          for (const day of days) {
            const part = day.toString(16).padStart(8, "0");
            const key = "antfly/progress/" + namespace + "/EMBEDDING_CACHE/" + part + "/fixture";
            const payload = "antfly/embedding-cache/" + domain + "/" + part + "/fixture";
            await stub.objectCall({op: "put", key}, new Uint8Array([1])); await env.DATABASE.put(payload, "scratch");
            keys.push({key, payload});
          }
          const result = await worker.fetch(new Request("https://example.test/api", {method: "POST", headers: {Authorization: "Bearer local-test"}, body: JSON.stringify({op: "prune_checkpoints"})}), {...localEnv, ANTFLY_INCARNATION: namespace}, ctx);
          return Response.json({result: await result.json(), statuses: await Promise.all(keys.map(async ({key, payload}) => [(await stub.objectCall({op: "get", key}, new Uint8Array())).status, !!await env.DATABASE.head(payload)]))});
        }
        if (path === "/test/paged") return worker.fetch(new Request("https://example.test/api", request), {...localEnv, ANTFLY_INCARNATION: "test-paged", PAGED_TEXT: "true", PAGED_PROJECTIONS: "true"}, ctx);
        if (path === "/test/recycle") return worker.fetch(new Request("https://example.test/api", request), {...localEnv, ANTFLY_INCARNATION: "test-recycle", WASM_RECYCLE_BYTES: "4194304"}, ctx);
        if (path === "/test/embedding") {
          const dimensions = new URL(request.url).searchParams.get("dimensions") ?? "4";
          const embeddingEnv = {...localEnv, ANTFLY_INCARNATION: "test-embedding", EMBEDDING_MODEL: "@cf/test/embedding", EMBEDDING_DIMENSIONS: dimensions, AI_GATEWAY: "test-gateway",
            AI: {async run(_model, input) { embeddingCalls++; return {data: input.text.map(() => Array(Number(dimensions)).fill(0.5))}; }} };
          const response = await worker.fetch(new Request("https://example.test/api", request), embeddingEnv, ctx);
          response.headers.set("X-Model-Calls", String(embeddingCalls)); return response;
        }
        if (path === "/test/queue") {
          let ack = false, retry = false;
          await worker.queue({messages: [{body: {incarnation: env.ANTFLY_INCARNATION, ...(new URL(request.url).searchParams.has("compact") ? {kind: "compact"} : {})},
            ack() { ack = true; }, retry() { retry = true; }}]}, localEnv);
          return Response.json({ack, retry, sent});
        }
        if (path === "/test/scheduled") {
          if (new URL(request.url).searchParams.has("compact")) localEnv.AUTO_COMPACT = "true";
          await worker.scheduled({scheduledTime: 3600000}, localEnv);
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
    const scheduleCompaction = await mf.dispatchFetch("https://example.test/test/scheduled?compact");
    assert.deepEqual(await scheduleCompaction.json(), [{incarnation: "test-incarnation"}, {incarnation: "test-incarnation", kind: "compact"}]);
    const deniedCompaction = await mf.dispatchFetch("https://example.test/test/compaction-limit", {method: "POST", headers: {Authorization: "Bearer local-test"}, body: JSON.stringify({op: "compact", compaction_max_documents: 999999})});
    assert.equal(deniedCompaction.status, 409);
    assert.match((await deniedCompaction.json()).error, /CompactionBudgetExceeded/);
    const compactQueue = await mf.dispatchFetch("https://example.test/test/queue?compact");
    assert.deepEqual(await compactQueue.json(), {ack: true, retry: false, sent: []});
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
    let publicationFinished = false;
    for (let i = 0; i < 30; i++) {
      const finished = await mf.dispatchFetch("https://example.test/test/queue");
      const status = await finished.json();
      assert.equal(status.ack, true); assert.equal(status.retry, false);
      if (!status.sent.length) { publicationFinished = true; break; }
    }
    assert.ok(publicationFinished, "bounded queue publication must converge");
    assert.equal((await call({ op: "search", text: "queued", limit: 1 })).hits[0].doc_id, "queued/0000");
    let job, collected = false;
    for (let step = 0; step < 400; step++) {
      const gc = await call({ op: "collect", ...(job ? { job_id: job } : {}) });
      job = gc.job_id;
      if (gc.done) { collected = true; break; }
      if (gc.delay_seconds) await new Promise(resolve => setTimeout(resolve, gc.delay_seconds * 1000 + 50));
      if (step % 12 === 0) {
        await mf.dispose();
        mf = new Miniflare(convertV4MiniflareOptions(options));
      }
    }
    assert.ok(collected, "GC must finish across coordinator and Worker restarts");
    assert.equal((await call({ op: "search", text: "queued", limit: 1 })).hits[0].doc_id, "queued/0000");
    assert.equal((await call(mutation)).visibility, committed.visibility, "GC must preserve old retry receipts");
    const sqlPressure = await mf.dispatchFetch("https://example.test/test/sql-pressure", {method: "POST", headers: {Authorization: "Bearer local-test"}, body: JSON.stringify({op: "collect"})});
    assert.equal(sqlPressure.status, 503); assert.equal(sqlPressure.headers.get("Retry-After"), "1");
    let largeJournal;
    for (let i = 0; i < 100; i++) {
      const response = await mf.dispatchFetch("https://example.test/test/journal-scale");
      assert.equal(response.status, 200); largeJournal = await response.json();
      if (largeJournal.done) break;
      if (i === 32) { await mf.dispose(); mf = new Miniflare(convertV4MiniflareOptions(options)); }
    }
    assert.deepEqual(largeJournal, {done: true, marks: 2064});
    const checkpointGC = await mf.dispatchFetch("https://example.test/test/checkpoint-gc");
    assert.deepEqual(await checkpointGC.json(), {result: {deleted: 1, has_more: false}, statuses: [[1, false], [0, true]]});
    const embeddingCall = async (body, dimensions = "4") => mf.dispatchFetch(`https://example.test/test/embedding?dimensions=${dimensions}`, {method: "POST", headers: {Authorization: "Bearer local-test"}, body: JSON.stringify(body)});
    assert.equal((await embeddingCall({op: "mutate", operation_id: "message", mutations: [{kind: "upsert", doc_id: "message/a", body: JSON.stringify({text: "mail body", owner: "alice"})}]})).status, 200);
    assert.equal((await embeddingCall({op: "publish"})).status, 200);
    const enriched = await embeddingCall({op: "enrich"});
    assert.equal(enriched.status, 200); assert.equal(enriched.headers.get("X-Model-Calls"), "1");
    assert.equal((await embeddingCall({op: "publish"})).status, 200);
    assert.equal((await embeddingCall({op: "compact"})).status, 200);
    const compactedStatus = await embeddingCall({op: "status"});
    assert.equal((await compactedStatus.json()).pending_embeddings, "0");
    await embeddingCall({op: "mutate", operation_id: "state", mutations: [{kind: "upsert", doc_id: "state/a", body: JSON.stringify({text: "", message_ref: "message/a", read: true})}]});
    await embeddingCall({op: "publish"});
    const idle = await embeddingCall({op: "enrich"});
    assert.equal(idle.headers.get("X-Model-Calls"), "1");
    const changedProfile = await embeddingCall({op: "publish"}, "8");
    assert.equal(changedProfile.status, 409);
    assert.match((await changedProfile.json()).error, /HostedEmbeddingProfileChanged/);
    const checkpointSource = Array.from({length: 70 * 8}, (_, i) => `word${i}`).join(" ");
    assert.equal((await embeddingCall({op: "mutate", operation_id: "checkpoint-body", mutations: [{kind: "upsert", doc_id: "message/large", body: JSON.stringify({text: checkpointSource})}]})).status, 200);
    assert.equal((await embeddingCall({op: "publish"})).status, 200);
    const firstWindow = await embeddingCall({op: "enrich"});
    assert.equal((await firstWindow.json()).yielded, true);
    await mf.dispose(); mf = new Miniflare(convertV4MiniflareOptions(options));
    const resumedWindow = await embeddingCall({op: "enrich"});
    const resumedStats = await resumedWindow.json();
    assert.equal(resumedStats.wal_appends, 1); assert.equal(resumedStats.checkpoint_hits, 1);
    assert.equal(resumedWindow.headers.get("X-Model-Calls"), "1", "restart must reuse the completed provider window");
    assert.equal((await embeddingCall({op: "publish"})).status, 200);
    const pagedCall = async body => {
      const response = await mf.dispatchFetch("https://example.test/test/paged", {method: "POST", headers: {Authorization: "Bearer local-test"}, body: JSON.stringify(body)});
      const result = await response.json(); assert.equal(response.status, 200, JSON.stringify(result)); return result;
    };
    await pagedCall({op: "mutate", operation_id: "paged", mutations: Array.from({length: 32}, (_, i) => ({kind: "upsert", doc_id: `doc/${i}`, body: JSON.stringify({text: "paged durable", embedding: [1,i/32], sparse_embedding: {durable: 1}})}))});
    while ((await pagedCall({op: "publish"})).has_more) {}
    await mf.dispose(); mf = new Miniflare(convertV4MiniflareOptions(options));
    assert.equal((await pagedCall({op: "search", text: "durable"})).hits.length, 10);
    assert.equal((await pagedCall({op: "search", mode: "vector", vector: [1,0]})).hits[0].doc_id, "doc/0");
    assert.equal((await pagedCall({op: "search", mode: "sparse", sparse: [{term: "durable", weight: 1}]})).hits.length, 10);
    await pagedCall({op: "mutate", operation_id: "paged-change", mutations: [{kind: "delete", doc_id: "doc/0"}, {kind: "upsert", doc_id: "doc/1", body: '{"text":"replacement"}'}]});
    await pagedCall({op: "publish"});
    assert.equal((await pagedCall({op: "count"})).count, "31");
    assert.equal((await pagedCall({op: "search", text: "replacement"})).hits[0].doc_id, "doc/1");
    assert.equal((await pagedCall({op: "compact"})).published, false);
    assert.deepEqual(await (await mf.dispatchFetch("https://example.test/test/guard")).json(), {stale: 2, valid: 0, foreign: 3});
    const reindexCall = async body => {
      const response = await mf.dispatchFetch("https://example.test/test/reindex", {method: "POST", headers: {Authorization: "Bearer local-test"}, body: JSON.stringify(body)});
      const result = await response.json(); assert.equal(response.status, 200, JSON.stringify(result)); return result;
    };
    await reindexCall({op: "mutate", operation_id: "reindex-source", mutations: Array.from({length: 24}, (_, i) => ({kind: "upsert", doc_id: `doc/${i}`, body: JSON.stringify({text: "original durable", embedding: [1,i/24], sparse_embedding: {durable: 1}})}))});
    while ((await reindexCall({op: "publish"})).has_more) {}
    const rebuild = await reindexCall({op: "reindex"});
    await reindexCall({op: "reindex", reindex_action: "step", reindex_job_id: rebuild.job_id});
    for (let i = 0; i < 3; i++) {
      await reindexCall({op: "mutate", operation_id: `during-reindex/${i}`, mutations: [{kind: "upsert", doc_id: "needle", body: JSON.stringify({text: "replacementneedle durable", embedding: [0,1], sparse_embedding: {needle: 2}})}]});
      while ((await reindexCall({op: "publish"})).has_more) {}
    }
    await mf.dispose(); mf = new Miniflare(convertV4MiniflareOptions(options));
    assert.equal((await reindexCall({op: "reindex", reindex_action: "status"})).scanned, 8);
    let retained = false;
    for (let step = 0; step < 256; step++) {
      const result = await reindexCall({op: "collect"});
      if (result.done) {retained = true; break;}
      if (result.delay_seconds) await new Promise(resolve => setTimeout(resolve, result.delay_seconds * 1000 + 100));
    }
    assert.ok(retained, "collection must retain the old source and partial reindex roots");
    let reindexed = false;
    for (let step = 0; step < 32; step++) {
      const result = await reindexCall({op: "reindex", reindex_action: "step", reindex_job_id: rebuild.job_id});
      if (!result.has_more) {reindexed = true; break;}
    }
    assert.ok(reindexed);
    assert.equal((await reindexCall({op: "count"})).count, "25");
    assert.equal((await reindexCall({op: "search", text: "replacementneedle"})).hits[0].doc_id, "needle");
    assert.equal((await reindexCall({op: "search", mode: "vector", vector: [0,1]})).hits[0].doc_id, "needle");
    assert.equal((await reindexCall({op: "search", mode: "sparse", sparse: [{term: "needle", weight: 1}]})).hits[0].doc_id, "needle");
    assert.equal((await reindexCall({op: "compact"})).published, false);
    const recycleCall = async body => {
      const response = await mf.dispatchFetch("https://example.test/test/recycle", {method: "POST", headers: {Authorization: "Bearer local-test"}, body: JSON.stringify(body)});
      const result = await response.json(); assert.equal(response.status, 200, JSON.stringify(result)); return result;
    };
    const recycledMutation = {op: "mutate", operation_id: "recycled-write", mutations: [{kind: "upsert", doc_id: "a", body: '{"text":"recycled compute"}'}]};
    const recycledReceipt = await recycleCall(recycledMutation);
    await recycleCall({op: "publish"});
    assert.equal((await recycleCall(recycledMutation)).visibility, recycledReceipt.visibility);
    assert.equal(JSON.parse((await recycleCall({op: "lookup", doc_id: "a", required_visibility: recycledReceipt.visibility})).body).text, "recycled compute");
    assert.equal((await recycleCall({op: "compact"})).published, true);
    assert.equal((await recycleCall({op: "count"})).count, "1");

  } finally {
    if (mf) await mf.dispose();
    await rm(persist, { recursive: true, force: true });
  }
});
