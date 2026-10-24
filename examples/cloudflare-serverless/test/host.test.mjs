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

import { Bucket } from "./object-fixture.mjs";
import { fixture } from "./collection-fixture.mjs";
import { collect } from "../collect.mjs";
import { withCommitCoordinator } from "../../../zig/lib/objectstore/wasm/coordinated.mjs";
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { instantiateHost } from "../../../zig/lib/objectstore/wasm/asyncify.mjs";
import { createImmutableReadCache } from "../../../zig/lib/objectstore/wasm/cache.mjs";
import { createR2Transport, ObjectStoreError } from "../../../zig/lib/objectstore/wasm/r2.mjs";
const module = await WebAssembly.compile(await readFile(new URL("../dist/antfly-serverless.wasm", import.meta.url)));
const payloadText = record => Buffer.from(record.payload_base64, "base64").toString("utf8");
const request = (op, args = {}) => ({ op, bucket: "database", prefix: "scope", namespace: "tenant/table/incarnation1", ...args });
const append = (id, payload = id) => request("append", { operation_id: id, payload, timestamp_ns: "9007199254740993" });
async function setup(bucket = new Bucket(), wrap = transport => transport, options = {}) {
  const transport = createR2Transport({ database: bucket });
  return { bucket, transport, host: await instantiateHost(module, wrap(transport), { onLog: message => console.error("WASM:", message), ...options }) };
}
test("suspended WASM commits once, survives eviction, and enforces visibility and retention", async () => {
  const { bucket, host } = await setup();
  const first = await host.request(append("op1", "one"));
  const writes = bucket.writes;
  assert.deepEqual(await host.request(append("op1", "one")), first);
  assert.equal(bucket.writes, writes, "idempotent retry must not write again");
  await assert.rejects(host.request(append("op1", "different")), /WalIdempotencyConflict/);
  const reopened = (await setup(bucket)).host;
  const result = await reopened.request(request("read", { required_visibility: first.visibility }));
  assert.equal(payloadText(result.records[0]), "one");
  assert.equal(result.records[0].timestamp_ns, "9007199254740993");
  await assert.rejects(reopened.request(request("read", { namespace: "other", required_visibility: first.visibility })), /VisibilityIncarnationMismatch/);
  await assert.rejects(reopened.request(request("read", { end_lsn: "2" })), /WalVisibilityUnavailable/);
  await assert.rejects(reopened.request(request("read", { max_bytes: 1 })), /WalReadBudgetExceeded/);
  await reopened.request(request("truncate", { keep_from_lsn: "100" }));
  await assert.rejects(reopened.request(request("read")), /WalSnapshotExpired/);
  assert.deepEqual(await reopened.request(append("op1", "one")), first, "retention preserves receipts");
  await reopened.request(append("op2", "two"));
  const tail = await reopened.request(request("read", { start_lsn: "2" }));
  assert.equal(tail.records[0].lsn, "2");
});
test("racing writers publish a contiguous prefix and retry an ambiguous committed write", async () => {
  const bucket = new Bucket();
  const a = (await setup(bucket)).host, b = (await setup(bucket)).host;
  await Promise.all([a.request(append("a")), b.request(append("b"))]);
  const result = await a.request(request("read"));
  assert.deepEqual(result.records.map(x => x.lsn), ["1", "2"]);
  assert.deepEqual(new Set(result.records.map(payloadText)), new Set(["a", "b"]));
  let fail = true;
  const fault = (await setup(bucket, transport => async (req, body) => {
    const result = await transport(req, body);
    if (req.op === "put" && req.key.endsWith("/HEAD") && fail) { fail = false; throw new Error("lost response after durable commit"); }
    return result;
  })).host;
  await assert.rejects(fault.request(append("ambiguous")), /ObjectStoreTransportFailed/);
  await fault.request(append("ambiguous"));
  const after = await a.request(request("read"));
  assert.equal(after.records.length, 3);
});
test("tail reads and idempotency lookup do not load historical WAL bytes", async t => {
  const { bucket, host } = await setup();
  for (let i = 0; i < 100; i++) await host.request(append(`op${i}`, "x".repeat(1024)));
  for (let i = 0; i < 64; i++) await host.request(request("append_batch", { operation_id: `bulk${i}`, payloads: Array(64).fill("y".repeat(1024)) }));
  const reopened = (await setup(bucket)).host;
  bucket.readBytes = 0; bucket.reads = 0;
  const result = await reopened.request(request("read", { start_lsn: "4196" }));
  assert.equal(result.records.length, 1);
  assert.ok(bucket.readBytes < 64 * 1024, `tail read fetched ${bucket.readBytes} bytes`);
  assert.ok(bucket.reads < 8);
  t.diagnostic(`cold tail of 4196 records: ${bucket.reads} GETs, ${bucket.readBytes} bytes, ${reopened.memoryBytes()} linear-memory bytes`);
  bucket.readBytes = 0;
  await reopened.request(append("op0", "x".repeat(1024)));
  assert.ok(bucket.readBytes < 64 * 1024, "retry must use the receipt index");
  assert.ok(host.memoryBytes() < 64 * 1024 * 1024);
});
test("R2 adapter preserves conditional/ranged operations and rejects unsupported deletion", async () => {
  const { transport } = await setup();
  const base = { bucket: "database", key: "key" };
  const put = await transport({ ...base, op: "put", if_none_match: true }, new TextEncoder().encode("abcdef"));
  await assert.rejects(transport({ ...base, op: "put", if_none_match: true }, new Uint8Array()), error => error.status === 2);
  const get = await transport({ ...base, op: "get", range: { offset: 2, length: 2 }, if_match_etag: put.json.etag });
  assert.equal(new TextDecoder().decode(get.body), "cd");
  await assert.rejects(transport({ ...base, op: "get", if_match_etag: "stale" }), error => error.status === 2);
  await assert.rejects(transport({ ...base, op: "get", max_response_bytes: 2 }), error => error.status === 4);
  await assert.rejects(transport({ ...base, op: "delete", if_match_etag: put.json.etag }), error => error.status === 3);
  await assert.rejects(transport({ ...base, op: "head", bucket: "unbound" }), ObjectStoreError);
});

test("host admission stops canceled or expired work before storage and bounds I/O", async () => {
  const { host, bucket } = await setup();
  await assert.rejects(host.request(append("cancel"), { signal: AbortSignal.abort() }), /Canceled/);
  await assert.rejects(host.request(append("expired"), { deadlineMs: 0 }), /DeadlineExceeded/);
  await assert.rejects(host.request(append("invalid-budget"), { maxOperations: NaN }), /invalid request budget/);
  await assert.rejects(host.request(append("invalid-budget"), { maxTransferBytes: -1 }), /invalid request budget/);
  assert.equal(bucket.writes, 0);
  await assert.rejects(host.request(append("bounded"), { maxOperations: 1 }), /ObjectIoBudgetExceeded/);
  assert.equal((await host.request(request("latest"))).visibility.slice(-16), "0000000000000000");
  const circular = {}; circular.self = circular;
  await assert.rejects(host.request(circular), TypeError);
  await host.request(append("ok"));
});

test("a batch publishes one committed prefix and retries without duplicate mutations", async () => {
  const { host, bucket } = await setup();
  const input = request("append_batch", { operation_id: "batch", payloads: ["a", "b", "c"] });
  const first = await host.request(input);
  const writes = bucket.writes;
  assert.deepEqual(await host.request(input), first);
  assert.equal(bucket.writes, writes);
  const result = await host.request(request("read", { required_visibility: first.visibility }));
  assert.deepEqual(result.records.map(payloadText), ["a", "b", "c"]);
  assert.deepEqual(result.records.map(x => x.lsn), ["1", "2", "3"]);
  await assert.rejects(host.request({ ...input, payloads: ["changed"] }), /WalIdempotencyConflict/);
});

test("native serverless artifacts publish and search through the WASM host", async () => {
  const { host } = await setup();
  const result = await host.request(request("mutate", { operation_id: "documents", timestamp_ns: "123", mutations: [
    { kind: "upsert", doc_id: "a", body: JSON.stringify({ text: "hello cloudflare" }) },
    { kind: "upsert", doc_id: "b", body: JSON.stringify({ text: "hello wasm" }) },
  ] }));
  assert.equal(result.committed, true);
  const published = await host.request(request("publish"));
  assert.equal(published.published, true);
  const search = await host.request(request("search", { text: "cloudflare" }));
  assert.deepEqual(search.hits.map(x => x.doc_id), ["a"]);
});

test("fresh lookup, text search and stable pages merge WAL mutations before publication", async () => {
  const { host } = await setup();
  const mutate = (id, mutations) => host.request(request("mutate", { operation_id: id, mutations }));
  const first = await mutate("initial", [
    { kind: "upsert", doc_id: "a", body: JSON.stringify({ text: "hello old", read: false }) },
    { kind: "upsert", doc_id: "b", body: JSON.stringify({ text: "hello old" }) },
  ]);
  const immediate = await host.request(request("lookup", { doc_id: "a", required_visibility: first.visibility }));
  assert.equal(JSON.parse(immediate.body).read, false);
  const page = await host.request(request("list", { limit: 1, required_visibility: first.visibility }));
  assert.equal(page.documents[0].doc_id, "a");
  await assert.rejects(host.request(request("list", { limit: 0 })), /QueryCandidateBudgetExceeded/);
  await assert.rejects(host.request(request("lookup", { cursor: page.cursor })), /QueryCursorCapabilityUnsupported/);
  assert.deepEqual((await host.request(request("search", { text: "hello", fresh: true }))).hits.map(x => x.doc_id), ["a", "b"]);
  await host.request(request("publish"));
  const changed = await mutate("changes", [
    { kind: "upsert", doc_id: "a", body: JSON.stringify({ text: "different", read: true }) },
    { kind: "delete", doc_id: "b" },
    { kind: "upsert", doc_id: "c", body: JSON.stringify({ text: "hello fresh" }) },
  ]);
  const fresh = await host.request(request("search", { text: "hello", required_visibility: changed.visibility }));
  assert.deepEqual(fresh.hits.map(x => x.doc_id), ["c"]);
  const read = await host.request(request("lookup", { doc_id: "a", required_visibility: changed.visibility }));
  assert.equal(JSON.parse(read.body).read, true);
  await host.request(request("publish"));
  const secondPage = await host.request(request("list", { limit: 1, cursor: page.cursor }));
  assert.equal(secondPage.documents[0].doc_id, "b", "resume original generation/cutoff despite later deletion");
  await assert.rejects(host.request(request("list", { limit: 2, cursor: page.cursor })), /QueryCursorBindingMismatch/);
});
test("bounded publications resume from durable watermarks", async () => {
  const { host } = await setup();
  const mutations = Array.from({ length: 300 }, (_, i) => ({ kind: "upsert", doc_id: `doc${String(i).padStart(4, "0")}`, body: JSON.stringify({ text: `bounded ${i}` }) }));
  await host.request(request("mutate", { operation_id: "bulk", mutations }));
  const first = await host.request(request("publish"));
  assert.equal(first.wal_end_lsn, "16");
  assert.equal(first.has_more, true);
  const second = await host.request(request("publish", {publication_records: 256}));
  assert.equal(second.wal_end_lsn, "272");
  const third = await host.request(request("publish", {publication_records: 256}));
  assert.equal(third.wal_end_lsn, "300");
  assert.equal(third.has_more, false);
});

test("immutable cache is bounded and never caches mutable commit records", async () => {
  const cache = createImmutableReadCache({ maxBytes: 4096, maxEntries: 2 });
  let calls = 0;
  const transport = cache.wrap(async () => { calls++; return { json: { etag: "v1" }, body: new Uint8Array(256) }; }, "scope");
  const key = `scope/wal-v1/${"a".repeat(64)}/pages/${"b".repeat(64)}`;
  await transport({ op: "get", bucket: "db", key });
  const cached = await transport({ op: "get", bucket: "db", key });
  assert.equal(calls, 1);
  cached.body[0] = 12;
  assert.equal((await transport({ op: "get", bucket: "db", key })).body[0], 0);
  await assert.rejects(transport({ op: "get", bucket: "db", key, max_response_bytes: 1 }), error => error.status === 4);
  await transport({ op: "get", bucket: "db", key: "scope/wal-v1/HEAD" });
  await transport({ op: "get", bucket: "db", key: "scope/wal-v1/HEAD" });
  assert.equal(calls, 3);
  assert.ok(cache.retainedBytes() <= 4096);
});

test("publication recovers from uploaded orphans and an ambiguous published HEAD", async () => {
  const { bucket, host } = await setup();
  await host.request(request("mutate", { operation_id: "initial", mutations: [
    { kind: "upsert", doc_id: "a", body: JSON.stringify({ text: "recoverable artifacts" }) },
  ] }));
  let failArtifact = true;
  const interrupted = (await setup(bucket, transport => async (req, body) => {
    const result = await transport(req, body);
    if (failArtifact && req.op === "put" && req.key.includes("/artifacts/")) {
      failArtifact = false;
      throw new Error("evicted after immutable upload");
    }
    return result;
  })).host;
  await assert.rejects(interrupted.request(request("publish")), /ObjectStoreTransportFailed/);
  let failHead = true;
  const ambiguous = (await setup(bucket, transport => async (req, body) => {
    const result = await transport(req, body);
    if (failHead && req.op === "put" && req.key.includes("/progress/") && req.key.endsWith("/HEAD") &&
      Number(new TextDecoder().decode(body).trim()) > 0) {
      failHead = false;
      throw new Error("lost publication response");
    }
    return result;
  })).host;
  await assert.rejects(ambiguous.request(request("publish")), /ObjectStoreTransportFailed/);
  const recovered = (await setup(bucket)).host;
  await recovered.request(request("publish"));
  assert.deepEqual((await recovered.request(request("search", { text: "recoverable" }))).hits.map(x => x.doc_id), ["a"]);
  assert.equal((await recovered.request(request("latest"))).visibility.slice(-16), "0000000000000001");
});


test("WAL reads authenticate immutable page bytes", async () => {
  const { bucket, host } = await setup();
  await host.request(append("one"));
  const head = JSON.parse(new TextDecoder().decode([...bucket.objects.entries()].find(([key]) => key.endsWith("/HEAD"))[1].body));
  const digest = Buffer.from(head.records.digest).toString("hex");
  const page = [...bucket.objects.entries()].find(([key]) => key.endsWith(`/${digest}`))[1];
  const saved = page.body.slice();
  page.body[page.body.length - 1] ^= 1;
  await assert.rejects(host.request(request("read")), /ArtifactIntegrityMismatch/);
  page.body = saved;
  assert.equal((await host.request(request("read"))).records.length, 1);
});


test("bounded hosted GC evicts orphan and expired pages, survives restarts and preserves receipts", async () => {
  const f = fixture(), bucket = new Bucket();
  try {
    let journal = f.journal();
    const base = createR2Transport({ database: bucket });
    const coordinator = { collection: async (op, args) => journal.rpc(op, args), async objectCall(req, body) {
      if (req.collection) journal.valid(req.collection);
      try {
        const result = await base(req, body);
        if (req.op === "put" && req.key.includes("/READ_PINS/")) f.storage.kv.put(req.key, { body: body.slice() });
        return { status: 0, ...result };
      } catch (error) { return { status: error.status ?? 7 }; }
    } };
    let host = (await setup(bucket, t => withCommitCoordinator(t, coordinator), { clock: f.now })).host;
    const execute = (input, token) => host.request(request(input.op, input), { transport: withCommitCoordinator(base, coordinator, token) });
    const first = request("mutate", { operation_id: "first", mutations: [{ kind: "upsert", doc_id: "a", body: JSON.stringify({ text: "old" }) }] });
    const receipt = await execute(first);
    for (let i = 0; i < 5; i++) {
      await execute(request("mutate", { operation_id: `change-${i}`, mutations: [{ kind: "upsert", doc_id: "a", body: JSON.stringify({ text: `current ${i}` }) }] }));
      await execute(request("publish"));
    }
    await execute(request("compact"));
    const before = bucket.objects.size;
    // Expire the native reader pins with the same host clock as the journal.
    f.tick(11 * 60 * 1000);
    const env = { ANTFLY_INCARNATION: "table", DATABASE: bucket, COMMITS: {getByName: () => coordinator} };
    let job, deleted = false;
    for (let i = 0; i < 500; i++) {
      const result = await collect(env, execute, job);
      job = result.job_id;
      if (result.delay_seconds) f.tick(result.delay_seconds * 1000 + 1);
      if (i % 4 === 0) { journal = f.journal(); host = (await setup(bucket, t => withCommitCoordinator(t, coordinator), { clock: f.now })).host; }
      if (result.done) { deleted = true; break; }
    }
    assert.ok(deleted, "bounded passes must finish");
    assert.ok(bucket.objects.size < before, `${bucket.objects.size} objects remain from ${before}`);
    assert.deepEqual(await execute(first), receipt);
    const current = await execute(request("lookup", { doc_id: "a" }));
    assert.equal(JSON.parse(current.body).text, "current 4");
    assert.deepEqual((await execute(request("search", { text: "current" }))).hits.map(hit => hit.doc_id), ["a"]);
  } finally { f.db.close(); }
});


test("a WAL epoch barrier rejects a writer staged before collection", async () => {
  const bucket = new Bucket(), reader = (await setup(bucket)).host;
  await reader.request(append("initial"));
  let interleave = true;
  const writer = (await setup(bucket, transport => async (req, body) => {
    const result = await transport(req, body);
    if (interleave && req.op === "put" && req.key.includes("/pages/")) {
      interleave = false;
      await reader.request(request("gc_wal", { gc_id: "a".repeat(32), gc_keep_from: "1" }));
    }
    return result;
  })).host;
  await writer.request(append("racing"));
  const records = await reader.request(request("read"));
  assert.deepEqual(records.records.map(payloadText), ["initial", "racing"]);
});

test("fresh metadata filters refill ranked hits and counts apply unique current mutations", async () => {
  const {host} = await setup();
  const change = (id, mutations) => host.request(request("mutate", {operation_id: id, mutations}));
  await change("base", [
    {kind: "upsert", doc_id: "a", body: JSON.stringify({text: "needle needle needle", state: {read: false}})},
    {kind: "upsert", doc_id: "b", body: JSON.stringify({text: "needle needle", state: {read: false}})},
    {kind: "upsert", doc_id: "c", body: JSON.stringify({text: "needle", state: {read: true}})},
  ]);
  assert.equal((await host.request(request("count"))).count, "3");
  await host.request(request("publish"));
  const committed = await change("updates", [
    {kind: "upsert", doc_id: "a", body: JSON.stringify({text: "needle", state: {read: true}})},
    {kind: "delete", doc_id: "b"},
    {kind: "upsert", doc_id: "d", body: JSON.stringify({text: "needle", state: {read: true}})},
    {kind: "upsert", doc_id: "d", body: JSON.stringify({text: "needle needle", state: {read: false}})},
  ]);
  const filters = [{field: "state.read", eq: true}];
  const hits = await host.request(request("search", {text: "needle", filters, limit: 2, required_visibility: committed.visibility}));
  assert.deepEqual(hits.hits.map(hit => hit.doc_id), ["a", "c"]);
  const page = await host.request(request("list", {filters, limit: 1}));
  assert.equal(page.documents[0].doc_id, "a");
  assert.equal((await host.request(request("list", {filters, limit: 1, cursor: page.cursor}))).documents[0].doc_id, "c");
  await assert.rejects(host.request(request("list", {filters: [{field: "state.read", eq: false}], limit: 1, cursor: page.cursor})), /QueryCursorBindingMismatch/);
  assert.equal((await host.request(request("count", {required_visibility: committed.visibility}))).count, "3");
  await host.request(request("publish"));
  assert.equal((await host.request(request("count"))).count, "3");
});

test("hosted enrichment resumes native progress and skips empty-text state", async () => {
  const {host} = await setup(); let calls = 0;
  const inference = async req => { calls++; return {json: {data: req.texts.map(() => Array(req.dimensions).fill(0.5))}}; };
  const publish = () => host.request(request("publish", {enrichment: true}));
  await host.request(request("mutate", {operation_id: "body", mutations: [{kind: "upsert", doc_id: "message/a", body: JSON.stringify({text: "immutable message body", owner: "alice"})}]}));
  await publish();
  const settings = request("enrich", {enrichment: true, embedding_dimensions: 4});
  assert.equal((await host.request(request("status"))).pending_embeddings, "1");
  await assert.rejects(host.request(request("search", {mode: "vector", vector: [0.5, 0.5, 0.5, 0.5], fresh: true})), /EmbeddingPending/);
  assert.equal((await host.request(settings, {inference})).wal_appends, 1);
  await publish();
  assert.equal((await host.request(settings, {inference})).wal_appends, 0);
  assert.equal(calls, 1);
  assert.equal((await host.request(request("status"))).pending_embeddings, "0");
  const vector = await host.request(request("search", {mode: "vector", vector: [0.5, 0.5, 0.5, 0.5], fresh: true}));
  assert.equal(vector.embeddings_ready, true);
  assert.deepEqual(vector.hits.map(hit => hit.doc_id), ["message/a"]);
  const body = JSON.parse((await host.request(request("lookup", {doc_id: "message/a"}))).body);
  assert.equal(body.owner, "alice"); assert.ok(body.chunk_embeddings.length);
  await host.request(request("mutate", {operation_id: "state", mutations: [{kind: "upsert", doc_id: "state/a", body: JSON.stringify({text: "", message_ref: "message/a", read: true})}]}));
  await publish(); await host.request(settings, {inference}); assert.equal(calls, 1);
});


test("a model response cannot overwrite a write committed during inference", async () => {
  const {host, bucket} = await setup(), writer = (await setup(bucket)).host;
  await host.request(request("mutate", {operation_id: "source", mutations: [{kind: "upsert", doc_id: "a", body: JSON.stringify({text: "old source"})}]}));
  await host.request(request("publish", {enrichment: true}));
  const inference = async req => {
    await writer.request(request("mutate", {operation_id: "new-source", mutations: [{kind: "upsert", doc_id: "a", body: JSON.stringify({text: "new source", custom: true})}]}));
    return {json: {data: req.texts.map(() => Array(req.dimensions).fill(0.5))}};
  };
  await assert.rejects(host.request(request("enrich", {enrichment: true, embedding_dimensions: 4}), {inference}), /EnrichmentProgressChanged/);
  const body = JSON.parse((await writer.request(request("lookup", {doc_id: "a"}))).body);
  assert.equal(body.text, "new source"); assert.equal(body.custom, true); assert.equal(body.chunk_embeddings, undefined);
});

test("publication backpressure preserves previously accepted retry receipts", async () => {
  const {host} = await setup();
  const batch = i => request("mutate", {operation_id: `lag/${i}`, mutations: Array.from({length: 1024}, (_, n) => ({kind: "upsert", doc_id: `${i}/${n}`, body: "{\"text\":\"tiny\"}"}))});
  const first = await host.request(batch(0));
  for (let i = 1; i < 4; i++) await host.request(batch(i));
  await assert.rejects(host.request(request("mutate", {operation_id: "over-limit", mutations: [{kind: "delete", doc_id: "absent"}]})), /PublicationLagExceeded/);
  assert.deepEqual(await host.request(batch(0)), first);
});

test("ranked text cursors preserve score and snapshot across publication", async () => {
  const {host} = await setup();
  await host.request(request("mutate", {operation_id: "ranked-initial", mutations: [
    {kind: "upsert", doc_id: "a", body: JSON.stringify({text: "needle needle"})},
    {kind: "upsert", doc_id: "b", body: JSON.stringify({text: "needle"})},
    {kind: "upsert", doc_id: "c", body: JSON.stringify({text: "needle"})},
  ]}));
  await host.request(request("publish"));
  const first = await host.request(request("search", {text: "needle", limit: 1, fresh: true}));
  assert.equal(first.hits[0].doc_id, "a"); assert.ok(first.cursor);
  await host.request(request("mutate", {operation_id: "ranked-change", mutations: [{kind: "delete", doc_id: "b"}]}));
  await host.request(request("publish"));
  const second = await host.request(request("search", {text: "needle", limit: 1, cursor: first.cursor}));
  assert.equal(second.hits[0].doc_id, "b");
  await assert.rejects(host.request(request("search", {text: "different", limit: 1, cursor: first.cursor})), /QueryCursorBindingMismatch/);
});

test("collection bootstraps without a published generation and public writes discard stale derived markers", async () => {
  const {host} = await setup();
  const plan = await host.request(request("gc_prepare"));
  assert.deepEqual(plan.roots, []); assert.equal(plan.floor, "0");
  await host.request(request("gc_wal", {gc_id: "b".repeat(32), gc_keep_from: "1"}));
  await host.request(request("mutate", {operation_id: "reserved", mutations: [{kind: "upsert", doc_id: "a", body: JSON.stringify({text: "changed", owner: "alice", chunk_embeddings: [1], _enrichment: {chunk_embeddings_version: 1}})}]}));
  const body = JSON.parse((await host.request(request("lookup", {doc_id: "a"}))).body);
  assert.equal(body.owner, "alice"); assert.equal(body.chunk_embeddings, undefined); assert.equal(body._enrichment, undefined);
});

test("native hosted enrichment batches multi-chunk documents in one model request", async () => {
  const {host} = await setup(); let calls = 0;
  await host.request(request("mutate", {operation_id: "long-message", mutations: [{kind: "upsert", doc_id: "message/long", body: JSON.stringify({text: Array.from({length: 80}, (_, i) => `word${i}`).join(" ")})}]}));
  await host.request(request("publish", {enrichment: true}));
  const inference = async req => { calls++; assert.equal(req.texts.length, 10); return {json: {data: req.texts.map(() => Array(req.dimensions).fill(0.5))}}; };
  const result = await host.request(request("enrich", {enrichment: true, embedding_dimensions: 4}), {inference});
  assert.equal(result.wal_appends, 1); assert.equal(calls, 1);
  await host.request(request("publish", {enrichment: true}));
  const body = JSON.parse((await host.request(request("lookup", {doc_id: "message/long"}))).body);
  assert.equal(body.chunk_embeddings.length, 10);
  assert.equal((await host.request(request("status"))).pending_embeddings, "0");
});

test("cold flat-index publication reuses the authenticated packed snapshot instead of hydrating every old body", async t => {
  const {host, bucket} = await setup();
  await host.request(request("mutate", {operation_id: "cold-rebuild", mutations: Array.from({length: 300}, (_, i) => ({kind: "upsert", doc_id: `doc/${String(i).padStart(4, "0")}`, body: JSON.stringify({text: `cold snapshot body ${i}`})}))}));
  await host.request(request("publish", {publication_records: 256}));
  const reopened = (await setup(bucket)).host;
  bucket.reads = 0; bucket.readBytes = 0;
  await reopened.request(request("publish", {publication_records: 256}));
  assert.ok(bucket.reads < 64, `${bucket.reads} GETs for a 256-document base`);
  t.diagnostic(`cold publication of 44 mutations over 256 documents: ${bucket.reads} GETs, ${bucket.readBytes} bytes`);
  assert.equal((await reopened.request(request("count"))).count, "300");
});

test("host metrics preserve accepted outcomes and account native work separately from linear memory", async () => {
  const metrics = [];
  const {host} = await setup(new Bucket(), undefined, {onMetrics: sample => { metrics.push(sample); throw new Error("observer failed"); }});
  const mutation = request("mutate", {operation_id: "measured", mutations: [{kind: "upsert", doc_id: "a", body: '{"text":"measured body"}'}]});
  const accepted = await host.request(mutation);
  assert.equal((await host.request(mutation)).visibility, accepted.visibility);
  assert.equal(metrics[0].outcome, "ok");
  assert.ok(metrics[0].operations > 0);
  assert.ok(metrics[0].transferred_bytes > 0);
  assert.ok(metrics[0].working_set_peak_bytes > 0);
  assert.ok(metrics[0].working_set_peak_bytes <= 20 * 1024 * 1024);
  assert.ok(metrics[0].wasm_linear_bytes <= 32 * 1024 * 1024);
  assert.ok(metrics[0].elapsed_ms >= metrics[0].io_wait_ms);
  await host.request(request("latest"));
  assert.equal(metrics.at(-1).working_set_peak_bytes, 0, "WAL-only requests must not report a previous request's peak");
  await assert.rejects(host.request(request("lookup", {required_visibility: "invalid"})), /InvalidVisibilityToken/);
  assert.equal(metrics.at(-1).outcome, "error");
  assert.equal(host.usable(), true);
});

test("native compaction enforces hosted admission, survives an ambiguous publication and preserves facts and pending work", async () => {
  const {host, bucket} = await setup();
  assert.deepEqual(await host.request(request("compact")), {published: false, version: "0", artifact_count: 0});
  const mutation = request("mutate", {operation_id: "compact-source", mutations: ["a", "b"].map(doc_id => ({kind: "upsert", doc_id, body: JSON.stringify({text: `compact ${doc_id}`, owner: "alice"})}))});
  const receipt = await host.request(mutation);
  await host.request(request("publish", {enrichment: true}));
  const before = await host.request(request("status"));
  await assert.rejects(host.request(request("compact", {compaction_max_documents: 1})), /CompactionBudgetExceeded/);
  await assert.rejects(host.request(request("compact", {compaction_max_artifact_bytes: 1})), /CompactionBudgetExceeded/);
  assert.deepEqual(await host.request(request("status")), before);
  let lose = true;
  const writer = (await setup(bucket, transport => async (req, body) => {
    const response = await transport(req, body);
    if (lose && req.op === "put" && req.key.includes("/progress/") && req.key.endsWith("/HEAD") && Number(new TextDecoder().decode(body).trim()) > 1) {
      lose = false; throw new Error("lost compaction response after CAS");
    }
    return response;
  })).host;
  await assert.rejects(writer.request(request("compact")), /ObjectStoreTransportFailed/);
  assert.equal(lose, false, "fixture must cross the compactor's commit boundary");
  const reopened = (await setup(bucket)).host;
  assert.equal((await reopened.request(request("status"))).pending_embeddings, "2");
  assert.equal((await reopened.request(request("count", {required_visibility: receipt.visibility}))).count, "2");
  assert.equal((await reopened.request(request("search", {text: "compact", filters: [{field: "owner", eq: "alice"}]}))).hits.length, 2);
  assert.equal((await reopened.request(mutation)).visibility, receipt.visibility);
  assert.equal((await reopened.request(request("compact"))).published, false, "retry should reuse the already compacted head");
  const enriched = await reopened.request(request("enrich", {enrichment: true, embedding_dimensions: 4}), {inference: async req => ({json: {data: req.texts.map(() => [1, 0, 0, 0])}})});
  assert.equal(enriched.wal_appends, 1, "native pending work must remain runnable after compaction");
});

test("working-set rejection preserves the accepted WAL and leaves the instance reusable", async () => {
  const {host} = await setup();
  const mutation = request("mutate", {operation_id: "bounded-source", mutations: [{kind: "upsert", doc_id: "a", body: JSON.stringify({text: "bounded publication ".repeat(200)})}]});
  const accepted = await host.request(mutation);
  await assert.rejects(host.request(request("publish", {working_set_bytes: 4096})), /HostedWorkingSetBudgetExceeded/);
  assert.equal(host.usable(), true);
  assert.equal((await host.request(mutation)).visibility, accepted.visibility);
  await host.request(request("publish"));
  assert.equal((await host.request(request("count", {required_visibility: accepted.visibility}))).count, "1");
});

test("clock rollback fails closed and retires the lane so fresh compute can recover", async () => {
  let now = 10000;
  const bucket = new Bucket();
  const host = (await setup(bucket, undefined, {clock: () => now})).host;
  const mutation = request("mutate", {operation_id: "clock-source", mutations: [{kind: "upsert", doc_id: "a", body: '{"text":"clock recovery"}'}]});
  const accepted = await host.request(mutation);
  await host.request(request("publish"));
  now = 9000;
  await assert.rejects(host.request(request("status")), /ManifestReadLeaseExpired|WorkLeaseLost/);
  assert.equal(host.usable(), false);
  now = 10001;
  await assert.rejects(host.request(request("count")), /instance is unusable/);
  const recovered = (await setup(bucket, undefined, {clock: () => now})).host;
  assert.equal((await recovered.request(mutation)).visibility, accepted.visibility);
  assert.equal((await recovered.request(request("count"))).count, "1");
});

test("hosted embedding windows preserve chunk order and never publish partially completed model output", async () => {
  const {host} = await setup();
  const text = Array.from({length: 70 * 8}, (_, i) => `word${i}`).join(" ");
  await host.request(request("mutate", {operation_id: "window-source", mutations: [{kind: "upsert", doc_id: "large", body: JSON.stringify({text})}]}));
  await host.request(request("publish", {enrichment: true}));
  const settings = request("enrich", {enrichment: true, embedding_dimensions: 4});
  let calls = 0, position = 0;
  const inference = async req => {
    calls++;
    if (calls === 2) throw new Error("second window failed");
    return {json: {data: req.texts.map(() => [position++, 0, 0, 1])}};
  };
  await assert.rejects(host.request(settings, {inference}), /HostedEmbeddingFailed/);
  assert.equal((await host.request(request("status"))).pending_embeddings, "1");
  assert.equal(JSON.parse((await host.request(request("lookup", {doc_id: "large"}))).body).chunk_embeddings, undefined);
  calls = 0; position = 0;
  const success = await host.request(settings, {inference: async req => {
    calls++; assert.ok(req.texts.length <= 64);
    return {json: {data: req.texts.map(() => [position++, 0, 0, 1])}};
  }});
  assert.equal(success.wal_appends, 1);
  assert.ok(calls > 1);
  const body = JSON.parse((await host.request(request("lookup", {doc_id: "large"}))).body);
  assert.equal(body.chunk_embeddings.length, position);
  assert.deepEqual(body.chunk_embeddings.map(chunk => chunk.embedding[0]), Array.from({length: position}, (_, i) => i));
});
