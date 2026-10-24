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
import { readFile } from "node:fs/promises";
import { instantiateHost } from "../../../zig/lib/objectstore/wasm/asyncify.mjs";
import { createImmutableReadCache } from "../../../zig/lib/objectstore/wasm/cache.mjs";
import { createR2Transport, ObjectStoreError } from "../../../zig/lib/objectstore/wasm/r2.mjs";
const module = await WebAssembly.compile(await readFile(new URL("../dist/antfly-serverless.wasm", import.meta.url)));
class Bucket {
  objects = new Map(); revision = 0; writes = 0; readBytes = 0; reads = 0;
  metadata(key, object) { return { key, size: object.body.length, etag: object.etag, uploaded: new Date(0), httpMetadata: object.httpMetadata ?? {} }; }
  async put(key, body, options = {}) {
    await Promise.resolve();
    const old = this.objects.get(key), condition = options.onlyIf;
    if ((condition?.etagDoesNotMatch === "*" && old) || (condition?.etagMatches != null && old?.etag !== condition.etagMatches)) return null;
    const value = { body: body.slice(), etag: `e${++this.revision}`, httpMetadata: options.httpMetadata };
    this.objects.set(key, value); this.writes++;
    return this.metadata(key, value);
  }
  async get(key, options = {}) {
    await Promise.resolve(); this.reads++;
    const value = this.objects.get(key);
    if (!value) return null;
    const meta = this.metadata(key, value);
    if (options.onlyIf?.etagMatches != null && value.etag !== options.onlyIf.etagMatches) return meta;
    const offset = options.range?.offset ?? 0;
    const data = value.body.slice(offset, options.range?.length == null ? undefined : offset + options.range.length);
    this.readBytes += data.length;
    return { ...meta, range: options.range ? { offset, length: data.length } : undefined,
      body: new ReadableStream({ start(controller) { controller.enqueue(data); controller.close(); } }) };
  }
  async head(key) { const value = this.objects.get(key); return value ? this.metadata(key, value) : null; }
  async delete(key) { this.objects.delete(key); }
  async list(options = {}) {
    const keys = [...this.objects.keys()].sort().filter(key => key.startsWith(options.prefix ?? "") && (!options.startAfter || key > options.startAfter));
    const start = options.cursor ? Number(options.cursor) : 0;
    const selected = keys.slice(start, start + (options.limit ?? 1000));
    const truncated = start + selected.length < keys.length;
    return { objects: selected.map(key => this.metadata(key, this.objects.get(key))), truncated, cursor: truncated ? String(start + selected.length) : undefined };
  }
}
const payloadText = record => Buffer.from(record.payload_base64, "base64").toString("utf8");
const request = (op, args = {}) => ({ op, bucket: "database", prefix: "scope", namespace: "tenant/table/incarnation1", ...args });
const append = (id, payload = id) => request("append", { operation_id: id, payload, timestamp_ns: "9007199254740993" });
async function setup(bucket = new Bucket(), wrap = transport => transport) {
  const transport = createR2Transport({ database: bucket });
  return { bucket, transport, host: await instantiateHost(module, wrap(transport), { onLog: message => console.error("WASM:", message) }) };
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
  assert.equal(first.wal_end_lsn, "256");
  assert.equal(first.has_more, true);
  const second = await host.request(request("publish"));
  assert.equal(second.wal_end_lsn, "300");
  assert.equal(second.has_more, false);
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
  const page = [...bucket.objects.entries()].find(([key]) => key.endsWith(`/pages/${digest}`))[1];
  const saved = page.body.slice();
  page.body[page.body.length - 1] ^= 1;
  await assert.rejects(host.request(request("read")), /ArtifactIntegrityMismatch/);
  page.body = saved;
  assert.equal((await host.request(request("read"))).records.length, 1);
});
