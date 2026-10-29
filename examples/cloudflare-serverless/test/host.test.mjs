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
test("bounded point reads use eight MiB kernels and promote large bodies without changing results",async()=>{
 const {RequestPool}=await import("../request-pool.mjs"),{withPointReadLane}=await import("../point-read-lane.mjs"),small=await WebAssembly.compile(await readFile(new URL("../dist/antfly-serverless-observe.wasm",import.meta.url))),compact=await WebAssembly.compile(await readFile(new URL("../dist/antfly-serverless-compact.wasm",import.meta.url)));
 const {host,transport}=await setup(),bodies={small:JSON.stringify({text:"small",payload:"x".repeat(1024)}),large:JSON.stringify({text:"large",payload:"x".repeat(300*1024)})};
 const receipt=await host.request(request("mutate",{operation_id:"point-lane",mutations:Object.entries(bodies).map(([doc_id,body])=>({kind:"upsert",doc_id,body}))}));await host.request(request("publish"));
 // The coordinator creates the durable generation pin before native admission.
 await host.request(request("lookup",{doc_id:"small",required_visibility:receipt.visibility}));
 const variants=[],metrics=[],pool=new RequestPool(job=>{variants.push(job.hostKey);return instantiateHost(job.hostKey==="observe"?small:compact,null);},4,{defaultKernelVariant:"compact",maxReservedBytes:32*1024*1024,reservationBytes:16*1024*1024,recycleBytes:job=>(job.hostKey==="observe"?7:12)*1024*1024});let promotions=0;
 const lookup=id=>withPointReadLane(pool,lane=>lane.request(request("lookup",{doc_id:id,required_visibility:receipt.visibility}),{transport,onMetrics:m=>metrics.push(m)}),{onPromote:()=>promotions++});
 assert.equal((await lookup("small")).body,bodies.small);assert.equal(promotions,0);assert.deepEqual(variants,["observe"]);assert.ok(metrics[0].wasm_linear_bytes<=8*1024*1024);
 assert.equal((await lookup("large")).body,bodies.large);assert.equal(promotions,1);assert.ok(variants.includes("compact"));assert.equal(pool.reservedBytes,0);assert.ok(pool.memoryBytes()<=32*1024*1024);
});
test("point read capacity promotion preserves errors and admits four bounded lanes",async()=>{
 const {RequestPool}=await import("../request-pool.mjs"),{withPointReadLane}=await import("../point-read-lane.mjs");
 let release,active=0,peak=0;const held=new Promise(resolve=>release=resolve),pool=new RequestPool(async()=>({usable:()=>true,memoryBytes:()=>0,request:async()=>{peak=Math.max(peak,++active);await held;active--;return{};}}),4,{maxReservedBytes:32*1024*1024,reservationBytes:16*1024*1024,defaultKernelVariant:"compact"});
 const work=Array.from({length:4},()=>withPointReadLane(pool,host=>host.request({op:"lookup"})));try{const end=Date.now()+2000;while(peak<4&&Date.now()<end)await new Promise(resolve=>setTimeout(resolve,1));assert.equal(peak,4);assert.equal(pool.reservedBytes,32*1024*1024);}finally{release();await Promise.allSettled(work);}assert.equal(pool.reservedBytes,0);
 for(const message of["OutOfMemory","WASM allocation failed","InvalidDocumentFactsRoot","ObjectStoreTransportFailed","DeadlineExceeded","PointReadWriteForbidden"]){let calls=0;const error=Error(message),fake={withHost:async run=>{calls++;return run({request:async()=>{throw error;}});}};await assert.rejects(withPointReadLane(fake,host=>host.request({op:"lookup"})),e=>e===error);assert.equal(calls,1);}
 const signal=new AbortController().signal,deadlineMs=Date.now()+5000,input={op:"lookup",required_visibility:"original-fence",working_set_bytes:8*1024*1024};
 for(const failure of["ObjectIoBudgetExceeded","InvalidDocumentFactsRoot"]){let calls=0;const fake={withHost:async(run,options)=>{calls++;assert.equal(options.signal,signal);assert.equal(options.deadlineMs,deadlineMs);return run({request:async(actual,opts)=>{if(options.kernelVariant!=="observe"){assert.deepEqual(actual,input);return"promoted";}try{await opts.transport({op:"get"});}catch{throw Error(failure);}}});}},execute=host=>host.request(input,{transport:async()=>({body:new Uint8Array(129*1024)})});
  if(failure==="ObjectIoBudgetExceeded"){assert.equal(await withPointReadLane(fake,execute,{signal,deadlineMs}),"promoted");assert.equal(calls,2);}else{await assert.rejects(withPointReadLane(fake,execute,{signal,deadlineMs}),new RegExp(failure));assert.equal(calls,1);}
 }
 let lowBudgetCalls=0;const lowBudget={withHost:async run=>{lowBudgetCalls++;return run({request:async actual=>{assert.equal(actual.working_set_bytes,65536);throw Error("HostedWorkingSetBudgetExceeded");}});}};await assert.rejects(withPointReadLane(lowBudget,host=>host.request({...input,working_set_bytes:65536})),/HostedWorkingSetBudgetExceeded/);assert.equal(lowBudgetCalls,1);
});
async function setup(bucket = new Bucket(), wrap = transport => transport, options = {}) {
  const transport = createR2Transport({ database: bucket }, {maxPutBytes: 4 * 1024 * 1024});
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

for(const mode of ['deadline','abort'])test(`suspended host ${mode} returns before storage settles and drains auxiliary I/O`,{timeout:3000},async()=>{
 let release,releaseAux,entered,timer;const held=new Promise(r=>release=r),auxiliary=new Promise(r=>releaseAux=r),started=new Promise(r=>entered=r),metrics=[];
 const {host,bucket}=await setup(undefined,raw=>async(req,body)=>{entered();await held;return raw(req,body);});const controller=new AbortController();
 const work=host.request(request('latest'),{signal:controller.signal,deadlineMs:Date.now()+(mode==='deadline'?25:2000),drainTransport:()=>auxiliary,onMetrics:m=>metrics.push(m)}).then(value=>({value}),error=>({error}));await started;if(mode==='abort')controller.abort();
 try{
  const result=await Promise.race([work,new Promise(r=>timer=setTimeout(()=>r({late:true}),500))]);assert.equal(result.late,undefined,'caller must finish while underlying storage is still suspended');assert.match(result.error.message,mode==='deadline'?/DeadlineExceeded/:/Canceled/);assert.equal(host.usable(),false);assert.equal(bucket.writes,0);
  const draining=host.draining();assert.ok(draining);assert.equal(result.error.ioSettled,draining);release();await new Promise(r=>setTimeout(r,0));assert.ok(host.draining(),'all auxiliary prefetch must settle before releasing the reservation');releaseAux();await draining;assert.equal(host.draining(),null);assert.equal(metrics.length,1);assert.equal(metrics[0].failure_code,mode==='deadline'?'DeadlineExceeded':'Canceled');assert.equal(metrics[0].io_draining,true);
 }finally{clearTimeout(timer);release();releaseAux();await work;await host.draining();}
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
  assert.equal(first.consumed_records,16);
  assert.equal(first.has_more, true);
  const second = await host.request(request("publish", {publication_records: 256}));
  assert.equal(second.wal_end_lsn, "272");
  assert.equal(second.consumed_records,256);
  const third = await host.request(request("publish", {publication_records: 256}));
  assert.equal(third.wal_end_lsn, "300");
  assert.equal(third.consumed_records,28);
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

for (const paged of [false, true]) test(`publication recovers from uploaded orphans and an ambiguous published HEAD (paged=${paged})`, async () => {
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
  await assert.rejects(interrupted.request(request("publish", {paged_text: paged})), /ObjectStoreTransportFailed/);
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
  await assert.rejects(ambiguous.request(request("publish", {paged_text: paged})), /ObjectStoreTransportFailed/);
  const recovered = (await setup(bucket)).host;
  await recovered.request(request("publish", {paged_text: paged}));
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


for (const paged of [false, true, "projections"]) test(`bounded hosted GC evicts orphan and expired pages, survives restarts and preserves receipts (paged=${paged})`, async () => {
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
    const execute = (input, token) => host.request(request(input.op, {...input, paged_text: Boolean(paged), paged_projections: paged === "projections"}), { transport: withCommitCoordinator(base, coordinator, token) });
    const first = request("mutate", { operation_id: "first", mutations: [{ kind: "upsert", doc_id: "a", body: JSON.stringify({ text: "old" }) }] });
    const receipt = await execute(first);
    for (let i = 0; i < 5; i++) {
      await execute(request("mutate", { operation_id: `change-${i}`, mutations: [{ kind: "upsert", doc_id: "a", body: JSON.stringify({ text: `current ${i}`, ...(paged === "projections" ? {embedding: [1, i+1], sparse_embedding: {current: i+1}} : {}) }) }] }));
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
    if (paged === "projections") {
      assert.equal((await execute(request("search", {mode: "vector", vector: [1,5]}))).hits[0].doc_id, "a");
      const exact = await execute(request("search", {mode: "vector", vector: [1,5], limit: 1, search_effort: 1, exact_scan: true}));
      assert.equal(exact.complete, true); assert.equal(exact.hits[0].doc_id, "a");
      assert.ok(exact.residual_fetches > 0, "collection must retain original-value residual packs");
      assert.equal((await execute(request("search", {mode: "sparse", sparse: [{term: "current", weight: 1}]}))).hits[0].doc_id, "a");
    }
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


for (const checkpoint of [false, true]) test(`a model response cannot overwrite a write committed during inference (checkpoints=${checkpoint})`, async () => {
  const {host, bucket} = await setup(), writer = (await setup(bucket)).host;
  await host.request(request("mutate", {operation_id: "source", mutations: [{kind: "upsert", doc_id: "a", body: JSON.stringify({text: "old source"})}]}));
  await host.request(request("publish", {enrichment: true}));
  const inference = async req => {
    await writer.request(request("mutate", {operation_id: "new-source", mutations: [{kind: "upsert", doc_id: "a", body: JSON.stringify({text: "new source", custom: true})}]}));
    return {json: {data: req.texts.map(() => Array(req.dimensions).fill(0.5))}};
  };
  await assert.rejects(host.request(request("enrich", {enrichment: true, embedding_dimensions: 4, embedding_checkpoints: checkpoint}), {inference}), /EnrichmentProgressChanged/);
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


test("text-only publication replaces postings without hydrating unrelated large bodies", async () => {
  const {host, bucket} = await setup();
  for (let offset = 0; offset < 96; offset += 4) {
    await host.request(request("mutate", {operation_id: `large/${offset}`, mutations: Array.from({length: 4}, (_, i) => ({kind: "upsert", doc_id: `doc/${String(offset+i).padStart(3,"0")}`, body: JSON.stringify({text: `durable message ${offset+i}`, padding: "x".repeat(32768)})}))}));
    await host.request(request("publish"));
  }
  assert.equal((await host.request(request("count"))).count, "96");
  const cold = (await setup(bucket)).host;
  await cold.request(request("mutate", {operation_id: "edit-delete", mutations: [{kind: "upsert", doc_id: "doc/000", body: '{"text":"replacement unique"}'}, {kind: "delete", doc_id: "doc/001"}]}));
  bucket.readBytes = 0;
  await cold.request(request("publish"));
  assert.ok(bucket.readBytes < 384 * 1024, `${bucket.readBytes} bytes read for a small edit over 3 MiB of unrelated bodies`);
  assert.equal((await cold.request(request("search", {text: "replacement"}))).hits[0].doc_id, "doc/000");
  assert.equal((await cold.request(request("count"))).count, "95");
});

test("paged text migrates a flat generation, preserves cursors and updates bounded paths", async () => {
  const {host, bucket} = await setup();
  await host.request(request("mutate", {operation_id: "paged-base", mutations: Array.from({length: 200}, (_, i) => ({kind: "upsert", doc_id: `doc/${String(i).padStart(4,"0")}`, body: JSON.stringify({text: "durable searchable", keep: true})}))}));
  await host.request(request("publish", {publication_records: 256}));
  const first = await host.request(request("search", {text: "durable", limit: 2}));
  await host.request(request("mutate", {operation_id: "paged-edit", mutations: [{kind: "upsert", doc_id: "doc/0000", body: '{"text":"replacement"}'}, {kind: "delete", doc_id: "doc/0001"}, {kind: "upsert", doc_id: "new", body: '{"text":"durable new"}'}]}));
  await host.request(request("publish", {paged_text: true}));
  let reopened = (await setup(bucket)).host;
  assert.deepEqual((await reopened.request(request("search", {text: "durable", limit: 2, cursor: first.cursor}))).hits.map(hit => hit.doc_id), ["doc/0002", "doc/0003"]);
  assert.equal((await reopened.request(request("count"))).count, "200");
  assert.equal((await reopened.request(request("search", {text: "replacement"}))).hits[0].doc_id, "doc/0000");
  assert.equal((await reopened.request(request("compact", {compaction_max_documents: 1}))).published, false, "paged-only compaction must not reconstruct a corpus snapshot");
  await reopened.request(request("mutate", {operation_id: "paged-next", mutations: [{kind: "upsert", doc_id: "later", body: '{"text":"durable later"}'}]}));
  bucket.readBytes = 0;
  await reopened.request(request("publish"));
  assert.ok(bucket.readBytes < 128 * 1024, `paged update read ${bucket.readBytes} bytes`);
  reopened = (await setup(bucket)).host;
  assert.equal((await reopened.request(request("lookup", {doc_id: "later"}))).body, '{"text":"durable later"}');
});

test("embedding checkpoints resume beyond eight model windows across eviction", async () => {
  const bucket = new Bucket();
  let host = (await setup(bucket)).host, calls = 0;
  const source = Array.from({length: 600 * 8}, (_, i) => `word${i}`).join(" ");
  await host.request(request("mutate", {operation_id: "checkpoint-source", mutations: [{kind: "upsert", doc_id: "large", body: JSON.stringify({text: source})}]}));
  await host.request(request("publish", {enrichment: true}));
  const settings = request("enrich", {enrichment: true, embedding_dimensions: 4, embedding_checkpoints: true, embedding_identity: "model-v1", embedding_windows: 1});
  let complete = false, yielded = 0;
  for (let step = 0; step < 20; step++) {
    const result = await host.request(settings, {inference: async req => {calls++; return {json: {data: req.texts.map(text => [Number(text.match(/word(\d+)/)[1]), 0, 0, 1])}}; }});
    assert.ok(result.provider_calls <= 1);
    if (!result.yielded) {assert.equal(result.wal_appends, 1); complete = true; break;}
    yielded++;
    assert.equal((await host.request(request("status"))).pending_embeddings, "1");
    assert.equal(JSON.parse((await host.request(request("lookup", {doc_id: "large"}))).body).chunk_embeddings, undefined);
    host = (await setup(bucket)).host;
  }
  assert.ok(complete); assert.equal(calls, 10); assert.equal(yielded, 9);
  const body = JSON.parse((await host.request(request("lookup", {doc_id: "large"}))).body);
  assert.equal(body.chunk_embeddings.length, 600);
  assert.deepEqual(body.chunk_embeddings.map(chunk => chunk.embedding[0]), Array.from({length: 600}, (_, i) => i * 8));
  await host.request(request("publish", {enrichment: true}));
  assert.equal((await host.request(request("status"))).pending_embeddings, "0");
});

test("corrupt embedding checkpoints fail closed and missing scratch recomputes safely", async () => {
  const {host, bucket} = await setup(); let calls = 0;
  await host.request(request("mutate", {operation_id: "cache-source", mutations: [{kind: "upsert", doc_id: "a", body: JSON.stringify({text: Array.from({length: 70 * 8}, (_, i) => `word${i}`).join(" ")})}]}));
  await host.request(request("publish", {enrichment: true}));
  const settings = request("enrich", {enrichment: true, embedding_dimensions: 4, embedding_checkpoints: true, embedding_identity: "model-v1"});
  const inference = async req => {calls++; return {json: {data: req.texts.map(() => [1, 0, 0, 0])}};};
  assert.equal((await host.request(settings, {inference})).yielded, true);
  const key = [...bucket.objects.keys()].find(key => key.includes("/embedding-cache/"));
  bucket.objects.get(key).body[48] ^= 1;
  await assert.rejects(host.request(settings, {inference}), /HostedEmbeddingCheckpointInvalid/);
  assert.equal(calls, 1);
  assert.equal(JSON.parse((await host.request(request("lookup", {doc_id: "a"}))).body).chunk_embeddings, undefined);
  await bucket.delete(key);
  assert.equal((await host.request(settings, {inference})).yielded, true);
  assert.equal((await host.request(settings, {inference})).wal_appends, 1);
  assert.equal(calls, 3);
});

test("publication overlaps immutable body uploads without bypassing operation admission", async () => {
  let calls = 0, active = 0, peak = 0, sample;
  const {host} = await setup(new Bucket(), transport => async (req, body) => {
    calls++;
    if (req.op === "put" && req.key.includes("/artifacts/graph/")) {
      peak = Math.max(peak, ++active);
      try { await new Promise(resolve => setTimeout(resolve, 2)); return await transport(req, body); }
      finally { active--; }
    }
    return transport(req, body);
  });
  await host.request(request("mutate", {operation_id: "parallel-bodies", mutations: Array.from({length: 16}, (_, i) => ({kind: "upsert", doc_id: String(i), body: JSON.stringify({text: "durable " + i})}))}));
  calls = 0;
  await host.request(request("publish", {paged_text: true}), {onMetrics: value => {sample = value;}});
  assert.ok(peak > 1 && peak <= 4, "immutable bodies should upload concurrently within the connection allowance");
  assert.equal(active, 0); assert.equal(sample.operations, calls);
  assert.equal((await host.request(request("count"))).count, "16");
});

test("paged dense and sparse projections preserve scoring, cold edits and deletes", async () => {
  const flat = await setup(), paged = await setup();
  const records = Array.from({length: 160}, (_, i) => ({kind: "upsert", doc_id: `doc/${String(i).padStart(3,"0")}`, body: JSON.stringify({text: "searchable", embedding: [Math.cos(i*0.1), Math.sin(i*0.1)], sparse_embedding: {Alpha: i%7+0.5, alpha: 0.25, beta: i%3+1}})}));
  for (const [profile, enabled] of [[flat, false], [paged, true]]) {
    await profile.host.request(request("mutate", {operation_id: "projection-base", mutations: records}));
    while ((await profile.host.request(request("publish", {publication_records: 32, paged_projections: enabled}))).has_more) {}
  }
  const vector = {mode: "vector", vector: [1,0], search_effort: 1, num_probes: 256, limit: 10};
  const sparse = {mode: "sparse", sparse: [{term: "ALPHA", weight: 1.3}, {term: "beta", weight: 0.2}], limit: 10};
  const identities = result => result.hits.map(hit => [hit.doc_id, hit.score]);
  assert.deepEqual(identities(await paged.host.request(request("search", vector))), identities(await flat.host.request(request("search", vector))));
  assert.deepEqual(identities(await paged.host.request(request("search", sparse))), identities(await flat.host.request(request("search", sparse))));
  const cold = (await setup(paged.bucket)).host;
  await cold.request(request("mutate", {operation_id: "projection-edit", mutations: [{kind: "delete", doc_id: "doc/000"}, {kind: "upsert", doc_id: "doc/001", body: '{"text":"changed","embedding":[1,0],"sparse_embedding":{"alpha":100}}'}]}));
  await cold.request(request("publish")); // Existing paged roots remain incremental with the flag omitted.
  assert.equal((await cold.request(request("count"))).count, "159");
  assert.equal((await cold.request(request("search", vector))).hits[0].doc_id, "doc/001");
  assert.equal((await cold.request(request("search", sparse))).hits[0].doc_id, "doc/001");
  assert.equal((await cold.request(request("compact", {compaction_max_documents: 1}))).published, false);
});

test("adaptive vector projection shards split and remain writable after cold reopen", async () => {
  const {host, bucket} = await setup();
  for (let offset = 0; offset < 1500; offset += 100) {
    await host.request(request("mutate", {operation_id: `shards/${offset}`, mutations: Array.from({length: 100}, (_, n) => {
      const i = offset+n;
      return {kind: "upsert", doc_id: `doc/${String(i).padStart(5,"0")}`, body: JSON.stringify({text: "large corpus", embedding: i === 0 ? [1,0,0,0] : [0,1,i/1500,0]})};
    })}));
    while ((await host.request(request("publish", {publication_records: 64, paged_projections: true}))).has_more) {}
  }
  let cold = (await setup(bucket)).host;
  assert.equal((await cold.request(request("count"))).count, "1500");
  const query = {mode: "vector", vector: [1,0,0,0], num_probes: 256, search_effort: 1};
  assert.equal((await cold.request(request("search", query))).hits[0].doc_id, "doc/00000");
  await cold.request(request("mutate", {operation_id: "shards/replace", mutations: [{kind: "delete", doc_id: "doc/00000"}, {kind: "upsert", doc_id: "needle", body: '{"text":"new","embedding":[1,0,0,0]}'}]}));
  await cold.request(request("publish")); cold = (await setup(bucket)).host;
  assert.equal((await cold.request(request("search", query))).hits[0].doc_id, "needle");
  assert.equal((await cold.request(request("count"))).count, "1500");
});

test("reindex checkpoints survive cold instances, catch up live WAL and acknowledge publication once", async () => {
  const bucket = new Bucket();
  const transport = createR2Transport({database: bucket}, {maxPutBytes: 4 * 1024 * 1024});
  let loseHeadResponse = false, headThreshold = Infinity, failCheckpoint = false;
  let now = Date.now();
  const coordinator = {async objectCall(request, body) {
    if (failCheckpoint && request.op === "put" && request.key.endsWith("/REINDEX")) {failCheckpoint = false; return {status: 3};}
    if (request.guard_key != null && bucket.objects.get(request.guard_key)?.etag !== request.guard_etag) return {status: 2};
    const {guard_key, guard_etag, ...plain} = request;
    try {
      const result = await transport(plain, body);
      if (loseHeadResponse && request.op === "put" && request.key.endsWith("/HEAD") && request.content_type) {
        // Drop only the visible reindex publication, after it commits.
        const encoded = request.content_type.split("base64url=")[1];
        if (encoded && JSON.parse(Buffer.from(encoded, "base64url")).head_version > headThreshold) {loseHeadResponse = false; throw new Error("lost HEAD response");}
      }
      return {status: 0, ...result};
    } catch (error) {if (error.status) return {status: error.status}; throw error;}
  }};
  const reopen = async () => (await setup(bucket, t => withCommitCoordinator(t, coordinator), {clock: () => now})).host;
  let host = await reopen();
  const docs = Array.from({length: 45}, (_, i) => ({kind: "upsert", doc_id: `doc/${String(i).padStart(3,"0")}`, body: JSON.stringify({text: "durable original", embedding: [1,i/45], sparse_embedding: {durable: 1}})}));
  await host.request(request("mutate", {operation_id: "reindex-source", mutations: docs}));
  while ((await host.request(request("publish"))).has_more) {}
  const start = await host.request(request("reindex"));
  assert.equal(start.phase, "scan"); assert.ok(start.job_id);
  assert.equal((await host.request(request("reindex"))).job_id, start.job_id, "duplicate start keeps its checkpoint");
  failCheckpoint = true;
  await assert.rejects(host.request(request("reindex", {reindex_action: "step", reindex_job_id: start.job_id, reindex_records: 8})));
  host = await reopen();
  assert.equal((await host.request(request("reindex", {reindex_action: "status"}))).scanned, 0, "failed checkpoint must not advance the scan");
  await host.request(request("reindex", {reindex_action: "step", reindex_job_id: start.job_id, reindex_records: 8}));
  host = await reopen();
  assert.equal((await host.request(request("reindex", {reindex_action: "status"}))).scanned, 8);
  await host.request(request("mutate", {operation_id: "reindex-live-edit", mutations: [{kind: "delete", doc_id: "doc/000"}, {kind: "upsert", doc_id: "needle", body: '{"text":"replacementneedle","embedding":[0,1],"sparse_embedding":{"needle":2}}'}]}));
  // The published generation may advance while the frozen source scan resumes.
  while ((await host.request(request("publish"))).has_more) {}
  let finished = false, published = false, injected = false;
  for (let step = 0; step < 32; step++) {
    host = await reopen();
    const status = await host.request(request("reindex", {reindex_action: "status"}));
    if (status.phase === "publish" && !injected) {
      const head = bucket.objects.get("scope/progress/tenant/table/incarnation1/HEAD");
      headThreshold = JSON.parse(Buffer.from(head.httpMetadata.contentType.split("base64url=")[1], "base64url")).head_version;
      await host.request(request("mutate", {operation_id: "late-reindex-tail", mutations: [{kind: "upsert", doc_id: "doc/001", body: JSON.stringify({text: "durable original", embedding: [1,1/45], sparse_embedding: {durable: 1}})}]}));
      loseHeadResponse = true;
      await assert.rejects(host.request(request("reindex", {reindex_action: "step", reindex_job_id: start.job_id, reindex_records: 8})));
      assert.equal(loseHeadResponse, false, "the injected failure followed a committed HEAD update");
      injected = true;
      // More than one bounded lineage page may commit before the ack retries.
      for (let i = 0; i < 130; i++) {
        await host.request(request("mutate", {operation_id: `post-reindex/${i}`, mutations: [{kind: "upsert", doc_id: "doc/001", body: JSON.stringify({text: "durable original", embedding: [1,1/45], sparse_embedding: {durable: 1}})}]}));
        await host.request(request("publish"));
      }
      continue;
    }
    const result = await host.request(request("reindex", {reindex_action: "step", reindex_job_id: start.job_id, reindex_records: 8}));
    published ||= result.published;
    if (!result.has_more) {finished = true; break;}
  }
  assert.ok(finished && published && injected);
  assert.equal((await host.request(request("count"))).count, "45");
  assert.equal((await host.request(request("lookup", {doc_id: "doc/000"}))).body, null);
  assert.equal((await host.request(request("search", {text: "replacementneedle"}))).hits[0].doc_id, "needle");
  assert.equal((await host.request(request("search", {mode: "vector", vector: [0,1]}))).hits[0].doc_id, "needle");
  assert.equal((await host.request(request("search", {mode: "sparse", sparse: [{term: "needle", weight: 1}]}))).hits[0].doc_id, "needle");
  assert.equal((await host.request(request("compact", {compaction_max_documents: 0}))).published, false, "reindex produced fully paged serving roots");
  assert.equal((await host.request(request("reindex", {reindex_action: "step", reindex_job_id: start.job_id}))).has_more, false);
  const next = await host.request(request("reindex"));
  assert.notEqual(next.job_id, start.job_id);
  assert.equal((await host.request(request("reindex", {reindex_action: "cancel", reindex_job_id: start.job_id}))).has_more, false, "stale cancel cannot cancel its successor");
  assert.equal((await host.request(request("reindex", {reindex_action: "status"}))).job_id, next.job_id);
  await host.request(request("reindex", {reindex_action: "cancel", reindex_job_id: next.job_id}));
  assert.equal((await host.request(request("reindex", {reindex_action: "status"}))).has_more, false);
  const expiring = await host.request(request("reindex"));
  now += 11 * 60 * 1000;
  host = await reopen();
  assert.equal((await host.request(request("reindex", {reindex_action: "status"}))).has_more, false);
  assert.equal((await host.request(request("reindex", {reindex_action: "step", reindex_job_id: expiring.job_id}))).has_more, false);
  const replacement = await host.request(request("reindex"));
  assert.notEqual(replacement.job_id, expiring.job_id);
  await host.request(request("reindex", {reindex_action: "cancel", reindex_job_id: replacement.job_id}));
});

test("decoded manifest reuse retains fresh WAL visibility and retirement-floor checks",async()=>{
 const metrics=[],{host,transport}=await setup(new Bucket(),t=>t,{onMetrics:sample=>metrics.push(sample)});
 await host.request(request("mutate",{operation_id:"cache/seed",mutations:[{kind:"upsert",doc_id:"cached",body:'{"text":"before"}'}]}));await host.request(request("publish"));
 await host.request(request("lookup",{doc_id:"cached"}));await host.request(request("lookup",{doc_id:"cached"}));
 assert.ok(metrics.at(-1).decoded_manifest_cache_hits>0);assert.ok(metrics.at(-1).decoded_manifest_cache_bytes<=512*1024);
 const receipt=await host.request(request("mutate",{operation_id:"cache/change",mutations:[{kind:"upsert",doc_id:"cached",body:'{"text":"after"}'}]}));
 assert.equal(JSON.parse((await host.request(request("lookup",{doc_id:"cached",required_visibility:receipt.visibility}))).body).text,"after");await host.request(request("publish"));
 assert.equal(JSON.parse((await host.request(request("lookup",{doc_id:"cached",required_visibility:receipt.visibility}))).body).text,"after");
 await transport({op:"put",bucket:"database",key:"scope/progress/tenant/table/incarnation1/MANIFEST_GC_FLOOR"},new TextEncoder().encode("999"));
 await assert.rejects(host.request(request("lookup",{doc_id:"cached"})),/ManifestVersionRetired/);
});


test('native exact scan survives host eviction and publication while preserving the original top-k',async()=>{
 const {host,bucket}=await setup();
 await host.request(request('mutate',{paged_text:true,paged_projections:true,operation_id:'exact-seed',mutations:Array.from({length:48},(_,i)=>({kind:'upsert',doc_id:'item/'+i,body:JSON.stringify({text:'exact',embedding:[1,i/48,0,0]})}))}));
 for(let i=0;i<10;i++)if(!(await host.request(request('publish',{paged_text:true,paged_projections:true,publication_records:16}))).has_more)break;
 const input={mode:'vector',vector:[1,0,0,0],limit:5,search_effort:1};
 const expected=await host.request(request('search',input));
 let step=await host.request(request('search',{...input,exact_scan:true,exact_records:7}));
 assert.equal(step.complete,false);assert.deepEqual(step.hits,[]);assert.equal(step.step_records,7);
 const initial=structuredClone(step.exact_state);
 await host.request(request('mutate',{operation_id:'exact-update',mutations:[{kind:'delete',doc_id:'item/0'}]}));
 await host.request(request('publish',{paged_text:true,paged_projections:true,publication_records:16}));
 let steps=1;
 while(!step.complete){const reopened=await setup(bucket);step=await reopened.host.request(request('search',{...input,exact_scan:true,exact_records:7,exact_state:step.exact_state}));assert.ok(step.step_records<=7);assert.ok(step.page_reads<=128);assert.ok(step.scan_read_bytes<=4*1024*1024);assert.ok(++steps<20);}
 assert.deepEqual(step.hits.map(h=>h.doc_id),expected.hits.map(h=>h.doc_id));assert.equal(step.visibility,expected.visibility);
 const replay=await setup(bucket);const repeated=await replay.host.request(request('search',{...input,exact_scan:true,exact_records:7,exact_state:initial}));
 assert.equal(repeated.exact_state.scanned,'14');
 await assert.rejects(replay.host.request(request('search',{...input,vector:[0,1,0,0],exact_scan:true,exact_records:7,exact_state:initial})),/ExactScanBindingMismatch/);
 await bucket.put('scope/progress/tenant/table/incarnation1/MANIFEST_GC_FLOOR',new TextEncoder().encode('999'));
 await assert.rejects(replay.host.request(request('search',{...input,exact_scan:true,exact_records:7,exact_state:initial})),/ManifestVersionRetired/);
});


test('native exhaustive scan ranks the stored FP32 values across an FP16 near tie',async()=>{
 const {host}=await setup();
 await host.request(request('mutate',{operation_id:'precision',mutations:[{kind:'upsert',doc_id:'a',body:JSON.stringify({embedding:[1,1.0002]})},{kind:'upsert',doc_id:'z',body:JSON.stringify({embedding:[1,1.0001]})}]}));
 await host.request(request('publish',{paged_text:true,paged_projections:true,publication_records:16}));
 let state=null,result;
 do{result=await host.request(request('search',{mode:'vector',vector:[1,0],limit:1,search_effort:1,exact_scan:true,exact_records:1,exact_state:state}));state=result.exact_state;}while(!result.complete);
 assert.equal(result.hits[0].doc_id,'z');
 const score=1/Math.hypot(1,Math.fround(1.0001));assert.ok(Math.abs(result.hits[0].distance-(1-score))<1e-6);
});

test('warm serving caches isolate buckets and prefixes on one reused kernel',async()=>{
 const buckets={database:new Bucket(),alternate:new Bucket()};
 const host=await instantiateHost(module,createR2Transport(buckets,{maxPutBytes:4*1024*1024}));
 const scopes=[{bucket:'database',prefix:'first',id:'a',vector:[1,0]},{bucket:'database',prefix:'second',id:'b',vector:[0,1]},{bucket:'alternate',prefix:'second',id:'c',vector:[1,1]}];
 for(const scope of scopes){
  const args={bucket:scope.bucket,prefix:scope.prefix,paged_text:true,paged_projections:true};
  await host.request(request('mutate',{...args,operation_id:'seed',mutations:[{kind:'upsert',doc_id:scope.id,body:JSON.stringify({embedding:scope.vector})}]}));
  while((await host.request(request('publish',args))).has_more){}
 }
 for(const scope of [...scopes,scopes[0]]){
  const result=await host.request(request('search',{bucket:scope.bucket,prefix:scope.prefix,mode:'vector',vector:scope.vector,limit:1,search_effort:1,exact_scan:true}));
  assert.equal(result.complete,true);assert.equal(result.hits[0].doc_id,scope.id);
 }
});

test('warm exact resumes reuse durable read protection while checking retirement on every request',async()=>{
 const calls=[];const{host,bucket}=await setup(new Bucket(),transport=>async(request,body)=>{calls.push(request);return transport(request,body);});
 await host.request(request('mutate',{operation_id:'lease-cache',mutations:[0,1,2].map(i=>({kind:'upsert',doc_id:'doc/'+i,body:JSON.stringify({embedding:[1,i/3]})}))}));
 while((await host.request(request('publish',{paged_text:true,paged_projections:true}))).has_more){}
 const query={mode:'vector',vector:[1,0],limit:1,search_effort:1,exact_scan:true,exact_records:1};
 let result=await host.request(request('search',query));
 assert.equal(result.complete,false);assert.equal(result.exact_state.format,2);assert.ok(result.exact_state.pending.length>0);
 const start=calls.length;
 result=await host.request(request('search',{...query,exact_state:result.exact_state}));
 const resumed=calls.slice(start).filter(call=>call.op==='get');
 assert.ok(resumed.some(call=>call.key.endsWith('/MANIFEST_GC_FLOOR')));
 assert.equal(resumed.filter(call=>call.key.includes('/READ_PINS/')).length,0);
 await bucket.put('scope/progress/tenant/table/incarnation1/MANIFEST_GC_FLOOR',new TextEncoder().encode('999'));
 await assert.rejects(host.request(request('search',{...query,exact_state:result.exact_state})),/ManifestVersionRetired/);
});

test('exact original-source reuse requires the published document revision and falls back after a non-vector edit',async()=>{
 const{host}=await setup();
 const vector=Array(128).fill(0);vector[0]=1;
 const other=Array(128).fill(0);other[1]=1;
 await host.request(request('mutate',{operation_id:'source-revisions',mutations:Array.from({length:16},(_,i)=>({kind:'upsert',doc_id:i===0?'needle':'other/'+i,body:JSON.stringify({text:'before',embedding:i===0?vector:other})}))}));
 while((await host.request(request('publish',{paged_text:true,paged_projections:true,publication_records:64}))).has_more){}
 const query={mode:'vector',vector,limit:1,search_effort:1,exact_scan:true};
 const original=await host.request(request('search',query));
 assert.equal(original.complete,true);assert.equal(original.hits[0].doc_id,'needle');assert.ok(original.body_refinements>0);
 await host.request(request('mutate',{operation_id:'source-text-only',mutations:[{kind:'upsert',doc_id:'needle',body:JSON.stringify({text:'after',embedding:vector})}]}));
 while((await host.request(request('publish',{paged_text:true,paged_projections:true}))).has_more){}
 const updated=await host.request(request('search',query));
 assert.equal(updated.hits[0].doc_id,'needle');assert.equal(updated.body_refinements,0);assert.ok(updated.residual_fetches>0);assert.equal(JSON.parse(updated.hits[0].body).text,'after');
});

test('vector run maintenance recovers uploaded orphans and an ambiguous HEAD without advancing WAL visibility',async()=>{
 const {host,bucket}=await setup();
 const publish=request('publish',{paged_text:true,paged_projections:true,publication_records:16});
 await host.request(request('mutate',{operation_id:'merge-fault-source',mutations:Array.from({length:64},(_,i)=>({kind:'upsert',doc_id:'merge/'+i,body:JSON.stringify({embedding:[1,i/64]})}))}));
 for(let i=0;i<4;i++)assert.equal((await host.request(publish)).has_more,true);
 const headKey='scope/progress/tenant/table/incarnation1/HEAD';
 const original=Number(new TextDecoder().decode(bucket.objects.get(headKey).body).trim());
 const visibility=(await host.request(request('latest'))).visibility;
 let upload=true;
 const interrupted=(await setup(bucket,transport=>async(req,body)=>{
  const response=await transport(req,body);
  if(upload&&req.op==='put'&&req.key.includes('/artifacts/')){upload=false;throw Error('lost merge upload');}
  return response;
 })).host;
 await assert.rejects(interrupted.request(publish),/ObjectStoreTransportFailed/);
 assert.equal(upload,false);assert.equal(Number(new TextDecoder().decode(bucket.objects.get(headKey).body).trim()),original);
 let head=true;
 const ambiguous=(await setup(bucket,transport=>async(req,body)=>{
  const response=await transport(req,body);
  if(head&&req.op==='put'&&req.key===headKey&&Number(new TextDecoder().decode(body).trim())>original){head=false;throw Error('lost merge HEAD response');}
  return response;
 })).host;
 await assert.rejects(ambiguous.request(publish),/ObjectStoreTransportFailed/);
 assert.equal(head,false);
 const recovered=(await setup(bucket)).host;
 for(let i=0;i<16;i++){if(!(await recovered.request(publish)).has_more)break;assert.ok(i<15);}
 assert.equal((await recovered.request(request('latest'))).visibility,visibility);
 assert.equal((await recovered.request(request('count'))).count,'64');
 const result=await recovered.request(request('search',{mode:'vector',vector:[1,0],limit:1,search_effort:1,exact_scan:true}));
 assert.equal(result.complete,true);assert.equal(result.hits[0].doc_id,'merge/0');
});

test('the final vector maintenance checkpoint finishes without an empty rescan request',async()=>{
 const{host}=await setup();
 await host.request(request('mutate',{operation_id:'maintenance-terminal',mutations:Array.from({length:64},(_,i)=>({kind:'upsert',doc_id:'terminal/'+i,body:JSON.stringify({embedding:[1,i/64]})}))}));
 const publish=request('publish',{paged_text:true,paged_projections:true,publication_records:16});
 for(let i=0;i<4;i++)assert.equal((await host.request(publish)).has_more,true);
 const result=await host.request(publish);
 assert.equal(result.published,true);assert.equal(result.consumed_records,0);assert.equal(result.has_more,false);
});


test("immutable write-through admits only successful conditional uploads and preserves byte bounds",async()=>{
 const cache=createImmutableReadCache({maxBytes:4096,maxEntries:8,writeThrough:true});let gets=0,fail=false;
 const transport=cache.wrap(async request=>{if(request.op==='put'){if(fail)throw Error('lost acknowledgement');return{json:{etag:'durable'}};}gets++;return{json:{etag:'backend'},body:Uint8Array.of(9)};},'scope');
 const key=`scope/artifacts/graph/${'a'.repeat(64)}/${'b'.repeat(32)}/${'c'.repeat(64)}`,put={op:'put',bucket:'db',key,if_none_match:true},get={op:'get',bucket:'db',key};
 const body=Uint8Array.of(1,2);await transport(put,body);body[0]=99;
 const first=await transport(get);assert.deepEqual(first.body,Uint8Array.of(1,2));assert.equal(first.json.etag,'durable');first.body[0]=88;assert.equal((await transport(get)).body[0],1);assert.equal(gets,0);
 await assert.rejects(transport({...get,max_response_bytes:1}),error=>error.status===4);
 cache.clear();fail=true;await assert.rejects(transport(put,Uint8Array.of(3)),/lost acknowledgement/);await transport(get);assert.equal(gets,1);
 cache.clear();fail=false;await transport({...put,if_none_match:false},Uint8Array.of(4));await transport(get);assert.equal(gets,2);
 cache.clear();const mutable={...put,key:'scope/progress/HEAD'};await transport(mutable,Uint8Array.of(5));await transport({...get,key:mutable.key});await transport({...get,key:mutable.key});assert.equal(gets,4);
 cache.clear();await transport(put,new Uint8Array(8192));await transport(get);assert.equal(gets,5);assert.ok(cache.retainedBytes()<=4096);
});


test("pooled host refreshes an older I/O context before native entry without extending a rolled-back clock",async()=>{
 let now=10000,refreshes=0;
 const bucket=new Bucket(),host=(await setup(bucket,undefined,{clock:()=>now,refreshClock:async()=>{refreshes++;now=10001;}})).host;
 const mutation=request("mutate",{operation_id:"stale-context",mutations:[{kind:"upsert",doc_id:"a",body:'{"text":"context refresh"}'}]});
 const accepted=await host.request(mutation);await host.request(request("publish"));
 now=9999;const samples=[];assert.equal((await host.request(request("count",{required_visibility:accepted.visibility}),{onMetrics:m=>samples.push(m)})).count,"1");
 assert.equal(refreshes,1);assert.equal(samples[0].clock_refreshes,1);assert.equal(samples[0].clock_regression_ms,1);assert.equal(samples[0].clock_valid,true);assert.equal(host.usable(),true);
 assert.equal((await host.request(mutation)).visibility,accepted.visibility);assert.equal(refreshes,1);
 const failed=(await setup(bucket,undefined,{clock:()=>now,refreshClock:async()=>{throw Error("clock refresh failed");}})).host;
 await failed.request(request("count"));now--;
 await assert.rejects(failed.request(request("count")),/clock refresh failed/);assert.equal(failed.usable(),false);
});


test("pooled host waits out small frozen clock gaps at admission and I/O resumption",async()=>{
 let now=10000;const waits=[],bucket=new Bucket(),{host,transport}=await setup(bucket,undefined,{clock:()=>now,refreshClock:async delay=>{waits.push(delay);now+=delay;}});
 const accepted=await host.request(request('mutate',{operation_id:'small-clock-gap',mutations:[{kind:'upsert',doc_id:'a',body:'{"text":"gap"}'}]}));await host.request(request('publish'));await host.request(request('count'));
 now-=5;assert.equal((await host.request(request('count'))).count,'1');assert.equal(waits[0],6);assert.ok(host.usable());
 let shifted=false;const before=waits.length,result=await host.request(request('lookup',{doc_id:'a',required_visibility:accepted.visibility}),{transport:async(req,body)=>{const value=await transport(req,body);if(!shifted&&req.op==='get'){shifted=true;now-=3;}return value;}});
 assert.equal(JSON.parse(result.body).text,'gap');assert.ok(shifted);assert.ok(waits.length>before);assert.ok(waits.every(delay=>delay>=1&&delay<=50));assert.ok(host.usable());assert.equal((await host.request(request('mutate',{operation_id:'small-clock-gap',mutations:[{kind:'upsert',doc_id:'a',body:'{"text":"gap"}'}]}))).visibility,accepted.visibility);
});
test("pooled host rejects a clock still behind after bounded fresh I/O",async()=>{
 let now=10000;const waits=[],{host}=await setup(new Bucket(),undefined,{clock:()=>now,refreshClock:async delay=>waits.push(delay)});await host.request(request('mutate',{operation_id:'clock-rollback',mutations:[{kind:'upsert',doc_id:'a',body:'{"text":"clock"}'}]}));await host.request(request('publish'));await host.request(request('count'));now-=1000;await assert.rejects(host.request(request('count')),/ManifestReadLeaseExpired/);assert.deepEqual(waits,[50]);assert.equal(host.usable(),false);
});

test("inline point bodies preserve exact results and survive disabling new writes",async()=>{
 const{host,bucket}=await setup();const body=JSON.stringify({text:"inline",payload:"x".repeat(1024)});
 await host.request(request("mutate",{operation_id:"inline-source",mutations:[{kind:"upsert",doc_id:"small",body}]}));
 await host.request(request("publish",{paged_text:true,inline_point_bodies:true}));
 assert.ok([...bucket.objects.values()].some(o=>new TextDecoder().decode(o.body.subarray(0,8))==="AFDFACTB"));
 const cold=(await setup(bucket)).host;assert.equal((await cold.request(request("lookup",{doc_id:"small"}))).body,body);
 await cold.request(request("mutate",{operation_id:"inline-update",mutations:[{kind:"upsert",doc_id:"small",body:'{"text":"updated"}'}]}));await cold.request(request("publish",{paged_text:true,inline_point_bodies:false}));assert.equal((await cold.request(request("lookup",{doc_id:"small"}))).body,'{"text":"updated"}');
});

test("canonical point bodies preserve exact results and survive disabling new writes",async()=>{
 const{host,bucket}=await setup();const body=JSON.stringify({text:"inline",payload:"x".repeat(1024)});
 await host.request(request("mutate",{operation_id:"inline-source",mutations:[{kind:"upsert",doc_id:"small",body}]}));
 await host.request(request("publish",{paged_text:true,canonical_point_bodies:true}));
 assert.ok([...bucket.objects.values()].some(o=>new TextDecoder().decode(o.body.subarray(0,8))==="AFDFACTC"));
 const cold=(await setup(bucket)).host;assert.equal((await cold.request(request("lookup",{doc_id:"small"}))).body,body);
 await cold.request(request("mutate",{operation_id:"inline-update",mutations:[{kind:"upsert",doc_id:"small",body:'{"text":"updated"}'}]}));await cold.request(request("publish",{paged_text:true,canonical_point_bodies:false}));assert.equal((await cold.request(request("lookup",{doc_id:"small"}))).body,'{"text":"updated"}');
});


test('native read plans prepare immutable bytes before admission and validate the fresh root and overlay',async()=>{
 const {bucket,host:writer,transport}=await setup();
 const mutate=async(id,body)=>writer.request(request('mutate',{operation_id:id,mutations:[{kind:'upsert',doc_id:'one',body:JSON.stringify({text:body})}]}));
 await mutate('plan-seed','original');await writer.request(request('publish',{paged_text:true}));
 let release,entered,before=0;const admitted=new Promise(r=>entered=r);let gate=Promise.resolve();
 const reader=await instantiateHost(module,async(req,body)=>{
  if(req.op==='read_view_barrier'){entered();await gate;return{};}
  if(req.op==='get'&&req.key.includes('/artifacts/'))before++;
  return transport(req,body);
 });
 const first=await reader.request(request('lookup',{doc_id:'one',native_read_plan_mode:true}));assert.equal(JSON.parse(first.body).text,'original');assert.ok(first.__read_plan);
 gate=new Promise(r=>release=r);let reached;const atBarrier=new Promise(r=>reached=r);entered=reached;before=0;
 const pending=reader.request(request('lookup',{doc_id:'one',native_read_plan_mode:true,native_read_plan:first.__read_plan}));
 await atBarrier;assert.ok(before>0,'immutable facts/body reads must precede fresh admission');
 await mutate('plan-overlay','fresh-overlay');release();assert.equal(JSON.parse((await pending).body).text,'fresh-overlay');
 // Publish a changed root while preparation is held at the barrier.
 gate=new Promise(r=>release=r);const atNext=new Promise(r=>entered=r);
 const changed=reader.request(request('lookup',{doc_id:'one',native_read_plan_mode:true,native_read_plan:first.__read_plan}));
 await atNext;await writer.request(request('publish',{paged_text:true}));release();const updated=await changed;assert.equal(JSON.parse(updated.body).text,'fresh-overlay');
 assert.notEqual(updated.__read_plan.source.artifact_id,first.__read_plan.source.artifact_id);
 const forged=structuredClone(first.__read_plan);forged.domain[0]^=1;
 await assert.rejects(reader.request(request('lookup',{doc_id:'one',native_read_plan_mode:true,native_read_plan:forged})),/InvalidNativeReadPlan/);
});

test('native WAL plans keep uploads outside metadata commits and atomically fence receipts and source dependencies',async()=>{
 const {bucket,host}=await setup();const owner=Array(16).fill(1),base={op:'coordinate',bucket:'database',prefix:'antfly',namespace:'metadata'};
 const coordinate=async(action,state,extra={})=>host.request({...base,coordinate:{action,owner,state,...extra}});
 let state=(await coordinate('begin',null)).state;
 const prepare=(operation_id,source=state)=>host.request({op:'mutate',bucket:'database',prefix:'antfly',namespace:'docs',wal_inline_tail:true,native_write_plan:{state:source,metadata_namespace:'metadata',reservation_id:operation_id},mutation_groups:[{operation_id,mutations:[{kind:'upsert',doc_id:operation_id,body:'{"text":"planned"}'}]}]});
 const first=await prepare('first');assert.ok(first.__write_plan);assert.equal(first.__write_plan.dependencies.length,2);
 assert.ok(!bucket.objects.has(first.__write_plan.wal_key),'preparation cannot write authoritative WAL HEAD');
 const observed=(await coordinate('begin',state)).state;assert.equal(observed.revision,state.revision);assert.equal(observed.root,state.root,'fresh recovery must see no metadata commit from preparation');state=observed;
 assert.deepEqual((await coordinate('read',state,{commands:[{kind:'get',key:first.__write_plan.wal_key}]})).results,[[]]);

 const committed=await coordinate('commit_wal',state,{wal_commit:first.__write_plan});assert.equal(committed.applied,true);state=committed.state;
 const replay=await prepare('first');assert.deepEqual(replay.results,first.results);
 const left=await prepare('left'),right=await prepare('right');
 const accepted=await coordinate('commit_wal',state,{wal_commit:left.__write_plan});assert.equal(accepted.applied,true);state=accepted.state;
 assert.equal((await coordinate('commit_wal',state,{wal_commit:right.__write_plan})).applied,false,'changed native WAL identity rejects stale preparation');
 const fresh=await prepare('right');const final=await coordinate('commit_wal',state,{wal_commit:fresh.__write_plan});assert.equal(final.applied,true);state=final.state;
 const taken=await host.request({...base,coordinate:{action:'begin',owner:Array(16).fill(2),state}});assert.notDeepEqual(taken.state.owner,owner);
 const stale=await prepare('fenced',state);
 await assert.rejects(coordinate('commit_wal',state,{wal_commit:stale.__write_plan}),/ImmutableLogFenced|PreconditionFailed/);
});


test('durable native publication finishes one large record under bounded uploads and recovers lost checkpoints and HEAD acknowledgements',{timeout:120000},async()=>{
 const bucket=new Bucket(),fault={puts:0,limit:12,checkpoint:false,head:false},headKey='scope/progress/tenant/table/incarnation1/HEAD';
 const open=async()=>setup(bucket,raw=>async(req,body)=>{
  if(req.op==='put'&&req.key.includes('/artifacts/')){if(++fault.puts>fault.limit)throw Error('InjectedPublicationSliceLimit');if(fault.partialAt===fault.puts){fault.partialAt=0;throw Error('InjectedPartialUploadFailure');}}
  let actual=req;
  if(req.guard_key!=null){if(bucket.objects.get(req.guard_key)?.etag!==req.guard_etag)throw new ObjectStoreError(2,'guard changed');actual={...req,guard_key:null,guard_etag:null};}
  const result=await raw(actual,body);
  if(req.op==='put'&&req.key.endsWith('/PUBLICATION_PLAN')&&body?.byteLength&&fault.checkpoint){fault.checkpoint=false;throw Error('InjectedCheckpointAcknowledgementLoss');}
  if(req.op==='put'&&req.key===headKey&&/^([1-9][0-9]*)$/.test(new TextDecoder().decode(body).trim())&&fault.head){fault.head=false;throw Error('InjectedHeadAcknowledgementLoss');}
  return result;
 });
 let {host}=await open();const text=Array.from({length:12000},(_,i)=>'word'+i).join(' '),body=JSON.stringify({text,sparse_embedding:{needle:1}});
 fault.limit=Infinity;const accepted=await host.request(request('mutate',{operation_id:'large-native-plan',mutations:[{kind:'upsert',doc_id:'one',body}]}));
 fault.limit=12;fault.puts=0;
 await assert.rejects(host.request(request('publish',{paged_text:true,paged_projections:true})),/ObjectStoreTransportFailed/);
 assert.equal(Number(new TextDecoder().decode(bucket.objects.get(headKey)?.body??new Uint8Array()).trim()||0),0);
 fault.puts=0;fault.checkpoint=true;fault.head=true;
 const input=request('publish',{paged_text:true,paged_projections:true,publication_continuations:true,publication_units:128});
 await assert.rejects(host.request(input),/ObjectStoreTransportFailed/);
 ({host}=await open());const status=await host.request({...input,publication_action:'status'});assert.ok(status.job_id);const job=status.job_id;
 fault.partialAt=2;let interrupted=false;
 for(let i=0;i<16;i++){fault.puts=0;try{await host.request({...input,publication_job_id:job});}catch(error){assert.match(error.message,/ObjectStoreTransportFailed/);interrupted=true;break;}}
 assert.equal(interrupted,true);assert.equal(fault.partialAt,0);({host}=await open());
 let result,steps=0,ambiguous=0,maximum=0;
 for(;steps<256;steps++){
  if(steps%11===0)({host}=await open());fault.puts=0;
  try{result=await host.request({...input,publication_job_id:job});}
  catch(error){assert.match(error.message,/ObjectStoreTransportFailed/);assert.equal(fault.head,false,'only the final HEAD acknowledgement may be ambiguous');ambiguous++;continue;}
  maximum=Math.max(maximum,fault.puts);assert.ok(fault.puts<=12);
  if(result.published)break;
  assert.equal(result.has_more,true);assert.equal(result.wal_end_lsn,'0','private progress cannot advance the public cutoff');
  assert.equal(Number(new TextDecoder().decode(bucket.objects.get(headKey).body).trim()||0),0);
 }
 assert.ok(steps>10&&steps<256);assert.equal(ambiguous,1);assert.equal(result.published,true);assert.ok(maximum<=12);
 const visible=await host.request(request('lookup',{doc_id:'one',required_visibility:accepted.visibility}));assert.equal(visible.body,body);
 assert.equal((await host.request(request('search',{text:'word11999'}))).hits[0].doc_id,'one');
 const durable=await host.request({...input,publication_action:'status',publication_job_id:job});assert.deepEqual(durable,result);
 fault.limit=Infinity;
 await host.request(request('mutate',{operation_id:'later',mutations:[{kind:'upsert',doc_id:'later',body:'{"text":"later"}'}]}));
 const obsolete=await host.request({...input,publication_continuation_force:true});assert.ok(obsolete.job_id);
 await host.request(request('publish',{paged_text:true,paged_projections:true}));
 const superseded=await host.request({...input,publication_job_id:obsolete.job_id});assert.equal(superseded.phase,'superseded');assert.equal(superseded.published,false);
 assert.deepEqual(await host.request({...input,publication_action:'status',publication_job_id:job}),durable);
});

test('native continuation probe selects prepared batching before constructing an ordinary publication',async()=>{
 const{host,bucket}=await setup();await host.request(request('mutate',{operation_id:'probe-small',mutations:[{kind:'upsert',doc_id:'one',body:'{"text":"small"}'}]}));
 const before=new Map([...bucket.objects].map(([key,value])=>[key,value.etag]));
 const plan=await host.request(request('publish',{paged_text:true,paged_projections:true,publication_continuations:true,publication_batch_probe:true}));assert.equal(plan.__publication_batch_records,1);assert.equal(plan.__publication_batch_version,'0');
 assert.deepEqual(new Map([...bucket.objects].map(([key,value])=>[key,value.etag])),before,'small-record planning must neither lease nor upload');
 assert.equal((await host.request(request('publish',{paged_text:true,paged_projections:true}))).published,true);
});


test('native publication batching stops before a large record and fences its source generation',async()=>{
 const bucket=new Bucket(),{host}=await setup(bucket,raw=>async(req,body)=>{if(req.guard_key!=null){if(bucket.objects.get(req.guard_key)?.etag!==req.guard_etag)throw new ObjectStoreError(2,'guard changed');return raw({...req,guard_key:null,guard_etag:null},body);}return raw(req,body);});const large=JSON.stringify({text:Array.from({length:3000},(_,i)=>'word'+i).join(' ')});
 await host.request(request('mutate',{operation_id:'mixed',mutations:[{kind:'upsert',doc_id:'first',body:'{"text":"small"}'},{kind:'upsert',doc_id:'large',body:large},{kind:'upsert',doc_id:'last',body:'{"text":"last"}'}]}));
 const probe=request('publish',{paged_text:true,paged_projections:true,publication_continuations:true,publication_batch_probe:true,publication_records:64});
 const plan=await host.request(probe);assert.equal(plan.__publication_batch_records,1);
 const published=await host.request(request('publish',{paged_text:true,paged_projections:true,publication_records:1,publication_batch_version:plan.__publication_batch_version}));assert.equal(published.wal_end_lsn,'1');assert.equal(published.has_more,true);
 await assert.rejects(host.request(request('publish',{publication_batch_version:plan.__publication_batch_version})),/PublicationBatchSourceChanged/);
 const job=await host.request(probe);assert.ok(job.job_id);assert.equal(job.wal_end_lsn,'1');
});

test('detached native preparation releases its call and validates fresh overlays, roots and cache misses',async()=>{
 const {host:writer,transport}=await setup();
 let artifactReads=0;const reader=await instantiateHost(module,async(req,body)=>{if(req.op==='get'&&req.key.includes('/artifacts/'))artifactReads++;assert.notEqual(req.op,'read_view_barrier','detached reads must never suspend a query stack on authority');return transport(req,body);});
 const mutate=async(op,body)=>writer.request(request('mutate',{operation_id:op,mutations:[{kind:body===null?'delete':'upsert',doc_id:'one',...(body===null?{}:{body})}]}));
 await mutate('detached-seed','{"text":"old"}');await writer.request(request('publish',{paged_text:true}));
 const seed=await writer.request(request('lookup',{doc_id:'one',native_read_plan_mode:true}),{transport:async(req,body)=>req.op==='read_view_barrier'?{}:transport(req,body)});
 const plan=seed.__read_plan,prepare=async(n,id='one')=>{const token=Array(16).fill(n),result=await reader.request(request('lookup',{doc_id:id,native_read_plan_mode:true,native_read_plan:plan,native_read_prepare:token}));assert.equal(result.__read_prepared,true);assert.equal(result.body,undefined);return{token,spent:result.__read_spent_bytes};};
 const resume=(p,id='one',extra={})=>reader.request(request('lookup',{doc_id:id,native_read_plan_mode:true,native_read_resume:p.token,native_read_spent_bytes:p.spent,...extra}));
 await reader.request(request('lookup',{doc_id:'one'}));
 const retained=await prepare(11);artifactReads=0;await resume(retained);const hitReads=artifactReads;artifactReads=0;await resume(retained);assert.ok(artifactReads>hitReads,'consuming a retained preparation must save immutable artifact reads relative to a ticket miss');
 const first=await prepare(1);await mutate('detached-update','{"text":"fresh"}');assert.equal((await resume(first)).body,'{"text":"fresh"}');
 const changed=await prepare(2);await writer.request(request('publish',{paged_text:true}));assert.equal((await resume(changed)).body,'{"text":"fresh"}');
 // Wrong-document and wrong-namespace tickets convey no membership or body.
 const different=await prepare(3);assert.equal((await resume(different,'missing')).body,null);assert.equal((await resume(different,'one',{namespace:'other'})).body,null);
 const evicted=await prepare(4);for(let i=5;i<10;i++)await prepare(i);assert.equal((await resume(evicted)).body,'{"text":"fresh"}');
 const deleted=await prepare(10);await mutate('detached-delete',null);assert.equal((await resume(deleted)).body,null);
 await assert.rejects(resume(deleted,'one',{native_read_spent_bytes:16*1024*1024+1}),/InvalidNativeReadPlan/);
});

for(const profile of[{native_read_state:true},{wal_document_index:true},{native_read_state:true,wal_document_index:true}])test('native read state preserves fresh visibility across append, publication, deletion and reopen '+JSON.stringify(profile),async()=>{
 const metrics=[],{bucket,host}=await setup(undefined,undefined,{onMetrics:sample=>metrics.push(sample)}),native=(op,args={})=>request(op,{...profile,...args}),body=text=>JSON.stringify({text});
 const seed=await host.request(native('mutate',{operation_id:'read-state-seed',mutations:[{kind:'upsert',doc_id:'one',body:body('base')},{kind:'upsert',doc_id:'stable',body:body('stable')}]}));await host.request(native('publish'));
 for(let i=0;i<2;i++)assert.equal((await host.request(native('lookup',{doc_id:'one'}))).body,body('base'));
 if(profile.native_read_state)assert.ok(metrics.some(sample=>sample.read_state_cache_hits>0));
 const update=await host.request(native('mutate',{operation_id:'read-state-update',mutations:[{kind:'upsert',doc_id:'one',body:body('fresh')}]}));
 assert.equal((await host.request(native('lookup',{doc_id:'one',required_visibility:update.visibility}))).body,body('fresh'));
 assert.equal((await host.request(native('lookup',{doc_id:'stable',required_visibility:update.visibility}))).body,body('stable'));
 if(profile.native_read_state)assert.ok(metrics.some(sample=>sample.read_state_cache_extensions>0));
 assert.equal((await host.request(native('count'))).count,'2');
 const cold=(await setup(bucket)).host;assert.equal((await cold.request(native('lookup',{doc_id:'one',required_visibility:update.visibility}))).body,body('fresh'));
 await host.request(native('publish'));
 const removed=await host.request(native('mutate',{operation_id:'read-state-delete',mutations:[{kind:'delete',doc_id:'one'}]}));
 assert.equal((await host.request(native('lookup',{doc_id:'one',required_visibility:removed.visibility}))).body,null);
 assert.equal((await host.request(native('count'))).count,'1');
 assert.equal((await host.request(native('lookup',{doc_id:'one',namespace:'other'}))).body,null);
 assert.equal(seed.visibility.length,update.visibility.length);
 for(const sample of metrics)assert.ok(sample.read_state_cache_bytes<=256*1024);
});

test('a failed WAL HEAD cannot publish document projection rows or receipts',async()=>{
 const{bucket,host}=await setup(),native=(op,args)=>request(op,{wal_document_index:true,...args});
 await host.request(native('mutate',{operation_id:'index-seed',mutations:[{kind:'upsert',doc_id:'one',body:'{"text":"old"}'}]}));
 const put=bucket.put.bind(bucket);let failed=false;
 bucket.put=async(key,...args)=>{if(key.includes('/wal-v1/')&&key.endsWith('/HEAD')&&!failed){failed=true;throw Error('injected-index-head-failure');}return put(key,...args);};
 const mutation=native('mutate',{operation_id:'index-retry',mutations:[{kind:'upsert',doc_id:'one',body:'{"text":"new"}'}]});
 await assert.rejects(host.request(mutation));assert.equal(failed,true);bucket.put=put;
 const reader=(await setup(bucket)).host;assert.equal((await reader.request(native('lookup',{doc_id:'one'}))).body,'{"text":"old"}');
 const accepted=await host.request(mutation);assert.equal((await reader.request(native('lookup',{doc_id:'one',required_visibility:accepted.visibility}))).body,'{"text":"new"}');
 assert.deepEqual(await host.request(mutation),accepted);
});

test('warm namespace affinity chooses an idle compatible kernel without waiting for a busy one',async()=>{
 const{RequestPool}=await import('../request-pool.mjs');let created=0;
 const pool=new RequestPool(()=>({id:++created,usable:()=>true,memoryBytes:()=>0}),2);
 let releaseA,releaseB,enteredA,enteredB;const aEntered=new Promise(r=>enteredA=r),bEntered=new Promise(r=>enteredB=r);
 const a=pool.withHost(async host=>{enteredA();await new Promise(r=>releaseA=r);return host.id;},{affinityKey:'a'});
 const b=pool.withHost(async host=>{enteredB();await new Promise(r=>releaseB=r);return host.id;},{affinityKey:'b'});
 await Promise.all([aEntered,bEntered]);releaseA();releaseB();const[aId,bId]=await Promise.all([a,b]);assert.notEqual(aId,bId);
 assert.equal(await pool.withHost(host=>host.id,{affinityKey:'b'}),bId);
 let release,entered;const started=new Promise(r=>entered=r),held=pool.withHost(async host=>{entered();await new Promise(r=>release=r);return host.id;},{affinityKey:'b'});
 await started;assert.equal(await pool.withHost(host=>host.id,{affinityKey:'b'}),aId,'affinity must not queue behind a busy host');release();assert.equal(await held,bId);assert.equal(created,2);
});


test("cancelled native I/O retains its caller and reports one matching physical settlement",async()=>{
 let release,entered;const held=new Promise(r=>release=r),started=new Promise(r=>entered=r),background=[],settled=[],metrics=[];
 const {host}=await setup(undefined,raw=>async(req,body)=>{entered();await held;return raw(req,body);});
 const controller=new AbortController(),work=host.request(request('latest'),{signal:controller.signal,background:p=>background.push(p),onIoSettled:m=>settled.push(m),onMetrics:m=>metrics.push(m)}).catch(e=>e);
 await started;controller.abort();
 try{const error=await work;assert.equal(error.message,'Canceled');assert.equal(background.length,1);assert.equal(background[0],error.ioSettled);assert.equal(settled.length,0);assert.equal(metrics[0].io_draining,true);assert.match(metrics[0].io_lifecycle_id,/^[a-f0-9-]{36}$/);
  release();await error.ioSettled;assert.deepEqual(settled,[{event:'antfly.io_settled',io_lifecycle_id:metrics[0].io_lifecycle_id}]);assert.equal(host.draining(),null);assert.equal(host.usable(),false);
 }finally{release();await work;await host.draining();}
});

test("R2 binding failures are typed without replaying ambiguous conditional writes",async()=>{
 const {r2BindingFailure,callR2Binding}=await import('../../../zig/lib/objectstore/wasm/r2.mjs');
 for(const code of [10001,10043,10054,10058]){
  const original=Error('get: provider failure ('+code+')'),typed=r2BindingFailure(original);assert.equal(typed.cause,original);assert.equal(typed.provider_code,code);assert.equal(typed.status,code===10058?6:7);assert.equal(typed.retryable,true);
  await assert.rejects(callR2Binding(async()=>{throw original;}),e=>e.cause===original);
 }
 for(const value of [Error('access denied (10003)'),Error('unrelated internal error'),new ObjectStoreError(2,'conflict')])assert.equal(r2BindingFailure(value),value);
 const bucket=new Bucket(),rawPut=bucket.put.bind(bucket);let calls=0;
 bucket.put=async(...args)=>{calls++;await rawPut(...args);throw Error('put: provider internal error (10001)');};
 const transport=createR2Transport({database:bucket});await assert.rejects(transport({op:'put',bucket:'database',key:'once',if_none_match:true},Uint8Array.of(1)),e=>e.status===7&&e.cause.message.includes('10001'));
 assert.equal(calls,1);assert.equal(bucket.objects.has('once'),true);
});


test('native receipt replay recovers an R2 internal error after its conditional commit',async()=>{
 const bucket=new Bucket(),first=(await setup(bucket)).host;await first.request(append('before'));
 const put=bucket.put.bind(bucket);let fail=true,ambiguous=0;
 bucket.put=async(key,body,options)=>{const result=await put(key,body,options);if(fail&&key.endsWith('/HEAD')&&options?.onlyIf?.etagMatches){fail=false;ambiguous++;throw Error('put: provider internal error (10001)');}return result;};
 const failed=(await setup(bucket)).host;await assert.rejects(failed.request(append('recover')),/ObjectStoreTransportFailed/);
 const reopened=(await setup(bucket)).host,receipt=await reopened.request(append('recover'));assert.equal(ambiguous,1);assert.equal(receipt.visibility.slice(-16),'0000000000000002');const rows=await reopened.request(request('read'));assert.deepEqual(rows.records.map(payloadText),['before','recover']);
});
