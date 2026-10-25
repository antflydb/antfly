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

// Actual native WASM over an in-process R2 contract fixture. These measurements
// characterize object amplification and WASM memory, not deployed R2 latency,
// total isolate memory or Cloudflare CPU admission.
import assert from "node:assert/strict";
import {readFile, writeFile, mkdir} from "node:fs/promises";
import {dirname} from "node:path";
import {createHash} from "node:crypto";
import {Bucket} from "../test/object-fixture.mjs";
import {instantiateHost} from "../../../zig/lib/objectstore/wasm/asyncify.mjs";
import {createR2Transport, ObjectStoreError} from "../../../zig/lib/objectstore/wasm/r2.mjs";
import {createImmutableReadCache} from "../../../zig/lib/objectstore/wasm/cache.mjs";

const deterministic=process.argv.includes("--deterministic");let fixtureRandom=1;
if(deterministic){const secure=globalThis.crypto;Object.defineProperty(globalThis,"crypto",{value:new Proxy(secure,{get(target,key){if(key==="getRandomValues")return bytes=>{for(let i=0;i<bytes.length;i++){fixtureRandom^=fixtureRandom<<13;fixtureRandom^=fixtureRandom>>>17;fixtureRandom^=fixtureRandom<<5;bytes[i]=fixtureRandom&255;}return bytes;};const value=target[key];return typeof value==="function"?value.bind(target):value;}})});}
const reindex = process.argv.includes("--reindex");
const recallQueries = Number(process.argv.find(arg => arg.startsWith("--recall-queries="))?.slice(17) ?? 0);
if (!Number.isSafeInteger(recallQueries) || recallQueries < 0 || recallQueries > 20) throw new Error("recall-queries must be 0..20");
const coarseProbes=Number(process.argv.find(arg=>arg.startsWith("--coarse-probes="))?.slice(16)??0);
if(!Number.isInteger(coarseProbes)||coarseProbes<0||coarseProbes>64)throw new Error("invalid coarse-probes");
const pagedProjections = process.argv.includes("--paged-projections");
const pagedText = pagedProjections || process.argv.includes("--paged-text");
const dimensions = Number(process.argv.find(arg => arg.startsWith("--dimensions="))?.slice(13) ?? 384);
if (!Number.isSafeInteger(dimensions) || dimensions < 2 || dimensions > 4096) throw new Error("dimensions must be 2..4096");
const embeddingPath=process.argv.find(arg=>arg.startsWith("--embedding-fixture="))?.slice(20);
const embeddingBytes=embeddingPath?await readFile(embeddingPath):null;
const embeddingFixture=embeddingBytes?JSON.parse(embeddingBytes):null;
if(embeddingFixture){for(const values of[embeddingFixture.documents,embeddingFixture.queries]){if(!Array.isArray(values)||!values.length||values.some(vector=>!Array.isArray(vector)||vector.length!==dimensions||vector.some(value=>!Number.isFinite(value))||!vector.some(value=>value!==0)))throw Error("InvalidEmbeddingFixture");}}
function normalizedFixture(vector){const norm=Math.sqrt(vector.reduce((sum,value)=>sum+value*value,0));return vector.map(value=>Math.fround(value/norm));}
// Deterministic unit vectors; these measurements exclude model latency.
function vectorFor(id) {
  if(embeddingFixture){if(!embeddingFixture.documents[id])throw Error("EmbeddingFixtureDocumentMissing");return normalizedFixture(embeddingFixture.documents[id]);}
  let seed = id + 1;
  const values = Array.from({length: dimensions}, () => {
    seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
    return (seed / 4294967296) * 2 - 1;
  });
  const norm = Math.sqrt(values.reduce((sum, v) => sum + v*v, 0));
  return values.map(v => Math.fround(v / norm));
}
function projections(id) {
  return pagedProjections ? {embedding: vectorFor(id), sparse_embedding: {durable: 1, [`term${id % 97}`]: 2}} : {};
}
const bodyBytes = Number(process.argv.find(arg => arg.startsWith("--body-bytes="))?.slice(13) ?? 0);
if (!Number.isSafeInteger(bodyBytes) || bodyBytes < 0 || bodyBytes > 256 * 1024) throw new Error("body-bytes must be 0..262144");
const sizes = (process.argv.find(arg => arg.startsWith("--sizes="))?.slice(8) ?? "100,1000,5000").split(",").map(Number);
const output = process.argv.find(arg => arg.startsWith("--output="))?.slice(9) ?? "dist/qualification/local-capacity.json";
const maxPutBytes = Number(process.argv.find(arg => arg.startsWith("--max-put-bytes="))?.slice(16) ?? 4 * 1024 * 1024);
const maxCompactionDocuments = Number(process.argv.find(arg => arg.startsWith("--compaction-documents="))?.slice(23) ?? 1000);
const recycleAtBytes = Number(process.argv.find(arg => arg.startsWith("--recycle-at-bytes="))?.slice(19) ?? 24 * 1024 * 1024);
const publicationRecords = Number(process.argv.find(arg => arg.startsWith("--publication-records="))?.slice(22) ?? 16);
if(embeddingFixture&&(recallQueries>embeddingFixture.queries.length||sizes.some(size=>size>embeddingFixture.documents.length)))throw Error("EmbeddingFixtureTooSmall");
if (!sizes.length || sizes.some(size => !Number.isSafeInteger(size) || size < 1 || size > 100000)) throw new Error("sizes must be integers from 1 to 100000");
if (!Number.isSafeInteger(maxPutBytes) || maxPutBytes < 1 || maxPutBytes > 8 * 1024 * 1024 || !Number.isSafeInteger(maxCompactionDocuments) || maxCompactionDocuments < 0) throw new Error("invalid capacity profile");
if (!Number.isSafeInteger(recycleAtBytes) || recycleAtBytes < 0 || recycleAtBytes > 32 * 1024 * 1024) throw new Error("invalid recycling threshold");
if (!Number.isSafeInteger(publicationRecords) || publicationRecords < 1 || publicationRecords > 256) throw new Error("invalid publication prefix");
const wasmPath=process.argv.find(arg=>arg.startsWith("--wasm="))?.slice(7);
const bytes = await readFile(wasmPath??new URL("../dist/antfly-serverless.wasm", import.meta.url));
const module = await WebAssembly.compile(bytes);
const readerPath=process.argv.find(arg=>arg.startsWith("--reader-wasm="))?.slice(14),readerBytes=readerPath?await readFile(readerPath):null,readerModule=readerBytes?await WebAssembly.compile(readerBytes):null;
const reads=new Set(["lookup","list","count","status","search","latest"]);
const report = {reader_wasm_sha256:readerBytes?createHash("sha256").update(readerBytes).digest("hex"):null,reader_working_set_bytes:readerBytes?8*1024*1024:null,deterministic_fixture:deterministic,profile: "local native WASM/R2 contract fixture", wasm_sha256: createHash("sha256").update(bytes).digest("hex"),
  embedding_fixture_sha256:embeddingBytes?createHash("sha256").update(embeddingBytes).digest("hex"):null, embedding_model:embeddingFixture?.model??null, timestamp: new Date().toISOString(), max_put_bytes: maxPutBytes, compaction_max_documents: maxCompactionDocuments, recycle_at_bytes: recycleAtBytes,
  publication_records: publicationRecords, reindex, recall_queries: recallQueries, paged_projections: pagedProjections, dimensions: pagedProjections ? dimensions : null, paged_text: pagedText, body_bytes: bodyBytes, clock: deterministic?"logical per-request fixture clock":"monotonic epoch fixture", total_isolate_memory: null, cases: []};
for (const count of sizes) {
  const bucket = new Bucket(), stages = [];
  let group, cache, host, hostRole="builder", accepted = 0, recycled = 0;
  const recall = [];let routingShape;
  const epoch = deterministic?1700000000000:Date.now(), monotonic = performance.now();let fixtureTick=0;if(deterministic)fixtureRandom=(count+1)>>>0;
  // The benchmark measures memory and traffic, not clock recovery. Clock
  // rollback is covered by fault tests; avoid NTP changes poisoning a long run.
  const clock = () => epoch + (deterministic?fixtureTick:Math.floor(performance.now() - monotonic));
  async function reopen(cold = true) {
    if (cold || !cache) cache = createImmutableReadCache();
    const raw = createR2Transport({database: bucket}, {maxPutBytes});
    // The in-process fixture runs serially. Real transactional guarded writes
    // and stale-owner rejection are covered by workerd, not this capacity run.
    const transport = async (request, body) => {
      if (request.guard_key != null) {
        assert.ok(request.key.endsWith("/REINDEX") && request.guard_key === request.key.slice(0, -7) + "HEAD");
        if (bucket.objects.get(request.guard_key)?.etag !== request.guard_etag) throw new ObjectStoreError(2, "stale guard");
      }
      const {guard_key, guard_etag, ...plain} = request;
      return raw(plain, body);
    };
    host = await instantiateHost(hostRole==="reader"?readerModule:module, cache.wrap(transport, "benchmark"), {
      clock,
      onMetrics(sample) { group.samples.push({...sample, cache_retained_bytes: cache.retainedBytes()}); },
    });
  }
  async function call(op, fields = {}) {
    const role=readerModule&&reads.has(op)?"reader":"builder";if(role!==hostRole){hostRole=role;host=null;}
    if (!host) await reopen(false);
    try { return await host.request({op, bucket: "database", prefix: "benchmark", namespace: `capacity/${count}`, paged_text: pagedText, paged_projections: pagedProjections, ...(hostRole==="reader"?{working_set_bytes:8*1024*1024}:{}), ...fields}); }
    finally { fixtureTick++;if (recycleAtBytes && host.memoryBytes() >= recycleAtBytes) { host = null; recycled++; } }
  }
  async function stage(name, body) {
    group = {name, samples: []};
    const started = performance.now();
    const before = {gets: bucket.reads, get_bytes: bucket.readBytes, puts: bucket.writes};
    try { await body(); group.ok = true; }
    catch (error) { group.ok = false; group.error = error.message; }
    group.duration_ms = performance.now() - started;
    group.gets = bucket.reads - before.gets; group.get_bytes = bucket.readBytes - before.get_bytes; group.puts = bucket.writes - before.puts;
    group.calls = group.samples.length;
    group.max_linear_bytes = Math.max(0, ...group.samples.map(sample => sample.wasm_linear_bytes));
    group.max_working_bytes = Math.max(0, ...group.samples.map(sample => sample.working_set_peak_bytes ?? 0));
    group.max_operations = Math.max(0, ...group.samples.map(sample => sample.operations));
    group.transferred_bytes = group.samples.reduce((sum, sample) => sum + sample.transferred_bytes, 0);
    group.decoded_manifest_cache_hits=group.samples.reduce((n,sample)=>n+(sample.decoded_manifest_cache_hits??0),0);
    group.max_decoded_manifest_cache_bytes=Math.max(0,...group.samples.map(sample=>sample.decoded_manifest_cache_bytes??0));
    delete group.samples; stages.push(group); return group.ok;
  }
  await reopen();
  const complete = await stage("ingest-and-publication", async () => {
    const mutationRecords = pagedProjections ? Math.max(1, Math.min(64, Math.floor(128000 / (dimensions * 22 + bodyBytes + 512)))) : 128;
    for (let offset = 0; offset < count; offset += mutationRecords) {
      await call("mutate", {operation_id: `batch/${offset}`, mutations: Array.from({length: Math.min(mutationRecords, count-offset)}, (_, i) => ({kind: "upsert", doc_id: `doc/${String(offset+i).padStart(6,"0")}`,
        body: JSON.stringify({text: `durable message ${offset+i} ` + "bounded index body ".repeat(12), padding: "x".repeat(bodyBytes), read: false, ...projections(offset+i)})}))});
      accepted = Math.min(count, offset + mutationRecords);
      let drained = false;
      for (let i = 0; i < 128; i++) if (!(await call("publish", {publication_records: publicationRecords})).has_more) {drained = true; break;}
      assert.ok(drained, "bounded publication must converge");
    }
  });
  let publishedLsn;
  await stage("accepted-visibility", async () => {
    const status = await call("status"); publishedLsn = status.published_lsn;
    assert.equal((await call("count")).count, String(accepted), "publication rejection must preserve accepted WAL visibility");
  });
  if (complete) {
    for(const object of bucket.objects.values()){
      if(object.body[0]!==123)continue;try{const root=JSON.parse(new TextDecoder().decode(object.body));if(root.kind==="vector_segment"&&root.docs?.records===count)routingShape={format:root.format,coarse_centers:root.global_centers?.records??0,routes:root.centroids?.records,shards:root.directory?.records};}catch{}
    }
    await stage("cold-query", async () => {
      await reopen(); assert.equal((await call("count")).count, String(count));
      assert.equal((await call("search", {text: "durable", limit: 10})).hits.length, Math.min(10, count));
      assert.ok((await call("lookup", {doc_id: "doc/000000"})).body);
    });
    if (pagedProjections) {
      await stage("cold-vector-query", async () => {
        for (const id of [...new Set([0, Math.floor(count / 2), count - 1])]) {
          await reopen();
          const result = await call("search", {mode: "vector", vector: vectorFor(id), coarse_probes:coarseProbes, limit: 10, num_probes: 8});
          assert.equal(result.hits[0]?.doc_id, `doc/${String(id).padStart(6,"0")}`, "native routing must recover the exact self-neighbor");
        }
      });
      if (recallQueries) await stage("vector-recall", async () => {
        const corpus = Array.from({length: count}, (_, i) => vectorFor(i));
        for (let q = 0; q < recallQueries; q++) {
          const query = embeddingFixture?normalizedFixture(embeddingFixture.queries[q]):vectorFor(count + q);
          const exact = corpus.map((v, i) => ({i, score: v.reduce((sum, value, d) => sum + value * query[d], 0)})).sort((l, r) => r.score-l.score || l.i-r.i).slice(0, 10);
          const expected = new Set(exact.map(({i}) => `doc/${String(i).padStart(6,"0")}`));
          for (const probes of [8, 32, "all"]) {
            await reopen();
            const beforeReads=bucket.reads,beforeBytes=bucket.readBytes;
            const result = await call("search", {mode: "vector", coarse_probes:coarseProbes, vector: query, limit: 10, num_probes: probes === "all" ? 2 : probes, search_effort: probes === "all" ? 1 : null});
            if(probes === "all")assert.equal(result.hits.filter(hit=>expected.has(hit.doc_id)).length,expected.size,"full-effort vector search must match brute-force cosine neighbors");
            recall.push({coarse_probes:probes==="all"?0:coarseProbes,r2_gets:bucket.reads-beforeReads,r2_bytes:bucket.readBytes-beforeBytes,query: q, num_probes: probes, search_effort: probes === "all" ? 1 : null, recall_at_10: result.hits.filter(hit => expected.has(hit.doc_id)).length / expected.size});
          }
        }
      });
      await stage("cold-sparse-query", async () => {
        await reopen();
        assert.equal((await call("search", {mode: "sparse", sparse: [{term: "durable", weight: 1}], limit: 10})).hits.length, Math.min(10, count));
      });
    }
    await stage("warm-query", async () => {
      for (let i = 0; i < 10; i++) assert.ok((await call("lookup", {doc_id: "doc/000000"})).body);
    });
    await stage("cold-point-updates", async () => {
      await reopen();
      const ids = [...new Set([0, Math.floor(count / 2), count - 1])];
      await call("mutate", {operation_id: "point-edits", mutations: ids.map(id => ({kind: "upsert", doc_id: `doc/${String(id).padStart(6,"0")}`, body: JSON.stringify({text: "changedneedle durable", padding: "y".repeat(bodyBytes), ...projections(id)})}))});
      for (let i = 0; i < 128; i++) if (!(await call("publish", {publication_records: publicationRecords})).has_more) break;
      await reopen();
      assert.equal((await call("search", {text: "changedneedle", limit: 10})).hits.length, ids.length);
      assert.equal((await call("count")).count, String(count));
    });
    await stage("cold-delete", async () => {
      await call("mutate", {operation_id: "point-delete", mutations: [{kind: "delete", doc_id: "doc/000000"}]});
      for (let i = 0; i < 128; i++) if (!(await call("publish", {publication_records: publicationRecords})).has_more) break;
      await reopen();
      assert.equal((await call("count")).count, String(count - 1));
      assert.equal((await call("lookup", {doc_id: "doc/000000"})).body, null);
    });
    if (reindex) await stage("cold-reindex", async () => {
      const start = await call("reindex");
      assert.ok(start.job_id);
      let done = false;
      for (let step = 0; step < Math.ceil(count / publicationRecords) + 32; step++) {
        await reopen();
        if (!(await call("reindex", {reindex_action: "step", reindex_job_id: start.job_id, reindex_records: Math.min(64, publicationRecords)})).has_more) {done = true; break;}
      }
      assert.ok(done, "bounded reindex must converge after cold restarts");
      await reopen();
      assert.equal((await call("count")).count, String(count - 1));
      assert.equal((await call("search", {text: "changedneedle"})).hits.length, count === 1 ? 0 : [...new Set([0, Math.floor(count / 2), count - 1])].length - 1);
    });
    await stage("compaction", async () => { await call("compact", {compaction_max_documents: maxCompactionDocuments}); assert.equal((await call("count")).count, String(count - 1)); });
  }
  const result = {documents: count, accepted_documents: accepted, published_lsn: publishedLsn, recycled_instances: recycled, stages, routing_shape:routingShape,vector_recall: recall, objects: bucket.objects.size};
  report.cases.push(result); console.log(JSON.stringify(result));
  await mkdir(dirname(output), {recursive: true}); await writeFile(output, JSON.stringify(report, null, 2));
}
console.log(`Saved ${output}`);
