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
import {createR2Transport} from "../../../zig/lib/objectstore/wasm/r2.mjs";
import {createImmutableReadCache} from "../../../zig/lib/objectstore/wasm/cache.mjs";

const sizes = (process.argv.find(arg => arg.startsWith("--sizes="))?.slice(8) ?? "100,1000,5000").split(",").map(Number);
const output = process.argv.find(arg => arg.startsWith("--output="))?.slice(9) ?? "dist/qualification/local-capacity.json";
const maxPutBytes = Number(process.argv.find(arg => arg.startsWith("--max-put-bytes="))?.slice(16) ?? 4 * 1024 * 1024);
const maxCompactionDocuments = Number(process.argv.find(arg => arg.startsWith("--compaction-documents="))?.slice(23) ?? 1000);
const recycleAtBytes = Number(process.argv.find(arg => arg.startsWith("--recycle-at-bytes="))?.slice(19) ?? 24 * 1024 * 1024);
const publicationRecords = Number(process.argv.find(arg => arg.startsWith("--publication-records="))?.slice(22) ?? 16);
if (!sizes.length || sizes.some(size => !Number.isSafeInteger(size) || size < 1 || size > 10000)) throw new Error("sizes must be integers from 1 to 10000");
if (!Number.isSafeInteger(maxPutBytes) || maxPutBytes < 1 || maxPutBytes > 8 * 1024 * 1024 || !Number.isSafeInteger(maxCompactionDocuments) || maxCompactionDocuments < 0) throw new Error("invalid capacity profile");
if (!Number.isSafeInteger(recycleAtBytes) || recycleAtBytes < 0 || recycleAtBytes > 32 * 1024 * 1024) throw new Error("invalid recycling threshold");
if (!Number.isSafeInteger(publicationRecords) || publicationRecords < 1 || publicationRecords > 256) throw new Error("invalid publication prefix");
const bytes = await readFile(new URL("../dist/antfly-serverless.wasm", import.meta.url));
const module = await WebAssembly.compile(bytes);
const report = {profile: "local native WASM/R2 contract fixture", wasm_sha256: createHash("sha256").update(bytes).digest("hex"),
  timestamp: new Date().toISOString(), max_put_bytes: maxPutBytes, compaction_max_documents: maxCompactionDocuments, recycle_at_bytes: recycleAtBytes,
  publication_records: publicationRecords, clock: "monotonic epoch fixture", total_isolate_memory: null, cases: []};
for (const count of sizes) {
  const bucket = new Bucket(), stages = [];
  let group, cache, host, accepted = 0, recycled = 0;
  const epoch = Date.now(), monotonic = performance.now();
  // The benchmark measures memory and traffic, not clock recovery. Clock
  // rollback is covered by fault tests; avoid NTP changes poisoning a long run.
  const clock = () => epoch + Math.floor(performance.now() - monotonic);
  async function reopen(cold = true) {
    if (cold || !cache) cache = createImmutableReadCache();
    host = await instantiateHost(module, cache.wrap(createR2Transport({database: bucket}, {maxPutBytes}), "benchmark"), {
      clock,
      onMetrics(sample) { group.samples.push({...sample, cache_retained_bytes: cache.retainedBytes()}); },
    });
  }
  async function call(op, fields = {}) {
    if (!host) await reopen(false);
    try { return await host.request({op, bucket: "database", prefix: "benchmark", namespace: `capacity/${count}`, ...fields}); }
    finally { if (recycleAtBytes && host.memoryBytes() >= recycleAtBytes) { host = null; recycled++; } }
  }
  async function stage(name, body) {
    group = {name, samples: []};
    const before = {gets: bucket.reads, get_bytes: bucket.readBytes, puts: bucket.writes};
    try { await body(); group.ok = true; }
    catch (error) { group.ok = false; group.error = error.message; }
    group.gets = bucket.reads - before.gets; group.get_bytes = bucket.readBytes - before.get_bytes; group.puts = bucket.writes - before.puts;
    group.calls = group.samples.length;
    group.max_linear_bytes = Math.max(0, ...group.samples.map(sample => sample.wasm_linear_bytes));
    group.max_working_bytes = Math.max(0, ...group.samples.map(sample => sample.working_set_peak_bytes ?? 0));
    group.max_operations = Math.max(0, ...group.samples.map(sample => sample.operations));
    group.transferred_bytes = group.samples.reduce((sum, sample) => sum + sample.transferred_bytes, 0);
    delete group.samples; stages.push(group); return group.ok;
  }
  await reopen();
  const complete = await stage("ingest-and-publication", async () => {
    for (let offset = 0; offset < count; offset += 128) {
      await call("mutate", {operation_id: `batch/${offset}`, mutations: Array.from({length: Math.min(128, count-offset)}, (_, i) => ({kind: "upsert", doc_id: `doc/${String(offset+i).padStart(6,"0")}`,
        body: JSON.stringify({text: `durable message ${offset+i} ` + "bounded index body ".repeat(12), read: false})}))});
      accepted = Math.min(count, offset + 128);
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
    await stage("cold-query", async () => {
      await reopen(); assert.equal((await call("count")).count, String(count));
      assert.equal((await call("search", {text: "durable", limit: 10})).hits.length, Math.min(10, count));
      assert.ok((await call("lookup", {doc_id: "doc/000000"})).body);
    });
    await stage("warm-query", async () => {
      for (let i = 0; i < 10; i++) assert.ok((await call("lookup", {doc_id: "doc/000000"})).body);
    });
    await stage("compaction", async () => { await call("compact", {compaction_max_documents: maxCompactionDocuments}); assert.equal((await call("count")).count, String(count)); });
  }
  const result = {documents: count, accepted_documents: accepted, published_lsn: publishedLsn, recycled_instances: recycled, stages, objects: bucket.objects.size};
  report.cases.push(result); console.log(JSON.stringify(result));
  await mkdir(dirname(output), {recursive: true}); await writeFile(output, JSON.stringify(report, null, 2));
}
console.log(`Saved ${output}`);
