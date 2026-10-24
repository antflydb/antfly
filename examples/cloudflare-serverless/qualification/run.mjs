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

import { readFile, writeFile } from "node:fs/promises";
import { performance } from "node:perf_hooks";
import assert from "node:assert/strict";
const manifest = process.argv[2], origin = process.argv[3];
if (!manifest || !origin?.startsWith("https://")) throw new Error("usage: node qualification/run.mjs RUN_JSON HTTPS_ORIGIN [--drain]");
const run = JSON.parse(await readFile(manifest, "utf8")), token = (await readFile(run.token_file, "utf8")).trim();
if (!run.id.startsWith("antfly-1278-") || !new URL(origin).hostname.startsWith(`${run.id}.`) || !new URL(origin).hostname.endsWith(".workers.dev")) throw new Error("qualification deployment identity mismatch");
const measurements = [], start = Date.now();
async function call(input, path = "/api") {
  let last;
  for (let attempt = 0; attempt < 30; attempt++) {
    const begin = performance.now();
    const response = await fetch(new URL(path, origin), {method: "POST", headers: {Authorization: `Bearer ${token}`, "Content-Type": "application/json"}, body: JSON.stringify(input), signal: AbortSignal.timeout(35000)});
    const body = await response.json();
    last = {status: response.status, body};
    if (attempt % 10 === 0 && response.status !== 200) console.error("qualification retry", input.op ?? path, JSON.stringify(last));
    if (response.status === 503 || (response.status === 409 && /WorkLeaseLost|WalCommitContended|CollectionLeaseLost/.test(body.error))) { await new Promise(resolve => setTimeout(resolve, 1000)); continue; }
    assert.equal(response.status, 200, JSON.stringify({status: response.status, body}));
    measurements.push({op: input.op ?? path, duration_ms: performance.now() - begin, version: response.headers.get("X-Antfly-Version")});
    return body;
  }
  throw new Error(`admission retries exhausted: ${JSON.stringify({op: input.op, last})}`);
}
if (process.argv.includes("--probe") || process.argv.includes("--status")) {
  for (const op of process.argv.includes("--status") ? ["status"] : ["status", "publish", "status"]) console.log(op, JSON.stringify(await call({op})));
  process.exit(0);
}
if (process.argv.includes("--drain")) {
  for (let i = 0; i < 10000; i++) if ((await call({}, "/_qualification/drain")).done) { console.log("disposable R2 and coordinator state drained"); process.exit(0); }
  throw new Error("cleanup limit exceeded");
}
if (process.argv.includes("--enrichment")) {
  const receipt = await call({op: "mutate", operation_id: `${run.id}/embedding`, mutations: [{kind: "upsert", doc_id: "message/a", body: JSON.stringify({text: "Cloudflare stores durable searchable messages."})}]});
  if (process.argv.includes("--reopen")) {
    const before = JSON.parse(await readFile(`${run.dir}/enrichment-report.json`, "utf8"));
    assert.equal(receipt.visibility, before.visibility, "reopen must preserve the original retry receipt");
    assert.notEqual(measurements[0].version, before.measurements[0].version, "reopen fixture requires a replacement deployment");
  }
  let ready = false;
  for (let i = 0; i < 180; i++) {
    const status = await call({op: "status"});
    if (status.published && BigInt(status.published_lsn) >= 2n && status.pending_embeddings === "0") {ready = true; break;}
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
  assert.ok(ready, "native enrichment did not converge");
  const vector = (await call({text: ["durable messages"]}, "/ai")).data[0];
  const hits = await call({op: "search", mode: "vector", vector, required_visibility: receipt.visibility, fresh: true});
  assert.ok(hits.hits.some(hit => hit.doc_id === "message/a"));
  const filename = process.argv.includes("--reopen") ? "reopen-report.json" : "enrichment-report.json";
  await writeFile(`${run.dir}/${filename}`, JSON.stringify({visibility: receipt.visibility, checks: {native_gateway_enrichment: true, fenced_vector: true, reopen: process.argv.includes("--reopen")}, measurements}, null, 2));
  console.log("live native Gateway enrichment and fenced vector search passed");
  process.exit(0);
}
const initial = {op: "mutate", operation_id: `${run.id}/initial`, mutations: Array.from({length: 300}, (_, i) => ({kind: "upsert", doc_id: `doc/${String(i).padStart(4,"0")}`, body: JSON.stringify({text: `qualification durable document ${i}`, read: false})}))};
const accepted = await call(initial);
assert.equal(accepted.committed, true);
assert.equal((await call(initial)).visibility, accepted.visibility);
assert.equal(JSON.parse((await call({op: "lookup", doc_id: "doc/0000", required_visibility: accepted.visibility})).body).read, false);
let published = false;
for (let i = 0; i < 180; i++) {
  const status = await call({op: "status"});
  if (status.published && BigInt(status.published_lsn) >= 300n) {published = true; break;}
  await new Promise(resolve => setTimeout(resolve, 1000));
}
assert.ok(published, "durable Queue publication did not converge");
assert.ok((await call({op: "search", text: "durable"})).hits.length);
await Promise.all(Array.from({length: 8}, (_, i) => call({op: "mutate", operation_id: `${run.id}/race/${i}`, mutations: [{kind: "upsert", doc_id: `race/${i}`, body: JSON.stringify({text: `racing ${i}`})}]})));
for (let i = 0; i < 30; i++) await call({op: "lookup", doc_id: `doc/${String(i % 10).padStart(4, "0")}`});
let job, complete = false;
for (let i = 0; i < 1000; i++) {
  const result = await call({op: "collect", ...(job ? {job_id: job} : {})}); job = result.job_id;
  if (i % 25 === 0) console.log("collection progress", result.phase, i);
  if (result.done) {complete = true; break;}
  if (result.delay_seconds) await new Promise(resolve => setTimeout(resolve, Math.min(60000, result.delay_seconds * 1000 + 100)));
}
assert.ok(complete, "live collection did not converge");
assert.equal((await call(initial)).visibility, accepted.visibility);
assert.ok((await call({op: "search", text: "durable"})).hits.length);
const embedding = await call({text: ["Antfly Cloudflare qualification"]}, "/ai");
assert.equal(embedding.data?.[0]?.length, 384);
assert.ok(embedding.data[0].every(Number.isFinite));
const durations = measurements.filter(item => item.op === "lookup").map(item => item.duration_ms).sort((a,b) => a-b);
const report = {run: run.id, origin, started_at: new Date(start).toISOString(), finished_at: new Date().toISOString(), checks: {idempotent_batch: true, fenced_lookup: true, publication: true, racing_writers: true, collection: true, gateway_embedding: true}, lookup_ms: {p50: durations[Math.floor(durations.length*.5)], p95: durations[Math.floor(durations.length*.95)]}, memory: {total_isolate_bytes: null, qualification: "No resource-limit error in this fixture; total isolate memory was not measured"}, measurements};
await writeFile(`${run.dir}/report.json`, JSON.stringify(report, null, 2));
console.log(JSON.stringify({...report, measurements: `${measurements.length} recorded`}, null, 2));
