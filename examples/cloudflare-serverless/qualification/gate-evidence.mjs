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

// Build a content-addressed evidence manifest from completed local test logs
// and paired, deterministic native ANN reports. It performs no cloud writes.
import{readFile,writeFile}from'node:fs/promises';
import{validateAnnPair}from'./ann-evidence.mjs';
import{passingNodeTestLog}from'./gate-policy.mjs';
import{resolve}from'node:path';
import{createHash}from'node:crypto';
const gate=JSON.parse(await readFile(process.argv[2])),artifacts=[];
async function artifact(path){const bytes=await readFile(path),sha256=createHash('sha256').update(bytes).digest('hex');artifacts.push({path:resolve(path),sha256});return bytes;}
const native=(await artifact(process.argv[3])).toString(),node=(await artifact(process.argv[4])).toString(),worker=(await artifact(process.argv[5])).toString();
if(![...native.matchAll(/([0-9]+) passed; [0-9]+ skipped; 0 failed; 0 leaked/g)].some(match=>Number(match[1])>=545)||/error:|FAIL|[1-9][0-9]* leaked|[1-9][0-9]* failed/.test(native)||!passingNodeTestLog(node,249)||!passingNodeTestLog(worker,18))throw Error('complete passing test logs required');
if(!/durable hints retain the manifest and facts within escaped native root capacity/.test(node))throw Error('escaped hint root capacity regression missing');
if(!/persisted diagnostic access probes constrain scope/.test(node))throw Error('persisted diagnostic access safety regression missing');
for(const pattern of[/redundant diagnostic coverage requires uninterrupted overlap/,/overlapping diagnostic sockets repair one connection/])if(!pattern.test(node))throw Error('redundant diagnostic coverage regression missing');
for(const pattern of[/report persistence does not block fixed arrivals/,/cancelled offered workloads stop before another arrival/,/per-request cache lifetimes return before fills/,/native app metadata cache fills use only the submitting request lifetime/])if(!pattern.test(node))throw Error('request lifetime or offered workload regression missing');
for(const pattern of [/suspended host deadline returns before storage settles/,/suspended host abort returns before storage settles/,/ambiguous metadata deadlines retain native reservations/])if(!pattern.test(node))throw Error('draining deadline safety regression missing');
if(!/expired native I\/O retains reservations through RPC completion/.test(worker))throw Error('draining I/O caller-context regression missing');
if(!/constructor reset replay preserves stable warm identities/.test(node))throw Error('constructor reset replay safety regression missing');
if(!/warm unowned proof reads overlap checkpoint and tail/.test(node))throw Error('parallel read proof safety regression missing');
for(const pattern of[/bounded role caches protect data across metadata churn/,/role cache binding scopes isolate identical/,/pooled host waits out small frozen clock gaps/,/pooled host rejects a clock still behind/,/transaction queue hands off ownership before exposing/,/paired phase cancels and settles peer I\/O/,/stopped qualification clients cancel active transport/,/streaming metrics discard HTTP credentials/,/tail framing bounds buffers/,/a failed transaction owner stays pending/,/Queue metrics overload uses bounded logical retries/,/background checkpoint progress survives/,/a suspended checkpoint hint neither holds foreground/,/background checkpoint hints reject a replaced owner/,/checkpoint completion schedules the latest committed revision/,/app observers bound active namespaces/,/app observers isolate bucket bindings/,/fresh corpus retries preserve/,/corpus preparation rejects active stale consumed/,/paired corpus provenance must match/,/the paired control must also satisfy/,/bounded point reads use eight MiB kernels/,/point read capacity promotion preserves errors/])if(!pattern.test(node))throw Error('required point-read capacity regression missing');
for(const pattern of[/finalize rejects expired/,/ambiguous finalize acknowledgement/,/read-only committed snapshots/,/tentative batches reject/,/read cohorts close/,/native exact scan survives/,/native exhaustive scan ranks the stored FP32/,/exact cursor rejects/,/foreground request acquires the shared reader/,/shared writer reservations reuse matching variants/,/eight MiB observation kernels preserve/,/weighted kernel reservations bound retained memory/,/cancelled weighted reservations release/,/fresh observations retain execution capacity/,/fresh observation reuses only cache rows/,/immutable write-through admits/,/qualification rejects cancelled/])if(!pattern.test(node))throw Error('required failure/admission regression missing');
for(const pattern of[/exact scan resumes bounded/,/exact top-k deduplicates/])if(!pattern.test(native))throw Error('required native exact-scan regression missing');
for(const pattern of[/queued read cohorts close at native admission/,/read cohort capacity preserves separate mutation metadata/,/native observations close admission before transport/,/edge cache scopes require a distinct bucket incarnation/,/edge immutable cache shares bounded call admission/,/edge immutable cache bounds background fills/,/edge immutable cache survives reconstruction/,/edge immutable cache isolates namespace/,/edge immutable cache admits only confirmed/,/edge immutable cache bounds stored bodies/,/edge immutable cache outages fall back/,/edge immutable cache background writes/,/idle read observers probe one tail slot/,/actor-version ownership resumes after reconstruction/,/overlapping reconstructed owners cannot acknowledge forked roots/,/warm transport retries retain original input/,/transport replay requires a stable mutation identity/,/cold attempts never retry transport/,/committed immutable hints survive metadata eviction/,/durable hint encoding bounds compact metadata/,/authority admission bounds pending callers/,/pooled host refreshes an older I\/O context/,/queued kernel work resumes in its submitting context/,/expired read leases return recoverable admission/,/cold preflight preserves unused actors/,/local gate evidence fails closed/,/platform diagnostics preserve bounded flags/,/runtime placement separates compute generations/,/variant recycling retires compact kernels/,/publication hints retain only confirmed immutable/,/publication hints prioritize the current manifest/,/immutable read hints preserve conditional ranges/,/advisory immutable hints share the bounded row cache/,/metadata sweep persists deletion intent/,/metadata deletion intents fail closed/,/unowned recovery continuations require a committed cursor/,/one idle commit advances the native recovery floor/,/point reads keep stable bounded placement/,/warm unowned observations prefetch authenticated tails/,/native reader views authenticate local metadata/,/local read admission falls back only/,/read-only authority checks never initialize/,/metadata retirement overlaps bounded immutable proofs/])if(!pattern.test(node))throw Error('required local read/retirement regression missing');
if(!/runtime actor queues resume each admitted builder/.test(worker))throw Error('runtime actor context regression missing');
if(!/mutation groups hand each noncoalesced commit/.test(worker))throw Error('mutation group context regression missing');
if(!/transaction groups resume native work in the first pending callers context/.test(worker))throw Error('transaction owner context regression missing');
if(!/cold committed observations preserve authority without taking writer ownership/.test(node))throw Error('cold committed observation regression missing');
for(const pattern of[/cold observers authenticate sealed owners without writing or repairing hints/,/partial epoch announcement batches recover from the accepted seal/])if(!pattern.test(native))throw Error('required native recovery regression missing');
for(const pattern of[/direct app reads avoid reader actors.*\(native\)/,/direct app reads avoid reader actors/,/Cache API shares confirmed immutable bytes across actors/,/R2 authority loading admits control RPCs/,/pooled kernels preserve originating Durable Object contexts/,/qualification epochs reopen/,/SQLite migration freezes/,/R2-native coordination survives/])if(!pattern.test(worker))throw Error('required persisted integration missing');
for(const pattern of[/object-native inline read capsules preserve ordered authority/,/object-native inline read capsule budgets fall back/,/object-native inline read capsules reject ambiguous/])if(!pattern.test(native))throw Error('native read capsule regression missing');
for(const pattern of[/native inline read views avoid metadata pages/,/metadata activation qualifies full compact and observation binaries/,/paired preparation bounds unpublished data/])if(!pattern.test(node))throw Error('read capsule or bounded preparation regression missing');
for(const pattern of[/direct app reads avoid reader actors.*\(capsules\)/,/native read capsules activate after compatible readers and survive erased platform storage/])if(!pattern.test(worker))throw Error('persisted read capsule regression missing');
for(const pattern of[/late storage resource failures remain observable/,/live diagnostic coverage rejects a late resource/,/live diagnostic coverage preserves connection gaps/,/gate rejects missing gapped duplicate/,/expired read admission retries only stable warm identities/,/completed live diagnostic evidence rejects altered/])if(!pattern.test(node))throw Error('live resource diagnostic regression missing');
const baseline=JSON.parse(await readFile(gate.trials[0].baseline)),candidate=JSON.parse(await readFile(gate.trials[0].candidate)),datasets=[];
for(const kind of['synthetic','real']){
 const a=JSON.parse(await artifact(resolve(gate.dir,'ann-'+kind+'-baseline.json'))),b=JSON.parse(await artifact(resolve(gate.dir,'ann-'+kind+'-candidate.json')));
 validateAnnPair(a,b,{minimumDocuments:kind==='synthetic'?10000:1000});
 if(Boolean(a.wal_inline_tail)!==Boolean(baseline.wal_inline_tail_enabled)||Boolean(b.wal_inline_tail)!==Boolean(candidate.wal_inline_tail_enabled))throw Error('ANN WAL policy differs from the deployed profile');
 if(a.wasm_sha256!==baseline.wasm_sha256||b.wasm_sha256!==candidate.wasm_sha256||a.reader_wasm_sha256!==baseline.reader_wasm_sha256||b.reader_wasm_sha256!==candidate.reader_wasm_sha256||a.reader_working_set_bytes!==8*1024*1024||b.reader_working_set_bytes!==8*1024*1024||!a.deterministic_fixture||!b.deterministic_fixture||a.embedding_fixture_sha256!==b.embedding_fixture_sha256||a.cases.length!==1||b.cases.length!==1||a.cases[0].documents!==b.cases[0].documents)throw Error('ANN artifact/corpus mismatch');
 if(kind==='real'){const bytes=await artifact(resolve(gate.dir,'model-embeddings.json'));if(createHash('sha256').update(bytes).digest('hex')!==a.embedding_fixture_sha256||!a.embedding_model)throw Error('model embedding fixture missing');}
 const points=[];
 for(const probes of[8,32,'all']){
  const left=a.cases[0].vector_recall.filter(r=>r.num_probes===probes),right=b.cases[0].vector_recall.filter(r=>r.num_probes===probes),samples=left.length;
  if(samples<10||right.length!==samples||new Set(left.map(r=>r.query)).size!==samples||new Set(right.map(r=>r.query)).size!==samples)throw Error('insufficient ANN queries');
  if(probes==='all'&&[...left,...right].some(r=>r.recall_at_10!==1))throw Error('exact ANN control failed');
  const mean=(rows,key)=>rows.reduce((n,row)=>n+row[key],0)/rows.length;
  points.push({probes,samples,baseline_recall:mean(left,'recall_at_10'),candidate_recall:mean(right,'recall_at_10'),baseline_gets:mean(left,'r2_gets'),candidate_gets:mean(right,'r2_gets'),baseline_bytes:mean(left,'r2_bytes'),candidate_bytes:mean(right,'r2_bytes')});
 }
 datasets.push({kind:kind==='real'?'model-embeddings':'synthetic',documents:a.cases[0].documents,fixture_sha256:a.embedding_fixture_sha256,model:a.embedding_model,points});
}
const summary=datasets[0].points.find(p=>p.probes===32),faultArtifacts=artifacts.slice(0,3);
const evidence={schema:1,baseline_wasm_sha256:baseline.wasm_sha256,candidate_wasm_sha256:candidate.wasm_sha256,artifacts,faults:{passed:true,artifact_sha256:createHash('sha256').update(JSON.stringify(faultArtifacts)).digest('hex')},ann:{...summary,exact_control:true,datasets}};
await writeFile(resolve(gate.dir,'evidence.json'),JSON.stringify(evidence,null,2));console.log('Saved content-addressed gate evidence.');
