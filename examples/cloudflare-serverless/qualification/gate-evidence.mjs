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
if(![...native.matchAll(/([0-9]+) passed; [0-9]+ skipped; 0 failed; 0 leaked/g)].some(match=>Number(match[1])>=545)||/error:|FAIL|[1-9][0-9]* leaked|[1-9][0-9]* failed/.test(native)||!passingNodeTestLog(node,154)||!passingNodeTestLog(worker,6))throw Error('complete passing test logs required');
for(const pattern of[/finalize rejects expired/,/ambiguous finalize acknowledgement/,/read-only committed snapshots/,/tentative batches reject/,/read cohorts close/,/native exact scan survives/,/native exhaustive scan ranks the stored FP32/,/exact cursor rejects/,/foreground request acquires the shared reader/,/fresh observation reuses only cache rows/,/immutable write-through admits/,/qualification rejects cancelled/])if(!pattern.test(node))throw Error('required failure/admission regression missing');
for(const pattern of[/exact scan resumes bounded/,/exact top-k deduplicates/])if(!pattern.test(native))throw Error('required native exact-scan regression missing');
for(const pattern of[/qualification epochs reopen/,/SQLite migration freezes/,/R2-native coordination survives/])if(!pattern.test(worker))throw Error('required persisted integration missing');
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
