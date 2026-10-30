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

import{test}from'node:test';import assert from'node:assert/strict';import{rehearsalSeedResume}from'../qualification/rehearsal-seed-resume.mjs';
function fixture(){const execution=crypto.randomUUID(),runs=['baseline','candidate'].map(kind=>({id:'run-'+kind,last_execution_id:execution,last_phase:'complete',revision:'source',version:crypto.randomUUID(),wasm_sha256:'a'.repeat(64),reader_wasm_sha256:'b'.repeat(64),bundle_hashes:{app:{'worker.js':'c'.repeat(64)}},corpus:{id:'corpus'}})),report={schema:1,qualification:false,stage:'complete',fatal:'Error: RehearsalInterrupted',finished_at:Date.now(),documents:512,execution_id:execution,sides:runs.map((r,i)=>({run:r.id,version:r.version,source_revision:r.revision,execution_id:execution,wasm_sha256:r.wasm_sha256,reader_wasm_sha256:r.reader_wasm_sha256,bundle_hashes:structuredClone(r.bundle_hashes),corpus:structuredClone(r.corpus),cold:[],resource_errors:[],samples:Array.from({length:i?32:3},(_,index)=>({phase:'seed',op:'mutate',tenant:['a','b','c','d'][Math.floor(index/8)]}))}))};return{report,runs};}
test('diagnostic setup resume retains the original seed identities with asymmetric partial completion',()=>{const{report,runs}=fixture();assert.equal(rehearsalSeedResume(report,runs),report.execution_id);const previous=crypto.randomUUID();report.resumed_seed_execution_id=previous;assert.equal(rehearsalSeedResume(report,runs),previous);});
test('measured, stale, changed, resource-failed and cold-used setup reports cannot be resumed',()=>{for(const change of[(p,r)=>p.workload={},(p,r)=>p.sides[0].samples.push({phase:'read'}),(p,r)=>p.sides[0].verification={passed:true},(p,r)=>p.sides[0].resource_errors.push({code:'SQLITE_NOMEM'}),(p,r)=>r[0].cold_execution_id=crypto.randomUUID(),(p,r)=>r[0].last_execution_id=crypto.randomUUID(),(p,r)=>r[0].last_phase='rehearsal',(p,r)=>r[0].version=crypto.randomUUID(),(p,r)=>r[0].corpus.id='changed',(p,r)=>p.sides[0].wasm_sha256='d'.repeat(64),(p,r)=>p.stage='seeding']){const{report,runs}=fixture();change(report,runs);assert.throws(()=>rehearsalSeedResume(report,runs));}});
