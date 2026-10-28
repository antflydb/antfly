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

import{test}from'node:test';import assert from'node:assert/strict';import{resumeCoverageSeed}from'../qualification/coverage-seed-resume.mjs';
function fixture(){const execution=crypto.randomUUID(),corpus={id:crypto.randomUUID()},runs=['baseline','candidate'].map((kind,index)=>({id:'run-'+index,kind,paused:true,last_execution_id:execution,last_phase:'coverage-probe',revision:'source-'+index,corpus,version:crypto.randomUUID(),wasm_sha256:'native',reader_wasm_sha256:'reader',bundle_hashes:{app:'sealed'},runtime_placement:null})),sides=runs.map((run,i)=>({run:run.id,source_revision:run.revision,corpus,version:run.version,wasm_sha256:run.wasm_sha256,reader_wasm_sha256:run.reader_wasm_sha256,bundle_hashes:run.bundle_hashes,runtime_placement:null,cold:[],errors:[],resource_errors:[],samples:Array.from({length:i?32:31},(_,index)=>({phase:'seed',op:'mutate',tenant:['a','b','c','d'][Math.floor(index/8)],epoch:0}))}));return{runs,report:{schema:1,qualification:false,stage:'complete',fatal:'DiagnosticSetupDeadline',documents:512,workload_started_at:null,execution_id:execution,scope:'fresh-seeded diagnostic; fixture',diagnostics:{passed:true},sides}};}
test('interrupted diagnostic setup replays the original seed identity without mutating its failed evidence',()=>{const{runs,report}=fixture(),before=structuredClone({runs,report});assert.equal(resumeCoverageSeed(report,runs),report.execution_id);assert.deepEqual({runs,report},before);});
test('setup replay rejects measured, changed, resource-failed, fenced or out-of-order evidence',()=>{
 for(const mutate of[f=>f.report.workload_started_at=1,f=>f.report.workload={},f=>f.report.sides[0].samples.push({phase:'read',op:'lookup',epoch:0}),f=>f.runs[0].cold_execution_id=crypto.randomUUID(),f=>f.report.sides[1].resource_errors.push({error:'OutOfMemory'}),f=>f.report.sides[0].samples[0].tenant='d',f=>f.runs[0].revision='different',f=>f.runs[0].last_phase='workload',f=>f.report.diagnostics.passed=false,f=>f.report.sides[0].errors.push({phase:'seed',error:'Error: UnknownFailure'})]){const f=fixture();mutate(f);assert.throws(()=>resumeCoverageSeed(f.report,f.runs),/Resume/);}
});
