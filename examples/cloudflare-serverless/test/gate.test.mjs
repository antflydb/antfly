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

import{test}from'node:test';
import assert from'node:assert/strict';
import{evaluateTrial,evaluateGate}from'../qualification/gate-policy.mjs';
function report(){
 const execution_id=crypto.randomUUID(),version='01234567-0123-4567-89ab-0123456789ab',samples=[...Array.from({length:1000},()=>({phase:'read',ms:100,version})),...Array.from({length:300},()=>({phase:'write',ms:500,version}))];
 const side={execution_id,version,samples,errors:[],resource_errors:[],verification:{bodies:2052,receipts:8,passed:true},converged:true,lag:["a","b","c","d"].flatMap(tenant=>Array.from({length:60},(_,minute)=>({minute,tenant,lag:0}))),queue:Array.from({length:60},(_,minute)=>({minute,backlog_count:0,oldest_ms:0,dead_letter_count:0})),cold:Array.from({length:100},(_,epoch)=>({epoch:epoch+1,ms:1000,version,count:'512',attempts:1}))};
 const point={samples:10,baseline_recall:.9,candidate_recall:.9,baseline_gets:100,candidate_gets:100,baseline_bytes:1000,candidate_bytes:1000},points=[8,32,"all"].map(probes=>({...point,probes}));
 return{schema:1,execution_id,trial:1,documents:512,soak_ms:3600000,baseline:structuredClone(side),candidate:side,faults:{passed:true,artifact_sha256:'a'.repeat(64)},ann:{datasets:[{kind:"synthetic",documents:10000,points},{kind:"model-embeddings",documents:1000,fixture_sha256:"b".repeat(64),points}],samples:10,exact_control:true,baseline_recall:.9,candidate_recall:.9,baseline_gets:100,candidate_gets:100,baseline_bytes:1000,candidate_bytes:1000}};
}
test('gate requires three independent complete trials',()=>{const a=report();assert.equal(evaluateTrial(a).passed,true);assert.equal(evaluateGate([a]).passed,false);const b=report(),c=report();b.trial=2;c.trial=3;assert.equal(evaluateGate([a,b,c]).passed,true);assert.equal(evaluateGate([a,a,a]).passed,false);});
for(const[name,change]of[
 ['short soak',r=>r.soak_ms=60000],['missing cold evidence',r=>r.candidate.cold=[]],['duplicate cold epochs',r=>r.candidate.cold.forEach(c=>c.epoch=1)],['mixed deployment',r=>r.candidate.samples[0].version='old'],['retry-inclusive tail',r=>r.candidate.samples.filter(s=>s.phase==='read').slice(0,60).forEach(s=>s.ms=2000)],['recall regression',r=>r.ann.candidate_recall=.8],['extra ANN bytes',r=>r.ann.candidate_bytes=1001],['missing fault evidence',r=>r.faults=undefined],['resource failure',r=>r.candidate.resource_errors.push('OutOfMemory')],['growing backlog',r=>r.candidate.queue.slice(-10).forEach(s=>s.backlog_count=20)],['missing minute coverage',r=>r.candidate.lag.forEach(s=>s.minute=0)]
])test('gate rejects '+name,()=>{const r=report();change(r);assert.equal(evaluateTrial(r).passed,false);});

test('qualification endpoints deny missing credentials including Bearer undefined',async()=>{
 const{qualificationApp}=await import('../qualification/physical-epochs.mjs');const app=qualificationApp({fetch(){throw Error('unauthenticated request reached application');}});
 for(const path of['/_qualification/drain','/_qualification/queue','/_qualification/version']){
  const response=await app.fetch(new Request('https://test.invalid'+path,{method:'POST',headers:{Authorization:'Bearer undefined'}}),{ANTFLY_INCARNATION:'qualification-test'});assert.equal(response.status,401);
 }
});

test('gate lifecycle scope rejects mismatched credentials and cloud destinations',async()=>{
 const{assertGateRun,assertGateConfig}=await import('../qualification/gate-scope.mjs');const id='antfly-1278-g-owned-1-c',run={id,account:'a'.repeat(32),bucket:id,worker:id,queues:[id,id+'-dead'],dir:'/tmp/gate/owned',token_file:'/tmp/gate/owned/api-token',services:['app','writer','reader','builder'].map(role=>({role,name:id+(role==='app'?'':'-'+role)}))};
 assert.doesNotThrow(()=>assertGateRun(run,{account:run.account,dir:'/tmp/gate'}));
 for(const change of[r=>r.token_file='/tmp/account-token',r=>r.bucket='production',r=>r.services[0].name='production']){const bad=structuredClone(run);change(bad);assert.throws(()=>assertGateRun(bad),/ScopeMismatch/);}
 const service=run.services[0],config={name:service.name,account_id:run.account,r2_buckets:[{bucket_name:id}]};assert.doesNotThrow(()=>assertGateConfig(run,service,config));assert.throws(()=>assertGateConfig(run,service,{...config,account_id:'b'.repeat(32)}),/ScopeMismatch/);
});
