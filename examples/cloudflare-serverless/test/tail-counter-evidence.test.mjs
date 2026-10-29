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

import{test}from'node:test';import assert from'node:assert/strict';import{reconcileTailCounter,reconcileTailCounterSnapshots}from'../qualification/tail-counter-evidence.mjs';
const source='antfly-1278-telemetry-fixture-source',version=crypto.randomUUID(),counterVersion=crypto.randomUUID(),captureId=crypto.randomUUID(),timeframe={from:1000,to:2000},scope={source,version,counterVersion,captureId,timeframe,settledAt:62000};
const record=(timestamp=1500)=>({script:source,version_id:version,timestamp,event:{outcome:'ok',cpuTime:1,wallTime:10}});
function delivery(records=[record()]){const id=crypto.randomUUID();return{key:'probe-v1/'+captureId+'/counter/'+id+'.json',value:{schema:1,qualification:false,source:'provider-tail-dispatch',producer:source,delivery_id:id,collector_version:counterVersion,received_at:3000,count:records.length,records}};}
const root=()=>({script:source,version_id:version,start:1500,terminal:true,invocation_id_sha256:'a'.repeat(64)});
test('unfiltered native tail deliveries independently reconcile exact provider root totals',()=>{const result=reconcileTailCounter([delivery()],[root()],scope);assert.equal(result.complete,true);assert.equal(result.count,1);assert.equal(result.sample_interval,undefined);assert.equal(result.sampling_contract,'unfiltered-one-per-producer-invocation');});
test('native counter loss, replay, duplicate identities, wrong versions and scopes fail closed',()=>{assert.equal(reconcileTailCounter([],[root()],scope).complete,false);for(const change of [d=>{d.value.count=2;},d=>{d.value.records[0].script='other';},d=>{d.value.collector_version=crypto.randomUUID();},d=>{d.value.records[0].version_id=crypto.randomUUID();},d=>{d.value.records[0].timestamp=1501;}]){const d=delivery();change(d);assert.equal(reconcileTailCounter([d],[root()],scope).complete,false);}const d=delivery();assert.equal(reconcileTailCounter([d,d],[root()],scope).complete,false);assert.equal(reconcileTailCounter([d],[root(),root()],scope).complete,true);assert.equal(reconcileTailCounter([d],[root(),{...root(),start:1501}],scope).complete,false);assert.equal(reconcileTailCounter([delivery(),delivery()],[root()],scope).complete,false);});
test('quiet populations require matched before and after native source witnesses',()=>{assert.equal(reconcileTailCounter([],[],scope).complete,false);const before='b'.repeat(64),after='c'.repeat(64),d=delivery([{...record(900),attempt_id_sha256:before},{...record(2100),attempt_id_sha256:after}]),s={...scope,witnesses:{before,after}};assert.equal(reconcileTailCounter([d],[],s).complete,true);d.value.records[1].attempt_id_sha256=before;assert.equal(reconcileTailCounter([d],[],s).complete,false);});
test('native tail counters keep recovered resource failures and truncation visible',()=>{const d=delivery();d.value.records[0].event.logs=[{message:[JSON.stringify({event:'antfly.resource_failure',failure_code:'SQLITE_NOMEM'})]}];assert.equal(reconcileTailCounter([d],[root()],scope).resource_failures,1);d.value.records[0].event.truncated=true;assert.equal(reconcileTailCounter([d],[root()],scope).complete,false);});

test('closed native populations require two separated immutable observations and reject late delivery',async()=>{
 const d=delivery(),batch={key:'probe-v1/'+captureId+'/batches/a.json',value:{rows:[root()]}},snapshot=t=>({qualification:false,capture_id:captureId,collector_version:counterVersion,snapshot_started_at:t,snapshot_finished_at:t+100,objects:[d,batch]});const first=snapshot(62000),second=snapshot(92100);
 assert.equal((await reconcileTailCounterSnapshots(first,second,scope)).complete,true);
 assert.equal((await reconcileTailCounterSnapshots(first,{...second,snapshot_started_at:92000},scope)).complete,false);
 const late=structuredClone(second);late.objects.push(delivery());assert.equal((await reconcileTailCounterSnapshots(first,late,scope)).complete,false);
 const forged=structuredClone(second);forged.objects.push(forged.objects[0]);assert.equal((await reconcileTailCounterSnapshots(first,forged,scope)).complete,false);
});
