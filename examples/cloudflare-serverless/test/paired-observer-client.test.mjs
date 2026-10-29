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

import{test}from'node:test';import assert from'node:assert/strict';import{reconcileObserverSource}from'../qualification/paired-observer-client.mjs';import{attemptIdentity}from'../qualification/invocation-accounting.mjs';
const source='antfly-1278-g-fixture-1-b',version=crypto.randomUUID(),counterVersion=crypto.randomUUID(),captureId=crypto.randomUUID(),attempt=crypto.randomUUID(),witnesses={before:'b'.repeat(64),after:'c'.repeat(64)},root={signal:'traces',script:source,version_id:version,start:1500,end:1600,handler:'fetch',outcome:'ok',terminal:true,invocation_id_sha256:'a'.repeat(64),trace_id_sha256:'d'.repeat(64),span_id_sha256:'e'.repeat(64),attempt_id_sha256:attemptIdentity(attempt),event:{outcome:'ok',cpuTime:1,wallTime:100}},scope={source,version,counterVersion,captureId,witnesses,ledger:{schema:1,overflow:false,attempts:[{id:attempt,finished_at:1700,status:200,version_id:version}]}};
function snapshots({quiet=false}={}){const delivery=crypto.randomUUID(),r=(timestamp,attempt_id_sha256)=>({script:source,version_id:version,timestamp,attempt_id_sha256,event:{outcome:'ok',cpuTime:1,wallTime:100}}),objects=[{key:'probe-v1/'+captureId+'/counter/'+delivery+'.json',value:{schema:1,qualification:false,source:'provider-tail-dispatch',producer:source,delivery_id:delivery,received_at:3000,collector_version:counterVersion,count:quiet?2:3,records:[r(1000,witnesses.before),...quiet?[]:[r(1500,attemptIdentity(attempt))],r(2000,witnesses.after)]}},{key:'probe-v1/'+captureId+'/batches/a.json',value:{rows:quiet?[]:[root]}}];return[62000,93000].map(t=>({qualification:false,capture_id:captureId,collector_version:counterVersion,snapshot_started_at:t,snapshot_finished_at:t+100,objects:structuredClone(objects)}));}
test('representative capture proves canonical terminals, independent native counts and every physical app attempt',async()=>{const [a,b]=snapshots(),result=await reconcileObserverSource(a,b,scope);assert.equal(result.complete,true);assert.equal(result.physical_attempts,1);assert.equal(result.native_terminals,1);assert.equal(result.terminals,1);const lost=structuredClone(b);lost.objects[1].value.rows=[];assert.equal((await reconcileObserverSource(a,lost,scope)).complete,false);assert.equal((await reconcileObserverSource(a,b,{...scope,ledger:{...scope.ledger,attempts:[]}})).complete,false);});
test('quiet producer coverage needs both native boundary witnesses and stable observations',async()=>{const[a,b]=snapshots({quiet:true}),s={...scope,ledger:undefined};assert.equal((await reconcileObserverSource(a,b,s)).complete,true);const missing=structuredClone(b);missing.objects[0].value.records.pop();assert.equal((await reconcileObserverSource(a,missing,s)).complete,false);});
test('recovered memory errors, invalid terminal metadata and late mutations cannot pass the rehearsal',async()=>{for(const edit of[b=>{b.objects[0].value.records[1].event.logs=[{message:[JSON.stringify({event:'antfly.resource_failure',failure_code:'SQLITE_NOMEM'})]}];},b=>{delete b.objects[1].value.rows[0].trace_id_sha256;},b=>{b.objects[1].value.rows[0].event.cpuTime=99;}]){const[a,b]=snapshots();edit(b);assert.equal((await reconcileObserverSource(a,b,scope)).complete,false);}});
