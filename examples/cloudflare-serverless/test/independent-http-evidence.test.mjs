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

import{test}from'node:test';import assert from'node:assert/strict';import{observerScopes}from'../qualification/paired-observer.mjs';import{providerAccountApi}from'../qualification/provider-account-api.mjs';import{closeIndependentEvidence}from'../qualification/independent-http-evidence.mjs';
const scopes=['antfly-1278-cold-one-1234','antfly-1278-cold-two-abcd'].flatMap(run=>['app','writer','reader','builder'].map(role=>({run,role,source:run+(role==='app'?'':'-'+role),capture_id:crypto.randomUUID()}))).map((s,i)=>({...s,binding:'PRODUCER_'+i}));
test('cold-pair export is explicit, complete and rejects mixed producers or aliases',()=>{
 const env={SOURCE_LAYOUT:'cold-pair-v1',SOURCE_SCOPES:JSON.stringify(scopes)};assert.equal(observerScopes(env).length,8);assert.throws(()=>observerScopes({...env,SOURCE_LAYOUT:undefined}),/Invalid/);for(const modify of[s=>s.pop(),s=>s[0].run='other',s=>s[0].source='production',s=>s[0].role='reader']){const bad=structuredClone(scopes);modify(bad);assert.throws(()=>observerScopes({...env,SOURCE_SCOPES:JSON.stringify(bad)}),/Invalid/);}
});
test('successful client calls cannot close missing service export or provider delivery windows',async()=>{
 const observer={id:'antfly-1278-observe-fixture',version:crypto.randomUUID(),observation_capture_id:crypto.randomUUID(),source_layout:'cold-pair-v1',scopes},reports=['baseline','candidate'].map((side,i)=>({side,run:scopes[i*4].run,source_versions:Object.fromEntries(['app','writer','reader','builder'].map(r=>[r,crypto.randomUUID()])),passed:true,completed:true,invocation_ledger:{schema:1,overflow:false,attempts:[]}}));const result=await closeIndependentEvidence({observer,reports,health:{rejections_present:false},history:[],first:{},second:{},witnesses:{},started_at:1000,finished_at:2000});assert.equal(result.complete,false);assert.equal(result.diagnostics.length,8);assert.ok(result.diagnostics.every(d=>!d.complete));assert.equal(result.full_gate_passed,false);
});
test('account API uses transient authorization once and never reads a credential file',async()=>{
 let auth=0,calls=0;const token='disposable-fixture-account-token',api=providerAccountApi('a'.repeat(32),{authenticate:async()=>{auth++;return token;},fetch:async(url,init)=>{calls++;assert.ok(url.startsWith('https://api.cloudflare.com/client/v4/accounts/'+'a'.repeat(32)));assert.equal(init.headers.Authorization,'Bearer '+token);return Response.json({success:true,result:[]});}});await api('workers/observability/destinations');await api('workers/observability/destinations');assert.equal(auth,1);assert.equal(calls,2);await assert.rejects(api('../other'),/Route/);const denied=providerAccountApi('a'.repeat(32),{authenticate:async()=>token,fetch:async()=>Response.json({success:false,error:token},{status:403})});await assert.rejects(denied('workers/observability/destinations'),e=>e.message==='ObserverApiStatus403'&&!String(e).includes(token));
});

test('all eight provider/canonical windows can close, but a missing quiet role or recovered memory fault fails',async()=>{
 const{attemptIdentity}=await import('../qualification/invocation-accounting.mjs');const observer={id:'antfly-1278-observe-fixture',version:crypto.randomUUID(),observation_capture_id:crypto.randomUUID(),source_layout:'cold-pair-v1',scopes},reports=['baseline','candidate'].map((side,i)=>({side,run:scopes[i*4].run,source_versions:Object.fromEntries(['app','writer','reader','builder'].map(r=>[r,crypto.randomUUID()])),passed:true,completed:true,invocation_ledger:{schema:1,overflow:false,attempts:[]}})),first={},second={},witnesses={};
 for(const scope of scopes){const report=reports.find(r=>r.run===scope.run),version=report.source_versions[scope.role],id=crypto.randomUUID(),before='b'.repeat(64),after='c'.repeat(64),delivery=crypto.randomUUID(),data=scope.role==='app',event={outcome:'ok',cpuTime:1,wallTime:100},record=(timestamp,attempt_id_sha256)=>({script:scope.source,version_id:version,timestamp,attempt_id_sha256,event:structuredClone(event)});witnesses[scope.source]={before,after};if(data)report.invocation_ledger.attempts.push({id,finished_at:1700,status:200,version_id:version});
 const rows=data?[{signal:'traces',script:scope.source,version_id:version,start:1500,end:1600,handler:'fetch',outcome:'ok',terminal:true,invocation_id_sha256:'a'.repeat(64),trace_id_sha256:'d'.repeat(64),span_id_sha256:'e'.repeat(64),attempt_id_sha256:attemptIdentity(id),event:structuredClone(event)}]:[],objects=[{key:'probe-v1/'+scope.capture_id+'/counter/'+delivery+'.json',value:{schema:1,qualification:false,source:'provider-tail-dispatch',producer:scope.source,delivery_id:delivery,received_at:3000,collector_version:observer.version,count:data?3:2,records:[record(1000,before),...data?[record(1500,attemptIdentity(id))]:[],record(2000,after)]}},{key:'probe-v1/'+scope.capture_id+'/batches/a.json',value:{rows}}];for(const[snapshot,time]of[[first,62000],[second,93000]])snapshot[scope.source]={qualification:false,capture_id:scope.capture_id,collector_version:observer.version,snapshot_started_at:time,snapshot_finished_at:time+100,objects:structuredClone(objects)};
 }
 const history=[1400,1700,2200].map(at=>({requested_at:at,at:at+100,status:{id:observer.id,observation_capture_id:observer.observation_capture_id,at,destinations:['logs','traces'].map(signal=>({signal,enabled:true,dataset:'opentelemetry-'+signal,last_complete:new Date(at).toISOString(),last_error:''}))}})),input={observer,reports,health:{rejections_present:false},history,first,second,witnesses,started_at:1400,finished_at:1600};assert.equal((await closeIndependentEvidence(input)).complete,true);const earlier=structuredClone(input);earlier.started_at=1500;assert.equal((await closeIndependentEvidence(earlier)).complete,true);
 const lost=structuredClone(input);lost.second[scopes[1].source].objects[0].value.records.pop();assert.equal((await closeIndependentEvidence(lost)).complete,false);
 const bad=structuredClone(input);for(const snapshots of[bad.first,bad.second]){const objects=snapshots[scopes[0].source].objects,logs=[{message:[JSON.stringify({event:'antfly.durable_storage_error',error_message:'SQLITE_NOMEM'})]}];objects[0].value.records[1].event.logs=structuredClone(logs);objects[1].value.rows[0].event.logs=structuredClone(logs);}const failure=await closeIndependentEvidence(bad);assert.equal(failure.complete,false);assert.ok(failure.diagnostics.some(d=>d.resource_failures>0));
});

test('portable evidence catalogs restore only JSON windows inside the downloaded artifact',async()=>{
 const{portableSnapshots}=await import('../qualification/independent-http-observation.mjs'),root='/artifact',source={fixture:{windows:[{path:'/artifact/observation/window.json'}]}};const portable=portableSnapshots(source,root);assert.equal(portable.fixture.windows[0].relative_path,'observation/window.json');assert.equal(portableSnapshots(portable,'/download',{restore:true}).fixture.windows[0].path,'/download/observation/window.json');for(const relative_path of['../secret.json','/secret.json','window.txt'])assert.throws(()=>portableSnapshots({fixture:{windows:[{relative_path}]}},root,{restore:true}),/PathInvalid/);assert.throws(()=>portableSnapshots({fixture:{windows:[{path:'/outside/window.json'}]}},root),/PathInvalid/);
});
test('only observability metadata is excluded from serving configuration pins',async()=>{
 const{measuredConfig}=await import('../qualification/observation-runs.mjs'),config={limits:{cpu_ms:30000},vars:{POOL:2},durable_objects:{bindings:[{name:'OWNER',class_name:'Owner'}]},observability:{enabled:false}},attached={...config,observability:{enabled:true},tail_consumers:[{service:'observer'}]};assert.deepEqual(measuredConfig(config),measuredConfig(attached));for(const change of[{limits:{cpu_ms:60000}},{vars:{POOL:3}},{durable_objects:{bindings:[]}}])assert.notDeepEqual(measuredConfig(config),measuredConfig({...attached,...change}));
});
