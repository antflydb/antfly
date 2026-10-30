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

import{test}from'node:test';import assert from'node:assert/strict';
import{observerScopes,fetchObserver,persistObserverTail}from'../qualification/paired-observer.mjs';import{Bucket}from'./object-fixture.mjs';
const pair='antfly-1278-g-fixture-1',scopes=['b','c'].flatMap(side=>['','-writer','-reader','-builder'].map(role=>({source:pair+'-'+side+role,capture_id:crypto.randomUUID()}))).map((s,i)=>({...s,binding:'PRODUCER_'+i})),token='local-disposable-fixture',version=crypto.randomUUID(),env=()=>({API_TOKEN:token,SOURCE_SCOPES:JSON.stringify(scopes),LOGS:new Bucket(),WORKER_VERSION:{id:version}}),req=(p,body)=>new Request('https://observer.invalid'+p,{method:'POST',headers:{Authorization:'Bearer '+token},body:JSON.stringify(body)});
const envelope=()=>({resourceSpans:scopes.map(s=>({resource:{attributes:[{key:'service.name',value:{stringValue:s.source}}]},scopeSpans:[{spans:[{traceId:'a'.repeat(32),spanId:'b'.repeat(16),startTimeUnixNano:'1000000000',endTimeUnixNano:'1010000000',attributes:[]}]}]}))});
test('paired observer isolates all eight exact producer prefixes and rejects foreign or incomplete scopes',async()=>{const e=env();assert.equal(observerScopes(e).length,8);assert.equal((await fetchObserver(req('/v1/traces',envelope()),e)).status,200);assert.equal(e.LOGS.objects.size,8);for(const s of scopes)assert.ok([...e.LOGS.objects.keys()].some(k=>k.startsWith('probe-v1/'+s.capture_id+'/')));const bad=envelope();bad.resourceSpans[7].resource.attributes[0].value.stringValue='production';assert.equal((await fetchObserver(req('/v1/traces',bad),e)).status,400);assert.equal([...e.LOGS.objects.keys()].filter(k=>k.startsWith('probe-v1/')).length,8);assert.throws(()=>observerScopes({...e,SOURCE_SCOPES:JSON.stringify(scopes.slice(1))}));assert.equal((await fetchObserver(new Request('https://observer.invalid/health'),e)).status,401);});
test('paired native tail populations retain own versions and late resource failures in separate immutable ledgers',async()=>{const e=env();await persistObserverTail(scopes.map(s=>({scriptName:s.source,scriptVersion:{id:version},eventTimestamp:1000,outcome:'ok',cpuTime:1,wallTime:20,logs:[{message:[JSON.stringify({event:'antfly.resource_failure',failure_code:'SQLITE_NOMEM'})]}]})),e);assert.equal(e.LOGS.objects.size,8);for(const object of e.LOGS.objects.values()){const v=JSON.parse(new TextDecoder().decode(object.body));assert.equal(v.count,1);assert.equal(v.foreign_events,0);assert.ok(JSON.stringify(v).includes('SQLITE_NOMEM'));}await assert.rejects(persistObserverTail([{scriptName:'foreign'}],e));});
test('private source witnesses carry no credentials and retirement allows only bounded cleanup',async()=>{const e=env(),s=scopes[1];e[s.binding]={fetch:async(url,options)=>{assert.equal(options.headers,undefined);assert.equal(new URL(url).origin,'https://qualification-witness.invalid');return new Response('not found',{status:404});}};const r=await(await fetchObserver(req('/source/'+s.source+'/counter-witness',{id:crypto.randomUUID()}),e)).json();assert.equal(r.status,404);assert.equal(r.producer,s.source);assert.match(r.attempt_id_sha256,/^[a-f0-9]{64}$/);await e.LOGS.put('owned','{}');assert.equal((await fetchObserver(req('/drain-all',{}),e)).status,404);e.RETIRE_MODE='true';assert.equal((await fetchObserver(req('/source/'+s.source+'/counter-witness',{}),e)).status,410);assert.equal((await(await fetchObserver(req('/drain-all',{}),e)).json()).deleted,1);});

test('tail rejection markers preserve fixed resource causes without leaking exception text',async()=>{
 const worker=(await import('../qualification/paired-observer-worker.mjs')).default,e=env(),put=e.LOGS.put.bind(e.LOGS);let pending;
 e.LOGS.put=async(key,...args)=>{if(key.includes('/counter/'))throw Error('SQLITE_NOMEM secret-bucket');return put(key,...args);};
 worker.tail([{scriptName:scopes[0].source,eventTimestamp:1000,outcome:'ok',cpuTime:1,wallTime:2}],e,{waitUntil(p){pending=p;}});
 await assert.rejects(pending,/ObserverTailFailed/);
 const marker=JSON.parse(new TextDecoder().decode([...e.LOGS.objects.values()][0].body));assert.equal(marker.cause,'SQLITE_NOMEM');assert.ok(!JSON.stringify(marker).includes('secret-bucket'));
});
