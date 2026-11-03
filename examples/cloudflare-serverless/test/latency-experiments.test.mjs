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

import{test}from'node:test';import assert from'node:assert/strict';import{AsyncLocalStorage}from'node:async_hooks';import{readFile}from'node:fs/promises';
import{FreshReadAdmissions}from'../fresh-read-admissions.mjs';import{OptimisticWal}from'../optimistic-wal.mjs';import{R2CoordinatorStore,encode}from'../r2-coordinator-store.mjs';import{Bucket}from'./object-fixture.mjs';
const module=await WebAssembly.compile(await readFile(new URL('../dist/antfly-serverless.wasm',import.meta.url))),text=new TextEncoder();
test('early read plans resume in each submitting context and retain final drain admission',async()=>{
 const context=new AsyncLocalStorage(),cohorts=new FreshReadAdmissions({delayMs:0});let release,ready;const done=new Promise(r=>release=r),early=new Promise(r=>ready=r),seen=[];
 const read=async(ids,close,publish)=>{close();publish({value:7});await done;return{value:7};};
 const calls=['a','b'].map(label=>context.run(label,()=>cohorts.run('same',read,label,{untilAdmission:true,onPrepared:view=>{seen.push([context.getStore(),view.value]);if(seen.length===2)ready();}})));
 await early;assert.deepEqual(seen,[['a',7],['b',7]]);let settled=false;calls[0].then(()=>settled=true);await new Promise(r=>setTimeout(r,0));assert.equal(settled,false);release();await Promise.all(calls);
});
test('optimistic WAL hints own bytes, require an existing live pin and expire without authority claims',()=>{
 let now=1000;const cache=new OptimisticWal({clock:()=>now}),view={entries:[['wal',{etag:'one',body:text.encode('wal'),contentType:null}],['p/READ_PINS/1',{etag:'pin',body:text.encode(String(BigInt(now+300000)*1000000n)),contentType:null}]]};cache.remember(view,'wal');view.entries[0][1].body.fill(0);assert.equal(new TextDecoder().decode(cache.prepare().entries[0][1].body),'wal');const copy=cache.prepare();copy.entries.length=0;assert.equal(cache.prepare().entries.length,2);now+=120000;assert.equal(cache.prepare(),null);cache.remember({entries:[]},'wal');assert.equal(cache.prepare(),null);
});
test('native WAL finalization rejects changed and absent preparation dependencies without publishing a receipt',async()=>{
 const bucket=new Bucket(),store=new R2CoordinatorStore(module,bucket,'wal-dependencies'),source={etag:'source',body:text.encode('1'),contentType:null};await store.transaction(tx=>tx.put('head',source));
 const make=dependencies=>({dependencies,wal_key:'wal',expected_etag:null,value:encode({etag:'wal-one',body:text.encode('1'),contentType:null}),publication_key:'pub',watermark:'1',now_ms:Date.now(),id:'job'});
 const stale=await store.commitWal(make([{key:'head',value:encode({...source,etag:'old'})}]));assert.equal(stale.applied,false);assert.equal(await store.transaction(tx=>tx.get('wal')),undefined);
 assert.equal((await store.commitWal(make([{key:'missing',value:encode(source)}]))).applied,false);
 assert.equal((await store.commitWal(make([{key:'head',value:encode(source)},{key:'missing',value:null}]))).applied,true);
 await assert.rejects(store.commitWal(make(Array.from({length:5},(_,i)=>({key:'x'+i,value:null})))),/InvalidCoordinatorWalCommit/);
});

test('fused read admission returns exact rows and selected pins in one native observation',async()=>{
 const bucket=new Bucket(),samples=[],store=new R2CoordinatorStore(module,bucket,'fused-native',{inlineReadSet:{keys:['head','wal'],prefix:'pins/'},onMetrics:sample=>samples.push(sample)});
 await store.transaction(async tx=>{await tx.put('head','one');await tx.put('wal','tail');await tx.put('pins/one','protected');});
 samples.length=0;const result=await store.readOnly(async tx=>({rows:await tx.getMany(['head','wal']),pin:await tx.get('pins/one')}),{readKeys:['head','wal'],readPrefix:'pins/'});
 assert.deepEqual(result,{rows:['one','tail'],pin:'protected'});
 assert.deepEqual(samples.filter(sample=>sample.event==='antfly.coordinator_snapshot_native').map(sample=>sample.action),['observe_read']);
});


test('latency diagnostics distinguish timing and capacity telemetry from explicit recovered resource failures',async()=>{
 const{inspectLatencyLog}=await import('../qualification/latency-log.mjs');
 assert.equal(inspectLatencyLog(JSON.stringify({event:'antfly.operation',native_cpu_ms:2,wasm_pool_limit_bytes:33554432})).failure,null);
 for(const value of[{event:'antfly.resource_failure',failure_code:'SQLITE_NOMEM'},{event:'antfly.checkpoint_deferred',error:'Error: SQLITE_NOMEM'},Error('out of memory: SQLITE_NOMEM'),'out of memory: SQLITE_NOMEM',{error:{message:'SQLITE_NOMEM'}}])assert.equal(inspectLatencyLog(value).failure,'SQLITE_NOMEM');
 assert.equal(inspectLatencyLog({failure_code:'CPULimitExceeded'}).failure,'CPULimitExceeded');
});
