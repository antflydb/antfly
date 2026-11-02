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
import{exactSearch,rejectPrivateScanInput}from'../exact-search.mjs';
const env={ANTFLY_INCARNATION:'tenant-a',API_TOKEN:'local-test-key',EXACT_SCAN_INLINE_STEPS:'1'};
const query={op:'search',mode:'vector',search_effort:1,vector:[1,0],limit:2};
const state={format:1,generation:'9007199254740993',cutoff:'9007199254740994',query:Array(32).fill(1),after:'a'.repeat(72),scanned:'512',top:[{doc_id:'a',score:1,distance:.9}]};
const partial={hits:[],complete:false,exact_state:state,step_records:512,visibility:'fence'};
test('exact search authenticates portable state and hides partial heaps',async()=>{
 let clock=1000,called=0;
 const first=await exactSearch(env,query,async input=>{called++;assert.equal(input.exact_state,null);return structuredClone(partial);},{now:()=>clock});
 assert.equal(first.complete,false);assert.deepEqual(first.hits,[]);assert.equal(first.exact_state,undefined);assert.ok(first.cursor);assert.equal(called,1);
 clock++;
 const final=await exactSearch({...env}, {...query,cursor:first.cursor},async input=>{called++;assert.deepEqual(input.exact_state,state);assert.equal(input.fresh,false);return{hits:[{doc_id:'a'}],complete:true,exact_state:null,step_records:1};},{now:()=>clock});
 assert.equal(final.complete,true);assert.equal(final.cursor,null);assert.deepEqual(final.hits,[{doc_id:'a'}]);
 // Lost HTTP acknowledgements can replay the same immutable step after eviction.
 await exactSearch({...env},{...query,cursor:first.cursor},async input=>{assert.deepEqual(input.exact_state,state);return{hits:[],complete:true};},{now:()=>clock});
});
test('exact cursor rejects tampering, another tenant/query, expiry and key rotation before I/O',async()=>{
 const first=await exactSearch(env,query,async()=>structuredClone(partial),{now:()=>1000});let calls=0;
 const execute=async()=>{calls++;throw Error('unverified state reached native reader');};
 for(const[e,q,time]of[[env,{...query,cursor:first.cursor.slice(0,-4)+'AAAA'},1001],[{...env,ANTFLY_INCARNATION:'tenant-b'},{...query,cursor:first.cursor},1001],[env,{...query,vector:[0,1],cursor:first.cursor},1001],[env,{...query,cursor:first.cursor},301000],[{...env,API_TOKEN:'rotated'},{...query,cursor:first.cursor},1001]])await assert.rejects(exactSearch(e,q,execute,{now:()=>time}),/ExactScan/);
 assert.equal(calls,0);
});
test('exact steps release their execution call, preserve freshness at capture and honor cancellation',async()=>{
 let calls=0;const controller=new AbortController();
 await assert.rejects(exactSearch({...env,EXACT_SCAN_INLINE_STEPS:'4'},{...query,fresh:true},async input=>{calls++;assert.equal(input.fresh,true);controller.abort();return structuredClone(partial);},{signal:controller.signal}),/Canceled/);assert.equal(calls,1);
 for(const key of ['exact_scan','exact_state','exact_records'])assert.throws(()=>rejectPrivateScanInput({...query,[key]:true}),/PrivateQueryStateUnsupported/);
});
test('exact orchestration bounds inline steps and rejects a stalled native continuation',async()=>{
 let calls=0;
 const result=await exactSearch({...env,EXACT_SCAN_INLINE_STEPS:'2'},query,async input=>{calls++;return{...structuredClone(partial),exact_state:{...state,scanned:String((input.exact_state?Number(input.exact_state.scanned):0)+512)}};});
 assert.equal(calls,2);assert.equal(result.complete,false);assert.equal(result.step_records,1024);
 await assert.rejects(exactSearch(env,{...query,cursor:result.cursor},async input=>({...structuredClone(partial),exact_state:input.exact_state})),/InvalidExactScanProgress/);
});


test('progress renews the idle window without extending the absolute lifetime',async()=>{
 let clock=1000;
 const execute=async input=>({...structuredClone(partial),exact_state:{...state,scanned:String((input.exact_state?Number(input.exact_state.scanned):0)+512)}});
 let result=await exactSearch(env,query,execute,{now:()=>clock});
 for(const next of[200000,400000]){clock=next;result=await exactSearch(env,{...query,cursor:result.cursor},execute,{now:()=>clock});assert.equal(result.complete,false);}
 clock=3601000;await assert.rejects(exactSearch(env,{...query,cursor:result.cursor},execute,{now:()=>clock}),/ExactScanExpired/);
});


test('a foreground request acquires the shared reader between exact scan steps',async()=>{
 const{RequestPool}=await import('../request-pool.mjs');
 const pool=new RequestPool(async()=>({usable:()=>true,memoryBytes:()=>0}),1),events=[];let unblock,entered;
 const first=new Promise(resolve=>entered=resolve),held=new Promise(resolve=>unblock=resolve);let n=0;
 const scan=exactSearch({...env,EXACT_SCAN_INLINE_STEPS:'4'},query,()=>pool.withHost(async()=>{
  n++;events.push('scan-'+n);if(n===1){entered();await held;}
  return n===3?{hits:[{doc_id:'winner'}],complete:true}: {...structuredClone(partial),exact_state:{...state,scanned:String(n*512)}};
 }));
 await first;
 const foreground=pool.withHost(async()=>{events.push('lookup');return'foreground';});unblock();
 const result=await scan;assert.equal(await foreground,'foreground');assert.equal(result.complete,true);assert.deepEqual(events,['scan-1','lookup','scan-2','scan-3']);
});

test('exact cursor authenticates deferred format-2 controls without exposing partial candidates',async()=>{
 const deferred={...state,format:2,pending:['YXV0aGVudGljYXRlZC1jb250cm9s'],refined_records:'1',pruned_records:'510'};
 const first=await exactSearch(env,query,async()=>({...partial,exact_state:deferred}),{now:()=>1000});
 assert.deepEqual(first.hits,[]);assert.equal(first.pending,undefined);assert.equal(first.exact_state,undefined);
 const result=await exactSearch(env,{...query,cursor:first.cursor},async input=>{assert.deepEqual(input.exact_state,deferred);return{hits:[{doc_id:'verified'}],complete:true};},{now:()=>1001});
 assert.deepEqual(result.hits,[{doc_id:'verified'}]);assert.equal(result.cursor,null);
});
