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
import{ResidentTail}from'../resident-tail.mjs';
import{CommittedHeaders}from'../../../zig/lib/objectstore/wasm/committed-headers.mjs';
import{ImmutableFlights}from'../../../zig/lib/objectstore/wasm/immutable-flights.mjs';
import{ReadCohorts}from'../read-cohorts.mjs';
const tail=(start,end)=>({scope:Array(32).fill(1),start_lsn:String(start),end_lsn:String(end),mutations:Array.from({length:end-start+1},(_,i)=>({kind:'upsert',doc_id:'id/'+(i+start),body:'{}'}))});
test('resident tail installs contiguous normalized commits and treats gaps owner terms and eviction as misses',()=>{
 let bytes=0;const cache=new ResidentTail({maxRecords:3,onCharge:x=>bytes=x});cache.install(tail(1,2),[1]);cache.install(tail(3,3),[1]);assert.equal(cache.snapshot([1]).mutations.length,3);cache.install(tail(4,4),[1]);assert.equal(cache.snapshot([1]).start_lsn,'2');cache.install(tail(1,1),[1]);assert.equal(cache.snapshot([1]).end_lsn,'4');cache.install(tail(8,8),[1]);assert.equal(cache.snapshot([1]).start_lsn,'8');assert.equal(cache.snapshot([2]),null);cache.install(tail(9,9),[2]);assert.equal(cache.snapshot([2]).start_lsn,'9');assert.ok(bytes>0);cache.clear();assert.equal(bytes,0);assert.equal(cache.snapshot([2]),null);
});
test('bootstrap headers are immutable generation-bound copies and preserve conditions and limits',async()=>{
 const pack=new CommittedHeaders('ns','2'),key='antfly/manifests/ns/manifests/2.bin',body=Uint8Array.of(1,2,3);pack.offer({op:'get',bucket:'database',key},{body,json:{etag:'one'}});body[0]=9;let calls=0;
 const raw=async()=>{calls++;return{};},read=CommittedHeaders.wrap(raw,pack.encode(),'ns','2');assert.deepEqual((await read({op:'get',bucket:'database',key})).body,Uint8Array.of(1,2,3));assert.equal((await read({op:'head',bucket:'database',key})).json.content_length,3);await assert.rejects(read({op:'get',bucket:'database',key,if_match_etag:'two'}),/condition/);await assert.rejects(read({op:'get',bucket:'database',key,max_response_bytes:2}),/budget/);await CommittedHeaders.wrap(raw,pack.encode(),'ns','3')({op:'get',bucket:'database',key});assert.equal(calls,1);await read({op:'put',bucket:'database',key});assert.equal(calls,2);
});
test('immutable misses coalesce through CPU wake tickets and followers read only completed owned bytes',async()=>{
 const flights=new ImmutableFlights(),cache=new Map();let physical=0,release,started;const ready=new Promise(r=>started=r),gate=new Promise(r=>release=r),request={op:'get',bucket:'database',key:'antfly/artifacts/sha256/aa/'+'b'.repeat(62),max_response_bytes:100};
 const read=flights.wrap(async req=>{if(cache.has(req.key))return{body:cache.get(req.key).slice()};physical++;started();await gate;cache.set(req.key,Uint8Array.of(7));return{body:cache.get(req.key).slice()};},'ns');
 const owner=read(request);await ready;const follower=read(request);await new Promise(r=>setTimeout(r,5));assert.equal(physical,1);release();const[a,b]=await Promise.all([owner,follower]);a.body[0]=1;assert.equal(b.body[0],7);assert.equal(physical,1);assert.equal(flights.entries.size,0);
});
test('read cohorts isolate member errors and hand off later work to its own submitter',async()=>{
 let batches=0;const cohorts=new ReadCohorts(async members=>{batches++;return members.map(m=>m.input.doc_id==='bad'?{error:'WalVisibilityUnavailable'}:{body:m.input.doc_id});});
 const results=await Promise.allSettled(['a','bad','c'].map(doc_id=>cohorts.submit({op:'lookup',doc_id})));assert.equal(batches,1);assert.equal(results[0].value.body,'a');assert.match(results[1].reason.message,/WalVisibility/);assert.equal(results[2].value.body,'c');assert.equal(cohorts.pending,0);assert.equal(cohorts.bytes,0);assert.equal((await cohorts.submit({op:'lookup',doc_id:'d'})).body,'d');assert.equal(batches,2);
});
test('coalesced failure settles every follower and releases its existing memory reservations',async()=>{
 let retained=0,release,started;const ready=new Promise(r=>started=r),gate=new Promise(r=>release=r);
 const flights=new ImmutableFlights({reserve:bytes=>{retained+=bytes;return()=>retained-=bytes;}});
 const read=flights.wrap(async()=>{started();await gate;throw Error('ObjectIoBudgetExceeded');},'ns'),request={op:'get',bucket:'database',key:'antfly/artifacts/sha256/aa/'+'b'.repeat(62)};
 const first=read(request);await ready;const follower=read(request);await new Promise(r=>setTimeout(r,5));assert.equal(retained,640);
 const results=Promise.allSettled([first,follower]);release();assert.ok((await results).every(x=>x.status==='rejected'&&x.reason.message==='ObjectIoBudgetExceeded'));assert.equal(retained,0);assert.equal(flights.entries.size,0);
 assert.throws(()=>new ImmutableFlights({maxFlights:5}),/InvalidImmutableFlightLimits/);
});
test('a follower deadline leaves the physical owner charged until storage settles',async()=>{
 let retained=0,release,started;const ready=new Promise(r=>started=r),gate=new Promise(r=>release=r),flights=new ImmutableFlights({reserve:n=>{retained+=n;return()=>retained-=n;}}),request={op:'get',bucket:'database',key:'antfly/artifacts/sha256/aa/'+'d'.repeat(62)};
 const raw=async()=>{started();await gate;return{body:Uint8Array.of(1)};};
 const owner=flights.wrap(raw,'ns')(request);await ready;
 await assert.rejects(flights.wrap(raw,'ns',{deadlineMs:Date.now()+10})(request),/DeadlineExceeded/);
 assert.equal(retained,512);assert.equal(flights.entries.size,1);release();await owner;assert.equal(retained,0);
});
