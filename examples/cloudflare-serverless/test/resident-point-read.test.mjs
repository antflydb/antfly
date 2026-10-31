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
import{withPointReadDependencies}from'../../../zig/lib/objectstore/wasm/point-read.mjs';import{pointDependencyMetrics}from'../../../zig/lib/objectstore/wasm/read-dependencies.mjs';import{RequestPool}from'../request-pool.mjs';
const key=n=>'antfly/artifacts/sha256/aa/'+n.repeat(62);
const pool=()=>new RequestPool(async()=>({readDependencyABI:1,usable:()=>true,memoryBytes:()=>1}),2,{reservationBytes:1,maxReservedBytes:2});
function lookup(kernel,id,{raw,cache=new Map(),deadlineMs=Date.now()+5000,...options}){
 return withPointReadDependencies({kernel,fairKey:'test',deadlineMs,background:()=>{},transport:raw,acquire:fn=>kernel.withHost(fn),wrapTransport:up=>async request=>{if(cache.has(request.key))return cache.get(request.key);const value=await up(request);cache.set(request.key,value);return value;},execute:async(host,o)=>{try{const r=await o.transport({op:'get',bucket:'database',key:id,max_response_bytes:16});return r.body;}catch(error){if(o.nativeReadYield&&error.status===10)return{__read_dependency:true};throw error;}finally{o.onMetrics({operations:1,transferred_bytes:0});}},...options});
}
test('cold immutable I/O releases native lanes and warm reads bypass exhausted transcript permits',async()=>{
 const kernel=pool(),cache=new Map([[key('a'),{body:Uint8Array.of(1)}]]),waiters=[];let started;const ready=new Promise(r=>started=r);
 const raw=()=>new Promise(resolve=>{waiters.push(resolve);if(waiters.length===2)started();}),usage=[];
 const cold=[lookup(kernel,key('b'),{raw,cache,onUsage:x=>usage.push(x)}),lookup(kernel,key('c'),{raw,cache,onUsage:x=>usage.push(x)})];
 await ready;assert.equal(pointDependencyMetrics(kernel).point_dependency_active,2);assert.equal(kernel.reservedBytes,0);
 assert.deepEqual(await lookup(kernel,key('a'),{raw,cache,deadlineMs:Date.now()+1000}),Uint8Array.of(1));
 waiters.forEach(resolve=>resolve({body:Uint8Array.of(2)}));for(const result of await Promise.all(cold))assert.deepEqual(result,Uint8Array.of(2));
 assert.equal(pointDependencyMetrics(kernel).point_dependency_active,0);assert.equal(pointDependencyMetrics(kernel).point_dependency_bytes,0);assert.ok(usage.every(x=>x.operations===3&&x.transferred_bytes>0));
});
test('detached timeout retains charged bytes until late resource failure settles without blocking warm reads',async()=>{
 const kernel=pool(),tasks=[],late=[];let rejectIo,started;const ready=new Promise(r=>started=r),raw=()=>new Promise((_,reject)=>{rejectIo=reject;started();});
 const pending=lookup(kernel,key('b'),{raw,deadlineMs:Date.now()+100,background:p=>tasks.push(p),onLateError:e=>late.push(e)});await ready;
 await assert.rejects(pending,e=>e.message==='DeadlineExceeded'&&!!e.ioSettled);assert.equal(tasks.length,1);assert.equal(kernel.reservedBytes,0);assert.equal(pointDependencyMetrics(kernel).point_dependency_active,1);assert.ok(pointDependencyMetrics(kernel).point_dependency_bytes>0);
 assert.deepEqual(await lookup(kernel,key('a'),{raw,cache:new Map([[key('a'),{body:Uint8Array.of(1)}]])}),Uint8Array.of(1));
 const failure=Error('SQLITE_NOMEM');rejectIo(failure);await tasks[0];assert.deepEqual(late,[failure]);assert.equal(pointDependencyMetrics(kernel).point_dependency_active,0);assert.equal(pointDependencyMetrics(kernel).point_dependency_bytes,0);
});
test('resource errors propagate once and older modules retain the ordinary transport',async()=>{
 const kernel=pool(),failure=Error('SQLITE_NOMEM');let calls=0;
 await assert.rejects(lookup(kernel,key('b'),{raw:async()=>{calls++;throw failure;}}),e=>e===failure);assert.equal(calls,1);assert.equal(pointDependencyMetrics(kernel).point_dependency_active,0);
 const old=new RequestPool(async()=>({usable:()=>true,memoryBytes:()=>1}),1);assert.deepEqual(await lookup(old,key('b'),{raw:async()=>({body:Uint8Array.of(3)})}),Uint8Array.of(3));
});
test('a queued cold miss expires without starting I/O or leaking admission behind active transcripts',async()=>{
 const kernel=pool(),waiters=[];let started;const ready=new Promise(r=>started=r),raw=()=>new Promise(resolve=>{waiters.push(resolve);if(waiters.length===2)started();});
 const cold=[lookup(kernel,key('b'),{raw}),lookup(kernel,key('c'),{raw})];await ready;
 await assert.rejects(lookup(kernel,key('d'),{raw,deadlineMs:Date.now()+50}),/DeadlineExceeded/);assert.equal(waiters.length,2);assert.equal(pointDependencyMetrics(kernel).point_dependency_queued,0);assert.equal(kernel.externalPending,0);
 waiters.forEach(resolve=>resolve({body:Uint8Array.of(1)}));await Promise.all(cold);assert.equal(pointDependencyMetrics(kernel).point_dependency_active,0);
});
test('an oversized immutable response fails the original request without retry or retained buffers',async()=>{
 const kernel=pool();let calls=0;
 await assert.rejects(lookup(kernel,key('b'),{raw:async()=>{calls++;return{body:new Uint8Array(17)};}}),/ObjectIoBudgetExceeded/);assert.equal(calls,1);assert.equal(pointDependencyMetrics(kernel).point_dependency_active,0);assert.equal(pointDependencyMetrics(kernel).point_dependency_bytes,0);
});
