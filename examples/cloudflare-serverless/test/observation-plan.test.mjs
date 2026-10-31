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

import{test}from'node:test';import assert from'node:assert/strict';import{readObservationResponse}from'../../../zig/lib/objectstore/wasm/observation-plan.mjs';import{ObjectStoreError}from'../../../zig/lib/objectstore/wasm/r2.mjs';
const key='scope/epochs/'+'a'.repeat(32)+'/'+'0'.repeat(20)+'.json',options={base:'scope',reserve(){},wait:p=>p};
test('detached observation reads exactly one bounded immutable object and charges before I/O',async()=>{let charged=false,calls=0;const value=await readObservationResponse(async request=>{calls++;assert.equal(charged,true);assert.equal(request.op,'get');assert.equal(request.max_response_bytes,16384);return{json:{etag:'one'},body:new TextEncoder().encode('{"frame":"accepted"}')};},key,{...options,reserve(ops,bytes){assert.equal(ops,1);assert.ok(bytes>16384+4096);charged=true;}});assert.equal(calls,1);assert.deepEqual(value,{key,body:'{"frame":"accepted"}'});});
test('only a typed missing-object error becomes an end probe; resource failures remain failures',async()=>{assert.deepEqual(await readObservationResponse(async()=>{throw new ObjectStoreError(1,'missing');},key,options),{key,body:null});await assert.rejects(readObservationResponse(async()=>{throw Error('SQLITE_NOMEM');},key,options),/SQLITE_NOMEM/);await assert.rejects(readObservationResponse(async()=>{throw new ObjectStoreError(7,'transport');},key,options),/transport/);});
test('foreign keys, cancellation and exhausted budgets cannot issue storage I/O',async()=>{let calls=0;const transport=async()=>{calls++;return{};};await assert.rejects(readObservationResponse(transport,key.replace('scope','foreign'),options),/InvalidImmutableLogObservationResponse/);await assert.rejects(readObservationResponse(transport,key,{...options,check(){throw Error('Canceled');}}),/Canceled/);await assert.rejects(readObservationResponse(transport,key,{...options,reserve(){throw Error('ObjectIoBudgetExceeded');}}),/ObjectIoBudgetExceeded/);assert.equal(calls,0);});
test('oversized bodies/headers and invalid UTF-8 cannot become trusted native input',async()=>{for(const result of[{body:new Uint8Array(16385)},{body:new Uint8Array(),json:{oversized:'x'.repeat(4096)}}])await assert.rejects(readObservationResponse(async()=>result,key,options),/ObjectIoBudgetExceeded/);await assert.rejects(readObservationResponse(async()=>({body:new Uint8Array([255])}),key,options),/encoded data|encoding|UTF-8/i);});
test('logical timeout retains the exact physical lifetime until a late resource error settles',async()=>{let rejectPhysical,draining,settled=false;const physical=new Promise((_,reject)=>rejectPhysical=reject);let error;try{await readObservationResponse(()=>physical,key,{...options,wait:()=>Promise.reject(Error('DeadlineExceeded')),onDraining:promise=>{draining=promise;promise.then(()=>settled=true);}});}catch(value){error=value;}assert.match(error.message,/DeadlineExceeded/);assert.equal(error.ioSettled,draining);assert.equal(settled,false);rejectPhysical(Error('SQLITE_NOMEM'));await error.ioSettled;assert.equal(settled,true);});

test('continuation permits allow same-namespace overlap within the existing byte limit and admit queued peers fairly',async()=>{
 const{ReadAheadBudget,proofPrefetchBytes}=await import('../../../zig/lib/objectstore/wasm/read-ahead.mjs');const budget=new ReadAheadBudget(2),one=await budget.acquire('hot',{concurrent:true}).ready,two=await budget.acquire('hot',{concurrent:true}).ready;
 assert.equal(budget.bytes,2*proofPrefetchBytes);assert.equal(budget.tryAcquire('hot'),null);const third=budget.acquire('hot',{concurrent:true}),peer=budget.acquire('peer',{concurrent:true});let hotAdmitted=false;third.ready.then(()=>hotAdmitted=true);one();const quiet=await peer.ready;assert.equal(hotAdmitted,false);assert.equal(budget.active,2);quiet();const hot=await third.ready;assert.equal(budget.active,2);assert.equal(budget.tryAcquire('hot'),null);two();assert.equal(budget.tryAcquire('hot'),null);hot();assert.equal(budget.active,0);assert.equal(budget.bytes,0);
 const sharedOne=await budget.acquire('hot',{concurrent:true}).ready,sharedTwo=await budget.acquire('hot',{concurrent:true}).ready,waitingExclusive=budget.acquire('hot'),laterShared=budget.acquire('hot',{concurrent:true});let exclusiveAdmitted=false;waitingExclusive.ready.then(()=>exclusiveAdmitted=true);sharedOne();assert.equal(budget.active,1);assert.equal(exclusiveAdmitted,false);sharedTwo();const turn=await waitingExclusive.ready;assert.equal(budget.active,1);turn();const after=await laterShared.ready;after();assert.equal(budget.active,0);
 const exclusive=await budget.acquire('hot').ready,blocked=budget.acquire('hot',{concurrent:true});assert.equal(budget.active,1);exclusive();const resumed=await blocked.ready;resumed();assert.equal(budget.active,0);
});
