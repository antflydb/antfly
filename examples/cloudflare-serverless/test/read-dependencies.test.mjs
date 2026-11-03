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

import {test} from 'node:test';
import assert from 'node:assert/strict';
import {ReadDependencies} from '../../../zig/lib/objectstore/wasm/read-dependencies.mjs';
import {ObjectStoreError} from '../../../zig/lib/objectstore/wasm/r2.mjs';
const request={op:'get',bucket:'database',key:'antfly/artifacts/sha256/aa/'+ 'b'.repeat(62),max_response_bytes:64};
const fixture=options=>new ReadDependencies({deadlineMs:Date.now()+10000,...options});
test('immutable dependencies unwind before I/O and own replay bytes without renewing authority',async()=>{
 let calls=0;const body=new Uint8Array([1,2]),d=fixture(),transport=d.wrap(async()=>{calls++;return{json:{content_length:2},body};});
 await assert.rejects(transport(request),e=>e.status===10);assert.equal(calls,0);await d.load();assert.equal(calls,1);body[0]=7;const a=await transport(request);assert.equal(a.body[0],1);a.body[0]=8;assert.equal((await transport(request)).body[0],1);d.clear();assert.equal(d.bytes,0);
});
test('only typed absence is replayed; resource, integrity and oversize responses fail',async()=>{
 for(const error of[new ObjectStoreError(1,'missing'),Error('SQLITE_NOMEM'),Error('ArtifactIntegrityMismatch')]){
 const d=fixture(),t=d.wrap(async()=>{throw error;});await assert.rejects(t(request),e=>e.status===10);
 if(error.status===1){await d.load();await assert.rejects(t(request),e=>e===error);}else await assert.rejects(d.load(),e=>e===error);
 }
 const d=fixture(),t=d.wrap(async()=>({body:new Uint8Array(65)}));await assert.rejects(t(request),e=>e.status===10);await assert.rejects(d.load(),/ObjectIoBudgetExceeded/);
});
test('dependency replay has fixed bounds, mutation guards, legacy fallback and exact request identities',async()=>{
 let calls=0;const d=fixture({maxBytes:4096}),t=d.wrap(async()=>{calls++;return{body:new Uint8Array()};});await t(request);assert.equal(calls,1);await assert.rejects(t({...request,op:'put'}),/PointReadWriteForbidden/);
 const old=fixture().wrap(async()=>({body:new Uint8Array()}),{enabled:false});await old(request);
 const x=fixture(),read=x.wrap(async()=>({body:new Uint8Array()}));await assert.rejects(read(request),e=>e.status===10);await x.load();await assert.rejects(read({...request,range:{offset:0,length:1}}),e=>e.status===10);
});
test('logical deadline retains physical settlement and late failure identity',async()=>{
 let reject;const d=fixture({deadlineMs:Date.now()+30}),t=d.wrap(()=>new Promise((_,no)=>reject=no));await assert.rejects(t(request),e=>e.status===10);
 let failure;try{await d.load();}catch(e){failure=e;}assert.equal(failure.message,'DeadlineExceeded');assert.ok(failure.ioSettled);const late=Error('SQLITE_NOMEM');reject(late);await assert.rejects(failure.ioSettled,e=>e===late);
});
test('point dependency admission preserves independent metadata proof capacity',async()=>{
 const{pointDependencyBudgetFor,pointDependencyMetrics}=await import('../../../zig/lib/objectstore/wasm/read-dependencies.mjs');
 const{readAheadBudgetFor}=await import('../../../zig/lib/objectstore/wasm/read-ahead.mjs');
 const kernel={slots:Array.from({length:4},()=>({}))},point=pointDependencyBudgetFor(kernel),proof=readAheadBudgetFor(kernel,4),held=[];
 for(let i=0;i<4;i++)held.push(point.tryAcquire('point/'+i));assert.equal(point.tryAcquire('point/overflow'),null);
 for(let i=0;i<4;i++)held.push(proof.tryAcquire('proof/'+i));assert.ok(held.every(Boolean));assert.equal(pointDependencyMetrics(kernel).point_dependency_bytes,4*102400);for(const release of held)release();assert.equal(pointDependencyMetrics(kernel).point_dependency_active,0);
});
test('capacity fallback drops transcripts before using the ordinary native response envelope',async()=>{
 const d=fixture(),t=d.wrap(async()=>({body:new Uint8Array(1)}));await assert.rejects(t(request),e=>e.status===10);await d.load();assert.ok(d.bytes>0);
 await t({...request,key:request.key.replace('/aa/','/bb/'),max_response_bytes:128*1024});assert.equal(d.disabled,true);assert.equal(d.bytes,0);assert.equal(d.values.size,0);await t(request);
});

test('future provider conditions pass through and malformed response buffers fail closed',async()=>{
 let calls=0;const d=fixture(),t=d.wrap(async()=>{calls++;return{body:new Uint8Array()};});await t({...request,if_match_revision:{kind:'gcs-generation',value:'12'}});await t({...request,if_generation_match:'12'});assert.equal(calls,2);assert.equal(d.pending,undefined);
 const invalid=fixture(),bad=invalid.wrap(async()=>({body:'not bytes'}));await assert.rejects(bad(request),e=>e.status===10);await assert.rejects(invalid.load(),/InvalidReadDependency/);
});
