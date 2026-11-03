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

import{test}from'node:test';import assert from'node:assert/strict';import{withReadView}from'../read-view.mjs';import{withNativeReadPlan}from'../native-read-plan.mjs';import{createRoleCache}from'../role-cache.mjs';import{createImmutableReadCache}from'../../../zig/lib/objectstore/wasm/cache.mjs';
const key='antfly/artifacts/sha256/aa/'+'a'.repeat(62),request={op:'get',bucket:'database',key},bytes=Uint8Array.of(1,2,3),record={etag:'immutable',contentType:'application/octet-stream',body:bytes},view=()=>({format:1,version:'1',entries:[['antfly/progress/ns/HEAD',{etag:'head',body:new TextEncoder().encode('1')}]],immutable:[[key,{...record,body:bytes.slice()}]]});
test('admitted immutable headers populate the bounded binding-scoped cache for later preparation',async()=>{
 const cache=createRoleCache({maxBytes:8192,maxEntries:8}),binding={},scope=cache.forBucket(binding),initial=view();let raw=0,offers=0;
 const retainImmutable=(req,result)=>{offers++;return scope.admit('ns',req,result);},read=withReadView(async()=>{throw Error('unexpected storage read');},{},'ns',{initial,retainImmutable});
 assert.deepEqual([...(await read(request)).body],[1,2,3]);assert.equal(offers,1);assert.ok(cache.retainedBytes()>0&&cache.retainedBytes()<=8192);initial.immutable[0][1].body.fill(0);
 const prepare=scope.wrap(async()=>{raw++;return{json:{},body:Uint8Array.of(9)};},'ns');assert.deepEqual([...(await prepare(request)).body],[1,2,3]);assert.equal(raw,0);
 const other=cache.forBucket({}).wrap(async()=>{raw++;return{json:{},body:Uint8Array.of(8)};},'ns');assert.deepEqual([...(await other(request)).body],[8]);assert.equal(raw,1);
 await read({op:'get',bucket:'database',key:'antfly/progress/ns/HEAD'});assert.equal(offers,1,'mutable metadata must not enter the immutable cache');
});
test('cache retention cannot bypass view expiry, response conditions or existing admission limits',async()=>{
 const cache=createImmutableReadCache({maxBytes:8192,maxEntries:8});let offers=0;const retainImmutable=(req,result)=>{offers++;return cache.admit('ns',req,result);},initial=view(),read=withReadView(async()=>{throw Error('unexpected fallback');},{},'ns',{initial,retainImmutable});await read(request);
 const expired=withReadView(cache.wrap(async()=>{throw Error('unexpected I/O');},'ns'),{},'ns',{initial:view(),retainImmutable,admittedAt:performance.now()-30001});await assert.rejects(expired(request),error=>error.status===8);await assert.rejects(read({...request,if_match_etag:'wrong'}),error=>error.status===2);await assert.rejects(read({...request,max_response_bytes:1}),error=>error.status===4);assert.equal(offers,1);
 const disabled=createImmutableReadCache({maxBytes:0,maxEntries:0});assert.equal(disabled.admit('ns',request,{json:{},body:bytes}),false);assert.equal(disabled.retainedBytes(),0);const header={content_length:3,revision:{kind:'etag',value:'original'},callback(){}};assert.equal(cache.admit('metadata',request,{json:header,body:bytes}),true);header.revision.value='changed';const copied=await cache.wrap(async()=>{throw Error('metadata copy missing');},'metadata')(request);assert.equal(copied.json.revision.value,'original');assert.equal(copied.json.callback,undefined);assert.equal(cache.admit('ns',{...request,key:'antfly/progress/ns/HEAD'},{json:{},body:bytes}),false);assert.equal(cache.admit('ns',{...request,range:{offset:0,length:1}},{json:{},body:bytes}),false);assert.equal(cache.admit('ns',request,{json:{content_length:4},body:bytes}),false);
});
test('native immutable preparation sees no admitted hint until its fresh read barrier',async()=>{
 const cache=createImmutableReadCache({maxBytes:8192,maxEntries:8}),initial=view();let offers=0,raw=0;const transport=withNativeReadPlan(async()=>{raw++;return{json:{},body:bytes.slice()};},{},'ns',{fresh:Promise.resolve({view:initial,started:performance.now()}),retainImmutable:(req,result)=>{offers++;return cache.admit('ns',req,result);}});
 await transport(request);assert.equal(raw,1);assert.equal(offers,0);await transport({op:'read_view_barrier'});await transport(request);assert.equal(raw,1);assert.equal(offers,1);assert.deepEqual([...(await cache.wrap(async()=>{throw Error('header was not retained');},'ns')(request)).body],[1,2,3]);
});
