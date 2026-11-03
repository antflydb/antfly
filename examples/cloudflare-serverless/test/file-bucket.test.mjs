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
import{stat}from'node:fs/promises';
import{FileBucket}from'./file-bucket.mjs';import{Bucket}from'./object-fixture.mjs';
import{createR2Transport}from'../../../zig/lib/objectstore/wasm/r2.mjs';

test('file-backed R2 fixture preserves conditional writes, ranges, pagination and exact traffic counters',async()=>{
 const disk=await FileBucket.create(),memory=new Bucket();
 try{
  async function exercise(bucket){
   const transport=createR2Transport({database:bucket}),request=(op,key,fields={})=>({op,bucket:'database',key,...fields}),out=[];
   out.push(await transport(request('put','a',{if_none_match:true,content_type:'application/octet-stream'}),Uint8Array.of(1,2,3,4)));
   await assert.rejects(transport(request('put','a',{if_none_match:true}),Uint8Array.of(9)),/conflict/);
   const results=await Promise.allSettled([5,6].map(value=>transport(request('put','a',{if_match_etag:out[0].json.etag}),Uint8Array.of(value,7,8))));assert.equal(results.filter(r=>r.status==='fulfilled').length,1);
   out.push(await transport(request('get','a',{range:{offset:1,length:2}})));out.push(await transport(request('head','a')));
   await assert.rejects(transport(request('get','a',{if_match_etag:'obsolete'})),/conflict/);
   for(const key of['row/3','row/1','row/2'])await transport(request('put',key),Uint8Array.of(42));
   out.push(await transport(request('list',undefined,{prefix:'row/',recursive:true,max_keys:2})));
   out.push(await transport(request('list',undefined,{prefix:'row/',recursive:true,max_keys:2,continuation_token:out.at(-1).json.next_continuation_token})));
   await transport(request('delete','a'));assert.equal(await bucket.head('a'),null);
   return{out,reads:bucket.reads,readBytes:bucket.readBytes,writes:bucket.writes};
  }
  assert.deepEqual(await exercise(disk),await exercise(memory));
  assert.ok([...disk.objects.values()].every(value=>!('body'in value)),'the index must retain no payload buffers');
 }finally{const directory=disk.directory;await disk.close();await assert.rejects(stat(directory),{code:'ENOENT'});}
});
test('file-backed fixture owns queued bytes and leaves in-flight readers on immutable prior extents',async()=>{
 const bucket=await FileBucket.create();
 try{
  const input=Buffer.from([1,2,3]),pending=bucket.put('key',input);input.fill(9);const first=await pending;
  const prior=await bucket.get('key');await bucket.put('key',Uint8Array.of(4,5),{onlyIf:{etagMatches:first.etag}});
  assert.deepEqual([...new Uint8Array(await new Response(prior.body).arrayBuffer())],[1,2,3]);
  assert.deepEqual([...new Uint8Array(await new Response((await bucket.get('key')).body).arrayBuffer())],[4,5]);
 }finally{await bucket.close();}
});
