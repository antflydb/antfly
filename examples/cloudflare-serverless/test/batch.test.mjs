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

import {test} from "node:test";
import assert from "node:assert/strict";
import {ObjectStoreError} from "../../../zig/lib/objectstore/wasm/r2.mjs";
import {executePutBatch, validatePutBatch} from "../../../zig/lib/objectstore/wasm/batch.mjs";
const request = {op: "put_many", bucket: "database", items: Array.from({length: 8}, (_, i) => ({key: String(i), offset: i, length: 1}))};
test("upload batches bound concurrency and preserve result ordering", async () => {
  let active = 0, peak = 0;
  const result = await executePutBatch(async (item, body) => {
    peak = Math.max(peak, ++active); assert.equal(body[0], Number(item.key));
    await new Promise(resolve => setTimeout(resolve, 2)); active--;
    return {json: {etag: item.key}};
  }, request, Uint8Array.from({length: 8}, (_, i) => i));
  assert.equal(peak, 4); assert.equal(active, 0);
  assert.deepEqual(result.json.results.map(item => item.etag), request.items.map(item => item.key));
});
test("failed upload batches join in-flight writes and stop scheduling new work", async () => {
  const started = [], finished = [];
  await assert.rejects(executePutBatch(async item => {
    started.push(item.key);
    if (item.key === "0") throw new Error("injected upload loss");
    await new Promise(resolve => setTimeout(resolve, 5)); finished.push(item.key);
    return {json: {}};
  }, request, new Uint8Array(8)), /injected upload loss/);
  assert.deepEqual(started, ["0", "1", "2", "3"]);
  assert.deepEqual(finished, ["1", "2", "3"]);
});
test("upload batch extents cannot alias bytes or amplify an admitted request", () => {
  assert.throws(() => validatePutBatch({...request, items: request.items.map(item => ({...item, offset: 0}))}, 8), /extent/);
  assert.throws(() => validatePutBatch(request, 9), /unclaimed/);
  assert.throws(() => validatePutBatch({...request, items: [...request.items, request.items[0]]}, 8), /invalid upload batch/);
});

test('read batches overlap bounded requests, preserve misses and opaque revisions, and join failures',async()=>{
 const {executeGetBatch,validateGetBatch}=await import('../../../zig/lib/objectstore/wasm/batch.mjs');
 const request={op:'get_many',bucket:'database',items:Array.from({length:5},(_,i)=>({key:String(i),max_response_bytes:16}))};
 let active=0,peak=0,completed=0;
 const result=await executeGetBatch(async item=>{peak=Math.max(peak,++active);await new Promise(r=>setTimeout(r,3));active--;completed++;if(item.key==='2')throw new ObjectStoreError(1,'missing');return{json:{bucket:'database',key:item.key,version_id:'9007199254740994123',revision_kind:'generation'},body:Uint8Array.of(Number(item.key))};},request);
 assert.equal(peak,4);assert.equal(completed,5);assert.equal(result.json.results[2].missing,true);assert.deepEqual([...result.body],[0,1,3,4]);assert.equal(result.json.results[4].metadata.version_id,'9007199254740994123');
 completed=0;const failure=new ObjectStoreError(7,'transport');await assert.rejects(executeGetBatch(async item=>{await new Promise(r=>setTimeout(r,2));completed++;if(item.key==='0')throw failure;return{json:{},body:Uint8Array.of(1)};},request),e=>e===failure);assert.equal(completed,4);
 for(const invalid of[{...request,items:[]},{...request,items:Array(9).fill(request.items[0])},{...request,items:[{key:'x',max_response_bytes:4*1024*1024},{key:'y',max_response_bytes:1}]}])assert.throws(()=>validateGetBatch(invalid));
});
test('read batch response budgets and cancellation fail without dropping started I/O',async()=>{
 const {executeGetBatch}=await import('../../../zig/lib/objectstore/wasm/batch.mjs');const request={op:'get_many',bucket:'database',items:[{key:'one',max_response_bytes:4},{key:'two',max_response_bytes:4}]};let completed=0;
 await assert.rejects(executeGetBatch(async()=>{completed++;return{json:{},body:new Uint8Array(5)};},request),e=>e.status===4);assert.equal(completed,2);
 completed=0;await assert.rejects(executeGetBatch(async()=>{completed++;return{json:{},body:new Uint8Array(1)};},request,()=>{throw new ObjectStoreError(5,'canceled');}),e=>e.status===5);assert.equal(completed,0);
});

test('read batch diagnostics preserve a later resource failure after another child fails first',async()=>{
 const {executeGetBatch}=await import('../../../zig/lib/objectstore/wasm/batch.mjs'),request={op:'get_many',bucket:'database',items:[{key:'first',max_response_bytes:4},{key:'later',max_response_bytes:4}]},primary=new ObjectStoreError(7,'transport failed'),seen=[];
 await assert.rejects(executeGetBatch(async item=>{await new Promise(r=>setTimeout(r,item.key==='first'?1:5));throw item.key==='first'?primary:new Error('SQLITE_NOMEM');},request,()=>{},4,error=>seen.push(error.message)),e=>e===primary);
 assert.deepEqual(seen,['transport failed','SQLITE_NOMEM']);
});
