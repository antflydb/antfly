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
import {immutableReadWave} from '../../../zig/lib/objectstore/wasm/immutable-read-wave.mjs';
const key=n=>'antfly/wal-v1/'+ 'a'.repeat(64)+'/pages/'+ 'b'.repeat(32)+'/'+String(n).repeat(64);
test('native immutable wave overlaps independent reads, bounds retention and preserves native validation',async()=>{
 let active=0,peak=0,calls=0;const charges=[];
 const raw=async r=>{calls++;peak=Math.max(peak,++active);await new Promise(resolve=>setTimeout(resolve,5));active--;return{json:{etag:'one'},body:Uint8Array.of(1,2,3)};};
 const reads=[1,2,3].map(n=>({key:key(n),max_response_bytes:8}));
 const wave=await immutableReadWave(raw,reads,{reserve:(...args)=>charges.push(args)});
 assert.equal(peak,3);assert.equal(calls,3);assert.deepEqual(charges,[[3,3*(8+4096)]]);
 const req={op:'get',bucket:'database',key:key(1),max_response_bytes:8};
 const first=await wave.transport(req);first.body[0]=9;assert.equal((await wave.transport(req)).body[0],1);assert.equal(calls,3);
 await assert.rejects(wave.transport({...req,max_response_bytes:2}),/budget/);
 await wave.transport({...req,if_match_etag:'one'});assert.equal(calls,4);
 wave.close();await assert.rejects(wave.transport(req),/Closed/);
});
test('invalid plans, original deadlines, oversize inputs, and resource errors fail closed',async()=>{
 const read={key:key(1),max_response_bytes:8};let calls=0;
 const raw=async()=>{calls++;return{body:new Uint8Array(9)};};
 for(const plan of[[read,read],Array(4).fill(read),[{...read,key:'ROOT'}],[{...read,max_response_bytes:16385}]])await assert.rejects(immutableReadWave(raw,plan),/Invalid/);
 assert.equal(calls,0);
 await assert.rejects(immutableReadWave(raw,[read],{deadlineMs:100,clock:()=>100}),/Deadline/);
 const wave=await immutableReadWave(raw,[read]);await assert.rejects(wave.transport({op:'get',bucket:'database',key:key(1)}),/budget/);wave.close();
 const failure=new Error('SQLITE_NOMEM');const failed=await immutableReadWave(async()=>{throw failure;},[read]);
 await assert.rejects(failed.transport({op:'get',bucket:'database',key:key(1)}),e=>e===failure);failed.close();
});
