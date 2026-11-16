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

// One request-owned wave, derived by native code from an accepted metadata
// hint. It never grants authority or interprets page bytes. Native decoding and
// final conditional commit remain mandatory. The caller owns the pool permit.
import {ObjectStoreError} from './r2.mjs';
export async function immutableReadWave(transport,reads,{bucket='database',reserve=()=>{},deadlineMs=Infinity,clock=Date.now}={}){
 if(!Array.isArray(reads)||reads.length>3)throw Error('InvalidImmutableReadWave');
 const keys=new Set();let charge=0;
 for(const r of reads){if(typeof r?.key!=='string'||r.key.length>4096||!/\/wal-v1\/[a-f0-9]{64}\/pages\/(?:[a-f0-9]{32}\/)?[a-f0-9]{64}$/.test(r.key)||!Number.isInteger(r.max_response_bytes)||r.max_response_bytes<1||r.max_response_bytes>16384||keys.has(r.key))throw Error('InvalidImmutableReadWave');keys.add(r.key);charge+=r.max_response_bytes+4096;}
 if(clock()>=deadlineMs)throw Error('DeadlineExceeded');
 reserve(reads.length,charge);const values=new Map();let closed=false;
 await Promise.all(reads.map(async r=>{
  try{const value=await transport({op:'get',bucket,key:r.key,max_response_bytes:r.max_response_bytes},new Uint8Array());
   if((value.body?.byteLength??0)>r.max_response_bytes||new TextEncoder().encode(JSON.stringify(value.json??{})).byteLength>4096)throw new ObjectStoreError(4,'immutable wave response exceeds budget');
   values.set(r.key,{value});
  }catch(error){values.set(r.key,{error});}
 }));
 if(clock()>=deadlineMs){values.clear();throw Error('DeadlineExceeded');}
 return {transport:async(request,body)=>{
  if(closed)throw Error('ImmutableReadWaveClosed');
  const entry=request.op==='get'&&request.bucket===bucket&&request.range==null&&request.if_match_etag==null&&request.version_id==null?values.get(request.key):null;
  if(!entry)return transport(request,body);
  if(entry.error)throw entry.error;
  if(entry.value.body.byteLength>(request.max_response_bytes??Infinity))throw new ObjectStoreError(4,'immutable wave response exceeds native budget');
  return {json:structuredClone(entry.value.json),body:entry.value.body.slice()};
 },close(){closed=true;values.clear();},retained_bytes:charge};
}
