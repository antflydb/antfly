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

// A single suspended stack per instance. I/O completes exactly once; rewind
// restores the original computation instead of re-executing writes or queries.
import { ObjectStoreError } from "./r2.mjs";

export function validatePutBatch(request, bodyLength) {
  if (!Array.isArray(request.items) || request.items.length < 1 || request.items.length > 8) throw new ObjectStoreError(3, "invalid upload batch");
  let end = 0;
  for (const item of request.items) {
    if (typeof item.key !== "string" || !Number.isSafeInteger(item.offset) || item.offset !== end || !Number.isSafeInteger(item.length) || item.length < 0 || item.length > bodyLength - end) throw new ObjectStoreError(3, "invalid upload batch extent");
    end += item.length;
  }
  if (end !== bodyLength) throw new ObjectStoreError(3, "unclaimed upload batch bytes");
  return request.items.length;
}

// Join every started upload before returning an error. Partially successful
// immutable uploads are orphans; callers never publish a partial batch.
export async function executePutBatch(transport, request, body, check = () => {}, concurrency = 4) {
  validatePutBatch(request, body.byteLength);
  if(!Number.isInteger(concurrency)||concurrency<1||concurrency>4)throw new ObjectStoreError(3,"invalid upload concurrency");
  const results = Array(request.items.length);
  let next = 0, failure;
  async function upload() {
    while (!failure && next < request.items.length) {
      const index = next++, item = request.items[index];
      try {
        check();
        const value = await transport({...item, op: "put", bucket: request.bucket}, body.subarray(item.offset, item.offset + item.length));
        results[index] = value.json;
      } catch (error) { failure ??= error instanceof Error ? error : new Error(String(error)); }
    }
  }
  await Promise.all(Array.from({length: Math.min(concurrency, request.items.length)}, upload));
  if (failure) throw failure;
  check();
  return {json: {results}};
}

// Independent bounded reads. The native caller anchors any authority proof
// BEFORE suspending; this host only overlaps transport and preserves ordering.
export function validateGetBatch(request,bodyLength=0){
 if(bodyLength!==0||!Array.isArray(request.items)||request.items.length<1||request.items.length>8)throw new ObjectStoreError(3,'invalid read batch');
 let total=0;
 for(const item of request.items){
  if(typeof item.key!=='string'||!item.key.length||item.key.length>4096||!Number.isSafeInteger(item.max_response_bytes)||item.max_response_bytes<1||item.max_response_bytes>4*1024*1024||item.if_match_etag!=null&&typeof item.if_match_etag!=='string')throw new ObjectStoreError(3,'invalid read batch item');
  total+=item.max_response_bytes;if(total>4*1024*1024)throw new ObjectStoreError(3,'read batch exceeds retained input budget');
 }
 return request.items.length;
}
export async function executeGetBatch(transport,request,check=()=>{},concurrency=4,onError=()=>{}){
 validateGetBatch(request);
 if(!Number.isInteger(concurrency)||concurrency<1||concurrency>4)throw new ObjectStoreError(3,'invalid read concurrency');
 const results=Array(request.items.length);let next=0,failure;
 async function read(){
  while(!failure&&next<request.items.length){
   const index=next++,item=request.items[index];
   try{
    check();const value=await transport({...item,op:'get',bucket:request.bucket},new Uint8Array());
    if(!(value.body instanceof Uint8Array)||value.body.byteLength>item.max_response_bytes||new TextEncoder().encode(JSON.stringify(value.json??{})).byteLength>4096)throw new ObjectStoreError(4,'read batch response exceeds budget');
    results[index]=value;
   }catch(error){if(error instanceof ObjectStoreError&&error.status===1)results[index]=null;else{try{onError(error);}catch{}failure??=error instanceof Error?error:new Error(String(error));}}
  }
 }
 await Promise.all(Array.from({length:Math.min(concurrency,request.items.length)},read));
 if(failure)throw failure;check();
 const total=results.reduce((n,value)=>n+(value?.body.byteLength??0),0),body=new Uint8Array(total);let offset=0;
 const headers=results.map(value=>{
  if(value===null)return{missing:true,offset,length:0,metadata:null};
  const result={missing:false,offset,length:value.body.byteLength,metadata:value.json};body.set(value.body,offset);offset+=value.body.byteLength;return result;
 });
 return{json:{results:headers},body};
}
