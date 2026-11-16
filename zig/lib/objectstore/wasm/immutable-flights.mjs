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

import{isImmutableObjectKey}from'./cache.mjs';
import{ObjectStoreError}from'./r2.mjs';
// The registry contains only CPU wake tickets. Physical I/O promises, streams
// and response handles stay exclusively in the selected caller's continuation.
export class ImmutableFlights {
 constructor({maxFlights=4,maxWaiters=16,onCharge=()=>{},reserve=()=>()=>{}}={}){if(!Number.isInteger(maxFlights)||maxFlights<1||maxFlights>4||!Number.isInteger(maxWaiters)||maxWaiters<0||maxWaiters>16)throw Error("InvalidImmutableFlightLimits");this.entries=new Map();this.maxFlights=maxFlights;this.maxWaiters=maxWaiters;this.onCharge=onCharge;this.reserve=reserve;}
 charge(){this.onCharge([...this.entries.values()].reduce((n,e)=>n+512+e.waiters*128,0));}
 wrap(readThrough,scope,{deadlineMs=Date.now()+30000}={}){const monoDeadline=performance.now()+Math.max(0,Math.min(30000,deadlineMs-Date.now()));return async(request,body)=>{
  if(request.op!=='get'||request.bucket!=='database'||!isImmutableObjectKey(request.key)||Object.keys(request).some(k=>!['op','bucket','key','range','if_match_etag','version_id','max_response_bytes'].includes(k)))return readThrough(request,body);
  const descriptor=JSON.stringify([scope,request.bucket,request.key,request.range?.offset??null,request.range?.length??null,request.if_match_etag??null,request.version_id??null,request.max_response_bytes??null]);
  if(descriptor.length>4096)return readThrough(request,body);
  const key=Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(descriptor))),b=>b.toString(16).padStart(2,'0')).join('');
  const prior=this.entries.get(key);
  if(prior&&prior.waiters<this.maxWaiters){
   const release=this.reserve(128);prior.waiters++;this.charge();
   try{
    // Each follower keeps a timer in its own request context. Resolving even a
    // CPU-only promise from the owner's context can cancel workerd followers.
    while(!prior.done){if(Date.now()>=deadlineMs||performance.now()>=monoDeadline)throw Error("DeadlineExceeded");await new Promise(resolve=>setTimeout(resolve,2));}
    if(prior.failure){if(prior.failure.status!=null)throw new ObjectStoreError(prior.failure.status,prior.failure.message);throw Error(prior.failure.message);}
    return await readThrough(request,body);
   }finally{prior.waiters--;release();this.charge();}
  }
  if(prior||this.entries.size>=this.maxFlights)return readThrough(request,body);
  const release=this.reserve(512),entry={waiters:0,done:false,failure:null};this.entries.set(key,entry);this.charge();let failure;
  try{return await readThrough(request,body);}
  catch(error){failure={message:String(error.message??error).slice(0,2048),...(error instanceof ObjectStoreError?{status:error.status}:{})};throw error;}
  finally{this.entries.delete(key);entry.failure=failure;entry.done=true;release();this.charge();}
 };}
}
