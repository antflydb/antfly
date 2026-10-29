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

import {reserveInput} from "./request-pool.mjs";

// A bounded admission queue. The timer lets concurrently arriving RPCs share
// one native update; callers are resolved only after its durable commit.
export class MutationGroups {
  constructor(commit,{clock=Date.now,monotonic=()=>performance.now()}={}) { this.clock=clock;this.monotonic=monotonic;this.commit=commit;this.queue=[];this.bytes=0;this.pending=0;this.running=false; }
  submit(input) {
    const bytes=new TextEncoder().encode(JSON.stringify(input)).byteLength;
    if(bytes>1024*1024)throw new Error("RequestTooLarge");
    if(this.pending>=16||this.bytes+bytes>4*1024*1024)throw new Error("CoordinatorCommitBusy");
    const release=reserveInput(bytes);
    this.pending++;this.bytes+=bytes;
    return new Promise((resolve,reject)=>{
      let admit;const ready=new Promise(start=>admit=start);
      ready.then(async owner=>{if(!owner)return;await new Promise(start=>setTimeout(start,2));await this.drain();});
      this.queue.push({input,bytes,submitted:this.monotonic(),resolve:value=>{admit(false);resolve(value);},reject:error=>{admit(false);reject(error);},startGroup:()=>admit(true),release,deadline:this.clock()+30000});this.schedule();
    });
  }
  schedule(){if(!this.running&&this.queue.length){this.running=true;this.queue[0].startGroup();}}
  async drain(){
    const group=[],expired=[];let bytes=0,records=0;
    while(this.queue.length&&group.length<16){const next=this.queue[0];if(this.clock()>=next.deadline){expired.push(this.queue.shift());if(!group.length)break;continue;}const count=next.input.mutations?.length??0;if(group.length&&(bytes+next.bytes>768*1024||records+count>1024))break;group.push(this.queue.shift());bytes+=next.bytes;records+=count;}
    const expire=()=>{for(const item of expired){this.pending--;this.bytes-=item.bytes;item.release();item.reject(new Error("DeadlineExceeded"));}};
    if(!group.length){this.running=false;this.schedule();expire();return;}
    const settle=async items=>{
      try{const results=await this.commit(items.map(item=>item.input),Math.min(...items.map(item=>item.deadline)),{queue_wait_ms:Math.max(...items.map(item=>Math.max(0,this.monotonic()-item.submitted)))});if(results.length!==items.length)throw new Error("InvalidMutationGroupResults");return results.map(value=>({value}));}
      catch(error){if(items.length>1&&/WalIdempotencyConflict|InvalidDocumentMutation|InvalidWalOperationId|InvalidWalBatch|WalRecordTooLarge|GraphPageRecordTooLarge|SyntaxError|UnexpectedToken|UnexpectedEndOfInput|InvalidCharacter|InvalidNumber|MissingField|UnknownField|Overflow/.test(String(error))){const results=[];for(const item of items)results.push(...await settle([item]));return results;}return items.map(()=>({error}));}
    };
    const results=await settle(group);
    this.pending-=group.length;this.bytes-=bytes;this.running=false;
    // Handoff and accounting finish before answering the owner RPC. Future
    // groups run through their own submitter, not this timer's old context.
    this.schedule();expire();
    group.forEach((item,i)=>{item.release();results[i].error?item.reject(results[i].error):item.resolve(results[i].value);});
  }
}
