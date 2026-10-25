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
  constructor(commit,{clock=Date.now}={}) { this.clock=clock;this.commit=commit;this.queue=[];this.bytes=0;this.pending=0;this.running=false; }
  submit(input) {
    const bytes=new TextEncoder().encode(JSON.stringify(input)).byteLength;
    if(bytes>1024*1024)throw new Error("RequestTooLarge");
    if(this.pending>=16||this.bytes+bytes>4*1024*1024)throw new Error("CoordinatorCommitBusy");
    const release=reserveInput(bytes);
    this.pending++;this.bytes+=bytes;
    return new Promise((resolve,reject)=>{this.queue.push({input,bytes,resolve,reject,release,deadline:this.clock()+30000});this.schedule();});
  }
  schedule(){if(!this.running){this.running=true;setTimeout(()=>this.drain(),2);}}
  async drain(){
    const group=[];let bytes=0,records=0;
    while(this.queue.length&&group.length<16){const next=this.queue[0];if(this.clock()>=next.deadline){this.queue.shift();this.pending--;this.bytes-=next.bytes;next.release();next.reject(new Error("DeadlineExceeded"));continue;}const count=next.input.mutations?.length??0;if(group.length&&(bytes+next.bytes>768*1024||records+count>1024))break;group.push(this.queue.shift());bytes+=next.bytes;records+=count;}
    if(!group.length){this.running=false;if(this.queue.length)this.schedule();return;}
    const settle=async items=>{
      try{const results=await this.commit(items.map(item=>item.input),Math.min(...items.map(item=>item.deadline)));if(results.length!==items.length)throw new Error("InvalidMutationGroupResults");return results.map(value=>({value}));}
      catch(error){if(items.length>1&&/WalIdempotencyConflict|InvalidDocumentMutation|InvalidWalOperationId|InvalidWalBatch|WalRecordTooLarge|GraphPageRecordTooLarge|SyntaxError|UnexpectedToken|UnexpectedEndOfInput|InvalidCharacter|InvalidNumber|MissingField|UnknownField|Overflow/.test(String(error))){const results=[];for(const item of items)results.push(...await settle([item]));return results;}return items.map(()=>({error}));}
    };
    const results=await settle(group);
    this.pending-=group.length;this.bytes-=bytes;this.running=false;
    group.forEach((item,i)=>{item.release();results[i].error?item.reject(results[i].error):item.resolve(results[i].value);});
    if(this.queue.length)this.schedule();
  }
}
