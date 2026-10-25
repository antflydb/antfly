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

// A module-scope pool is shared by colocated Durable Objects. Metadata has a
// separate pool so engines waiting on coordinator RPCs cannot exhaust its slots.
export class RequestPool {
 constructor(create,slots,{recycleBytes=12*1024*1024,maxQueued=32,clock=Date.now}={}){this.create=create;this.clock=clock;this.slots=Array.from({length:slots},()=>({host:null,busy:false}));this.recycleBytes=recycleBytes;this.maxQueued=maxQueued;this.pending=[];}
 request(input,options={}){return this.submit({input,options});}
 withHost(run,options={}){return this.submit({run,options});}
 submit(job){
  if(job.options.signal?.aborted||this.clock()>=(job.options.deadlineMs??Infinity))return Promise.reject(new Error("DeadlineExceeded"));
  if(this.pending.length>=this.maxQueued)return Promise.reject(new Error("CoordinatorCommitBusy"));
  return new Promise((resolve,reject)=>{
   Object.assign(job,{resolve,reject});let timer;
   const abort=()=>{const index=this.pending.indexOf(job);if(index<0)return;this.pending.splice(index,1);job.cleanup();reject(new Error("DeadlineExceeded"));};
   job.cleanup=()=>{job.options.signal?.removeEventListener("abort",abort);clearTimeout(timer);};
   job.options.signal?.addEventListener("abort",abort,{once:true});
   if(Number.isFinite(job.options.deadlineMs))timer=setTimeout(abort,Math.max(1,job.options.deadlineMs-this.clock()));
   this.pending.push(job);this.drain();
  });
 }
 drain(){for(const slot of this.slots){if(slot.busy||!this.pending.length)continue;slot.busy=true;this.execute(slot,this.pending.shift());}}
 async execute(slot,job){
  job.cleanup();
  try{await new Promise(resolve=>setTimeout(resolve,0));if(job.options.signal?.aborted||this.clock()>=(job.options.deadlineMs??Infinity))throw new Error("DeadlineExceeded");slot.host??=await this.create();if(job.options.signal?.aborted||this.clock()>=(job.options.deadlineMs??Infinity))throw new Error("DeadlineExceeded");job.resolve(await (job.run?job.run(slot.host):slot.host.request(job.input,job.options)));}
  catch(error){job.reject(error);}
  finally{if(slot.host&&(!slot.host.usable()||slot.host.memoryBytes()>=this.recycleBytes))slot.host=null;slot.busy=false;this.drain();}
 }
 memoryBytes(){return this.slots.reduce((n,slot)=>n+(slot.host?.memoryBytes()??0),0);}
}
let inputBytes=0;
export function reserveInput(bytes){if(inputBytes+bytes>8*1024*1024)throw new Error("CoordinatorCommitBusy");inputBytes+=bytes;let released=false;return()=>{if(!released){released=true;inputBytes-=bytes;}};}
