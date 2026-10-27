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
 constructor(create,slots,{recycleBytes=12*1024*1024,maxQueued=32,clock=Date.now,maxReservedBytes=Infinity,reservationBytes=0,defaultKernelVariant="default"}={}){this.create=create;this.clock=clock;this.defaultKernelVariant=defaultKernelVariant;this.maxReservedBytes=maxReservedBytes;this.reservationBytes=reservationBytes;this.reservedBytes=0;this.slots=Array.from({length:slots},()=>({host:null,busy:false}));this.recycleBytes=recycleBytes;this.maxQueued=maxQueued;this.pending=[];}
 request(input,options={}){return this.submit({input,options});}
 withHost(run,options={}){return this.submit({run,options});}
 submit(job){
  if(!this.slots.length)return Promise.reject(new Error("RuntimeCapabilityUnsupported"));
  job.reservationBytes=job.options.memoryReservationBytes??this.reservationBytes;job.hostKey=job.options.kernelVariant??this.defaultKernelVariant;
  if(!Number.isSafeInteger(job.reservationBytes)||job.reservationBytes<0||job.reservationBytes>this.maxReservedBytes||Number.isFinite(this.maxReservedBytes)&&job.reservationBytes===0)return Promise.reject(new Error("InvalidKernelMemoryReservation"));
  try{job.recycleBytes=typeof this.recycleBytes==="function"?this.recycleBytes(job):this.recycleBytes;}catch(error){return Promise.reject(error);}
  if(typeof job.recycleBytes!=="number"||Number.isNaN(job.recycleBytes)||job.recycleBytes<0)return Promise.reject(Error("InvalidKernelRecycleBudget"));
  if(job.options.signal?.aborted||this.clock()>=(job.options.deadlineMs??Infinity))return Promise.reject(new Error("DeadlineExceeded"));
  if(this.pending.length>=this.maxQueued)return Promise.reject(new Error("CoordinatorCommitBusy"));
  const admitted=new Promise((resolve,reject)=>{
   Object.assign(job,{admit:resolve,reject,queuedAt:performance.now(),admittedAt:this.clock()});let timer;
   const abort=()=>{const index=this.pending.indexOf(job);if(index<0)return;this.pending.splice(index,1);job.cleanup();reject(new Error("DeadlineExceeded"));this.drain();};
   job.cleanup=()=>{job.options.signal?.removeEventListener("abort",abort);clearTimeout(timer);};
   job.options.signal?.addEventListener("abort",abort,{once:true});
   if(Number.isFinite(job.options.deadlineMs))timer=setTimeout(abort,Math.max(1,job.options.deadlineMs-this.clock()));
   this.pending.push(job);this.drain();
  });
  // Register the continuation in the submitting request's I/O context. The
  // request releasing a slot only resolves admission; it must never execute
  // another Durable Object's callback or create its timers/transport promises.
  return admitted.then(slot=>this.execute(slot,job));
 }
 drain(){while(this.pending.length){
  const idle=this.slots.filter(slot=>!slot.busy);if(!idle.length)return;
  const now=this.clock(),rank=job=>now-(job.admittedAt??now)>=5000?0:job.options.priority==="commit"?-1:job.options.priority==="maintenance"?2:job.options.priority==="publication"?1:0;
  const available=this.maxReservedBytes-this.reservedBytes;
  // Large aged work drains smaller running reservations instead of starving.
  if(this.pending.some(job=>now-job.admittedAt>=5000&&job.reservationBytes>available))return;
  let selected=-1;for(let i=0;i<this.pending.length;i++){
   const a=this.pending[i];if(a.reservationBytes>available)continue;
   const b=selected<0?null:this.pending[selected];if(!b||rank(a)<rank(b)||rank(a)===rank(b)&&b.options.fairKey===this.lastKey&&a.options.fairKey!==this.lastKey)selected=i;
  }
  if(selected<0)return;
  const job=this.pending.splice(selected,1)[0];this.lastKey=job.options.fairKey;
  const slot=idle.find(s=>s.host&&s.hostKey===job.hostKey&&s.hostReservationBytes===job.reservationBytes)??idle.find(s=>!s.host)??idle[0];
  if(slot.host&&(slot.hostKey!==job.hostKey||slot.hostReservationBytes!==job.reservationBytes))slot.host=null;
  // Count busy reservations, including hosts still being instantiated, and
  // retained idle hosts. Evict idle variants before reserving more memory.
  for(const other of this.slots){
   const retained=this.slots.reduce((n,s)=>n+(s.busy?s.activeReservationBytes:s.host?s.hostReservationBytes??0:0),0);
   if(retained-(slot.host?slot.hostReservationBytes??0:0)+job.reservationBytes<=this.maxReservedBytes)break;
   if(other!==slot&&!other.busy)other.host=null;
  }
  this.reservedBytes+=job.reservationBytes;slot.activeReservationBytes=job.reservationBytes;slot.busy=true;job.admit(slot);
 }}

 async execute(slot,job){
  job.cleanup();
  const stage=(name,start)=>{try{job.options.onStage?.({name,duration_ms:performance.now()-start});}catch{}};
  stage("kernel_queue",job.queuedAt);
  try{await new Promise(resolve=>setTimeout(resolve,0));if(job.options.signal?.aborted||this.clock()>=(job.options.deadlineMs??Infinity))throw new Error("DeadlineExceeded");if(!slot.host){const start=performance.now();slot.host=await this.create(job);slot.hostKey=job.hostKey;slot.hostReservationBytes=job.reservationBytes;stage("wasm_instantiate",start);}if(job.options.signal?.aborted||this.clock()>=(job.options.deadlineMs??Infinity))throw new Error("DeadlineExceeded");return await (job.run?job.run(slot.host):slot.host.request(job.input,job.options));}
  finally{
   const release=()=>{if(slot.host&&(!slot.host.usable()||slot.host.memoryBytes()>=job.recycleBytes))slot.host=null;this.reservedBytes-=job.reservationBytes;slot.activeReservationBytes=0;slot.busy=false;this.drain();};
   const draining=slot.host?.draining?.();
   if(draining)Promise.resolve(draining).then(release,release);else release();
  }
 }
 memoryBytes(){return this.slots.reduce((n,slot)=>n+(slot.host?.memoryBytes()??0),0);}
}
let inputBytes=0;
export function reserveInput(bytes){if(inputBytes+bytes>8*1024*1024)throw new Error("CoordinatorCommitBusy");inputBytes+=bytes;let released=false;return()=>{if(!released){released=true;inputBytes-=bytes;}};}
