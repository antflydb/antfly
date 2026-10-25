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

import {readerKernel,buildKernel,sharedCache,kernelMetrics} from "./kernels.mjs";
import {reserveInput} from "./request-pool.mjs";
import {DurableObject} from "cloudflare:workers";
import {createRuntime} from "./runtime.mjs";
import {tenantEnvs} from "./tenants.mjs";
import {runtimeName,readerSlots,readOperations,buildOperations} from "./runtime-routing.mjs";
import {collect} from "./collect.mjs";

class RuntimeActor extends DurableObject {
 constructor(ctx,env,role,binding){
  super(ctx,env);const selected=tenantEnvs(env).find(candidate=>Array.from({length:role==="reader"?readerSlots(candidate):1},(_,slot)=>slot).some(slot=>env[binding].idFromName(runtimeName(candidate.ANTFLY_INCARNATION,slot)).equals(ctx.id)));
  if(!selected)throw new Error("UnknownRuntimeTenant");this.runtimeEnv=selected;this.role=role;this.runtime=createRuntime({kernel:role==="reader"?readerKernel:buildKernel,sharedCache,resources:kernelMetrics});this.pending=[];this.pendingBytes=0;this.running=false;
 }
 execute(input,collection){
  if(!(this.role==="reader"?readOperations:buildOperations).has(input?.op))throw new Error("RuntimeCapabilityUnsupported");
  const bytes=new TextEncoder().encode(JSON.stringify([input,collection])).byteLength;
  if(bytes>1024*1024)throw new Error("RequestTooLarge");
  if(this.pending.length+(this.running?1:0)>=8||this.pendingBytes+bytes>4*1024*1024)throw new Error("CoordinatorCommitBusy");
  const release=reserveInput(bytes);
  return new Promise((resolve,reject)=>{this.pendingBytes+=bytes;this.pending.push({input,collection,bytes,resolve,reject,at:Date.now(),deadline:Date.now()+30000,release});this.drain();});
 }
 async drain(){
  if(this.running)return;this.running=true;
  while(this.pending.length){
   // Publication takes the next slot ahead of queued collection; aged work
   // becomes equally eligible. An executing bounded step is never preempted.
   let index=0;if(this.role==="builder"){const urgent=this.pending.findIndex(job=>["publish","enrich"].includes(job.input.op));if(urgent>=0&&Date.now()-this.pending[0].at<5000)index=urgent;}
   const job=this.pending.splice(index,1)[0];
   try{if(Date.now()>=job.deadline)throw new Error("DeadlineExceeded");const result=await this.perform(job.input,job.collection,AbortSignal.timeout(Math.max(1,job.deadline-Date.now())));this.pendingBytes-=job.bytes;job.release();job.resolve(result);}
   catch(error){this.pendingBytes-=job.bytes;job.release();job.reject(error);}
  }
  this.running=false;
 }
 perform(input,collection,signal){
  const env=this.runtimeEnv,stub=env.COMMITS.getByName(env.ANTFLY_INCARNATION);
  if(input.op==="collect")return collect(env,(request,token)=>this.runtime.run(env,request,signal,token),input.job_id);
  if(input.op==="collect_metadata")return stub.collectMetadata(input.job_id,{scheduled:input.scheduled});
  if(input.op==="migrate_coordinator")return stub.migrateFromSqlite();
  return this.runtime.run(env,input,signal,collection);
 }
}
export class R2ReadRuntime extends RuntimeActor {constructor(ctx,env){super(ctx,env,"reader","READERS");}}
export class R2BuildRuntime extends RuntimeActor {constructor(ctx,env){super(ctx,env,"builder","BUILDERS");}}
