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
import {CriticalPath} from "./trace.mjs";
import {reserveInput} from "./request-pool.mjs";
import {DurableObject} from "cloudflare:workers";
import {createRuntime} from "./runtime.mjs";
import {tenantEnvs} from "./tenants.mjs";
import {runtimeName,readerSlots,readOperations,buildOperations} from "./runtime-routing.mjs";
import {collect} from "./collect.mjs";
import module from "./dist/antfly-serverless-observe.wasm";
import {NativeReadViews} from "./native-read-views.mjs";
import {prepareReadAdmission} from "./read-admission.mjs";

class RuntimeActor extends DurableObject {
 constructor(ctx,env,role,binding){
  super(ctx,env);const instanceId=crypto.randomUUID(),selected=tenantEnvs(env).find(candidate=>Array.from({length:role==="reader"?readerSlots(candidate):1},(_,slot)=>slot).some(slot=>candidate[binding].idFromName(runtimeName(candidate.ANTFLY_INCARNATION,slot)).equals(ctx.id)));
  if(!selected)throw new Error("UnknownRuntimeTenant");this.runtimeEnv=selected;this.role=role;this.runtime=createRuntime({kernel:role==="reader"?readerKernel:buildKernel,sharedCache,resources:()=>({...kernelMetrics(),actor_instance_id:instanceId})});this.pending=[];this.pendingBytes=0;this.running=0;
  if(env.LOG_METRICS==="true")console.log(JSON.stringify({event:"antfly.actor_started",role,actor_id:ctx.id.toString(),actor_instance_id:instanceId,version:env.WORKER_VERSION?.id,...kernelMetrics()}));
  if(role==="reader"&&env.COORDINATOR_BACKEND==="r2"&&env.LOCAL_METADATA_READS==="true")this.readViews=new NativeReadViews(module,selected,{kernel:readerKernel,cache:sharedCache,onMetrics:sample=>{if(env.LOG_METRICS==="true")console.log(JSON.stringify({event:"antfly.reader_metadata",...sample,...kernelMetrics(),actor_instance_id:instanceId}));}});
 }
 execute(input,collection,traceMeta){
  if(!(this.role==="reader"?readOperations:buildOperations).has(input?.op))throw new Error("RuntimeCapabilityUnsupported");
  const inputBytes=new TextEncoder().encode(JSON.stringify([input,collection])).byteLength;
  if(inputBytes>1024*1024)throw new Error("RequestTooLarge");
  // Four bounded metadata records plus keys/ETags/objects fit in this charge.
  // Reserve before starting I/O, including preparations still awaiting RPC.
  const bytes=inputBytes+(this.role==="reader"?32768:0);
  if(this.pending.length+this.running>=8||this.pendingBytes+bytes>4*1024*1024)throw new Error("CoordinatorCommitBusy");
  const release=reserveInput(bytes);
  return new Promise((resolve,reject)=>{
   const trace=this.role==="reader"?new CriticalPath({id:traceMeta?.trace_id,scope:"actor:reader"}):null;
   const preparation=trace?prepareReadAdmission(this.runtimeEnv,input,trace,this.readViews).then(value=>({value}),error=>({error})):null;
   this.pendingBytes+=bytes;this.pending.push({input,collection,traceId:traceMeta?.trace_id,trace,preparation,bytes,resolve,reject,at:Date.now(),deadline:Date.now()+30000,release});this.drain();
  });
 }
 drain(){
  const capacity=this.role==="reader"?2:1;
  while(this.pending.length&&this.running<capacity){
   let index=0;if(this.role==="builder"){const urgent=this.pending.findIndex(job=>["publish","enrich"].includes(job.input.op));if(urgent>=0&&Date.now()-this.pending[0].at<5000)index=urgent;}
   const job=this.pending.splice(index,1)[0];this.running++;
   this.executeAdmitted(job).finally(()=>{this.running--;this.drain();});
  }
 }
 async executeAdmitted(job){
  const trace=job.trace??new CriticalPath({id:job.traceId,scope:"actor:"+this.role});trace.stage({name:"actor_queue",duration_ms:Date.now()-job.at});
  try{
   if(Date.now()>=job.deadline)throw new Error("DeadlineExceeded");
   const prepared=job.preparation?await job.preparation:null;if(prepared?.error)throw prepared.error;
   if(Date.now()>=job.deadline)throw new Error("DeadlineExceeded");
   const result=await this.perform(job.input,job.collection,AbortSignal.timeout(Math.max(1,job.deadline-Date.now())),trace,prepared?.value);
   job.resolve(result);
  }catch(error){trace.error("execute",error);job.reject(error);}
  finally{this.pendingBytes-=job.bytes;job.release();trace.emit(this.runtimeEnv.LOG_METRICS==="true");}
 }
 perform(input,collection,signal,trace,readAdmission){
  const env=this.runtimeEnv,stub=env.COMMITS.getByName(env.ANTFLY_INCARNATION);
  if(input.op==="collect")return collect(env,(request,token)=>this.runtime.run(env,request,signal,token,undefined,trace),input.job_id);
  if(input.op==="collect_metadata")return stub.collectMetadata(input.job_id,{scheduled:input.scheduled});
  if(input.op==="migrate_coordinator")return stub.migrateFromSqlite();
  return this.runtime.run(env,input,signal,collection,undefined,trace,readAdmission);
 }
}
export class R2ReadRuntime extends RuntimeActor {constructor(ctx,env){super(ctx,env,"reader","READERS");}}
export class R2BuildRuntime extends RuntimeActor {constructor(ctx,env){super(ctx,env,"builder","BUILDERS");}}
