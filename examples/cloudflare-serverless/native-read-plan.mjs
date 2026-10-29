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

import {withReadView} from './read-view.mjs';
import {ObjectStoreError} from '../../zig/lib/objectstore/wasm/r2.mjs';

// Only native immutable source descriptors survive requests. No completed
// authority view, context, promise, pin admission time or caller I/O is retained.
export class NativeReadPlans {
 constructor({clock=()=>performance.now()}={}){this.clock=clock;this.buckets=new WeakMap();}
 key(env){return JSON.stringify([env.ANTFLY_INCARNATION,env.COORDINATOR_STORAGE_ID??env.ANTFLY_INCARNATION]);}
 get(env){const map=this.buckets.get(env.DATABASE),key=this.key(env),value=map?.get(key);if(!value)return null;if(this.clock()-value.at>=60000){map.delete(key);return null;}return structuredClone(value.plan);}
 remember(env,plan){if(!plan){this.buckets.get(env.DATABASE)?.delete(this.key(env));return;}const encoded=JSON.stringify(plan);if(encoded.length>4096||plan.format!==1)return;let map=this.buckets.get(env.DATABASE);if(!map)this.buckets.set(env.DATABASE,map=new Map());const key=this.key(env);map.delete(key);while(map.size>=8)map.delete(map.keys().next().value);map.set(key,{at:this.clock(),plan:structuredClone(plan)});}
}

// Preparation owns no authority-bearing transport and returns its WASM slot
// before waiting for fresh admission.
export function withNativeReadPreparation(transport,{onMetrics=()=>{}}={}){
 return async(request,body)=>{
  if(request.op==='bucket_exists'&&request.bucket==='database')return transport(request,body);
  if(!['get','head'].includes(request.op)||request.bucket!=='database'||!/^antfly\/artifacts\/(sha256\/[a-f0-9]{2}\/[a-f0-9]{62}|graph\/[a-f0-9]{64}\/[a-f0-9]{32}\/[a-f0-9]{64})$/.test(request.key??''))throw new ObjectStoreError(3,'immutable read preparation only');
  onMetrics({native_read_plan_prepared_reads:1});return transport(request,body);
 };
}

// Native code issues the barrier only after immutable preparation. Fresh
// admission is already running independently. Once the barrier resolves, the
// ordinary native live reader validates HEAD, floors, pins and the WAL overlay.
export function withNativeReadPlan(transport,coordinator,namespace,{fresh,onMetrics=()=>{}}){
 let admitted=null,barrier=false;
 return async(request,body)=>{
  if(request.op==='read_view_barrier'){
   if(barrier)throw new ObjectStoreError(3,'duplicate read admission barrier');
   barrier=true;const admission=await fresh;
   if(!admission?.view||admission.legacy)throw new ObjectStoreError(3,'native read admission required');
   admitted=withReadView(transport,coordinator,namespace,{initial:admission.view,admittedAt:admission.started,onMetrics});
   onMetrics(admission.physical??{});onMetrics({native_read_plan_barriers:1});return{};
  }
  if(admitted)return admitted(request,body);
  if(request.op==='bucket_exists'&&request.bucket==='database')return transport(request,body);
  if(!['get','head'].includes(request.op)||request.bucket!=='database'||!/^antfly\/artifacts\/(sha256\/[a-f0-9]{2}\/[a-f0-9]{62}|graph\/[a-f0-9]{64}\/[a-f0-9]{32}\/[a-f0-9]{64})$/.test(request.key??''))throw new ObjectStoreError(3,'immutable read preparation only');
  onMetrics({native_read_plan_prepared_reads:1});return transport(request,body);
 };
}


const speculativePools=new WeakMap(),MiB=1024*1024;
// Leave one eight-MiB slot available throughout multi-call authority admission,
// not merely for its first observation. Only promise resolution crosses owners;
// all query/admission I/O resumes in the submitting request's own continuation.
export async function withNativeReadPlanPermit(kernel,bytes,run,{deadlineMs=Date.now()+30000,signal}={}){
 const capacity=kernel.maxReservedBytes-8*MiB,slots=kernel.slots.length-1;
 if(!Number.isSafeInteger(bytes)||bytes<=0||!Number.isFinite(deadlineMs))throw Error('InvalidKernelMemoryReservation');
 if(slots<1||bytes>capacity)return run(false);
 let pool=speculativePools.get(kernel);if(!pool){pool={bytes:0,active:0,pending:[]};speculativePools.set(kernel,pool);}
 if(signal?.aborted||Date.now()>=deadlineMs)throw Error('DeadlineExceeded');
 if(pool.pending.length>=64)throw Error('CoordinatorCommitBusy');
 const drain=()=>{if(kernel.pending?.some(job=>(kernel.clock?.()??Date.now())-job.admittedAt>=5000))return;for(let i=0;i<pool.pending.length;){const item=pool.pending[i];if(pool.active>=slots||pool.bytes+item.bytes>capacity){i++;continue;}pool.pending.splice(i,1);item.cleanup();pool.bytes+=item.bytes;pool.active++;kernel.waitingReadPlans=(kernel.waitingReadPlans??0)+1;item.resolve();}};
 (kernel.capacityListeners??=new Set()).add(drain);
 await new Promise((resolve,reject)=>{
  let timer;const item={bytes,resolve,cleanup:()=>{clearTimeout(timer);signal?.removeEventListener('abort',cancel);}};
  const cancel=()=>{const index=pool.pending.indexOf(item);if(index<0)return;pool.pending.splice(index,1);item.cleanup();reject(Error('DeadlineExceeded'));kernel.capacityListeners.delete(drain);drain();};
  timer=setTimeout(cancel,Math.max(0,deadlineMs-Date.now()));signal?.addEventListener('abort',cancel,{once:true});pool.pending.push(item);drain();
 });
 let released=false;const release=()=>{if(released)return;released=true;pool.bytes-=bytes;pool.active--;kernel.waitingReadPlans--;kernel.drain?.();drain();};
 try{if(signal?.aborted||Date.now()>=deadlineMs)throw Error('DeadlineExceeded');return await run(true,release);}
 finally{release();drain();kernel.capacityListeners.delete(drain);}
}
