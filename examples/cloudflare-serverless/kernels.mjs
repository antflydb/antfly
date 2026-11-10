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

import full from "./dist/antfly-serverless.wasm";
import compact from "./dist/antfly-serverless-compact.wasm";
import observe from "./dist/antfly-serverless-observe.wasm";
import {instantiateHost} from "../../zig/lib/objectstore/wasm/asyncify.mjs";
import{pointDependencyMetrics}from"../../zig/lib/objectstore/wasm/read-dependencies.mjs";
import{readAheadMetrics}from"../../zig/lib/objectstore/wasm/read-ahead.mjs";
import{createRoleCache}from"./role-cache.mjs";
import {RequestPool} from "./request-pool.mjs";
export const sharedCache=createRoleCache();
export const readerKernel=new RequestPool(job=>instantiateHost(job.hostKey==="observe"?observe:compact,null),2,{maxReservedBytes:32*1024*1024,reservationBytes:16*1024*1024,defaultKernelVariant:"compact",recycleBytes:job=>(job.hostKey==="observe"?7:12)*1024*1024});
const standaloneMetadataKernel=new RequestPool(()=>instantiateHost(compact,null),1);
export let metadataKernel=standaloneMetadataKernel;
let snapshotKernel=new RequestPool(job=>instantiateHost(job.hostKey==="compact"?compact:observe,null),0,{maxReservedBytes:16*1024*1024,reservationBytes:8*1024*1024,defaultKernelVariant:"observe",recycleBytes:job=>(job.hostKey==="compact"?12:7)*1024*1024});
export function readObservationKernel(){return ownerReadsEnabled&&residentKernel===writerKernel?{request:(input,options)=>writerKernel.request(input,{kernelVariant:"observe",memoryReservationBytes:8*1024*1024,...options}),withHost:(run,options)=>writerKernel.withHost(run,{kernelVariant:"observe",memoryReservationBytes:8*1024*1024,...options}),tryWithHost:(run,options)=>writerKernel.tryWithHost(run,{kernelVariant:"observe",memoryReservationBytes:8*1024*1024,...options})}:snapshotKernel.slots.length?snapshotKernel:metadataKernel;}
const standaloneBuildKernel=new RequestPool(job=>instantiateHost(job.options.kernelVariant==="compact"?compact:full,null),1,{recycleBytes:job=>(job.hostKey==="compact"?12:24)*1024*1024,maxReservedBytes:32*1024*1024,reservationBytes:32*1024*1024});
export let buildKernel=standaloneBuildKernel;
const writerKernel=new RequestPool(job=>instantiateHost(job.hostKey==="full"?full:job.hostKey==="observe"?observe:compact,null),0,{maxReservedBytes:48*1024*1024,reservationBytes:16*1024*1024,defaultKernelVariant:"compact",recycleBytes:job=>(job.hostKey==="full"?24:job.hostKey==="observe"?7:12)*1024*1024});
export let residentKernel=new RequestPool(job=>instantiateHost(job.hostKey==="observe"?observe:compact,null),0,{maxReservedBytes:16*1024*1024,reservationBytes:16*1024*1024,defaultKernelVariant:"compact",recycleBytes:job=>(job.hostKey==="observe"?7:12)*1024*1024});
let ownerReadsEnabled=false;
export function enableOwnerReads({shared=false}={}){
 if(ownerReadsEnabled){if(shared!==(residentKernel===snapshotKernel))throw Error("ResidentExecutionPolicyMismatch");return;}
 if(runtimeRole!=="writer"||writerKernel.pending.length||writerKernel.slots.some(slot=>slot.host||slot.busy))throw Error("ResidentRuntimeAlreadyStarted");
 // Partition the existing 48 MiB execution allowance: 32 MiB commit/preparation
 // and 16 MiB point reads. The separate observation allowance stays unchanged.
 if(shared){
  if(snapshotKernel.pending.length||snapshotKernel.slots.some(slot=>slot.host||slot.busy))throw Error("ResidentRuntimeAlreadyStarted");
  writerKernel.maxReservedBytes=64*1024*1024;writerKernel.slots=Array.from({length:8},()=>({host:null,busy:false}));
  snapshotKernel=writerKernel;residentKernel=writerKernel;residentKernel.pointDependencySlots=2;ownerReadsEnabled=true;return;
 }
 writerKernel.slots=Array.from({length:2},()=>({host:null,busy:false}));writerKernel.maxReservedBytes=32*1024*1024;
 residentKernel.slots=Array.from({length:2},()=>({host:null,busy:false}));ownerReadsEnabled=true;
}
let runtimeRole="shared";
const roles={shared:[2,1,1,0,0,80],writer:[0,0,0,2,3,64],reader:[4,0,0,0,0,32],builder:[0,0,1,0,0,32],app:[0,0,0,0,0,0],"app-reader":[4,0,0,0,0,32]};
export function configureKernels(role){
 if(ownerReadsEnabled)throw Error("RuntimeAlreadyStarted");
 const layout=roles[role];if(!layout)throw new Error("InvalidRuntimeRole");if(runtimeRole!=="shared"&&runtimeRole!==role&&!(runtimeRole==="app"&&role==="app-reader"))throw new Error("RuntimeRoleMismatch");
 for(const[pool,index]of[[readerKernel,0],[standaloneMetadataKernel,1],[standaloneBuildKernel,2],[snapshotKernel,3],[writerKernel,4]]){if(pool.pending.length||pool.externalPending||readAheadMetrics(pool).proof_prefetch_active||pointDependencyMetrics(pool).point_dependency_active||pool.slots.some(slot=>slot.host||slot.busy))throw new Error("RuntimeAlreadyStarted");pool.slots=Array.from({length:layout[index]},()=>({host:null,busy:false}));}
 metadataKernel=role==="writer"?writerKernel:standaloneMetadataKernel;buildKernel=role==="writer"?writerKernel:standaloneBuildKernel;
 runtimeRole=role;
}
// Upgrade only the app profile, before any kernel has admitted work.
export function enableAppReads(){if(runtimeRole==="app-reader")return;if(runtimeRole!=="app")throw Error("DirectReaderRequiresSplitApp");configureKernels("app-reader");}
const stores=new Map();
export function boundRows(store){stores.delete(store);stores.set(store,true);while(stores.size>8){const oldest=stores.keys().next().value;oldest.clearServingTail?.();oldest.rows.clear();oldest.rowBytes=0;oldest.hintBytes=0;oldest.cursorHintBytes=0;oldest.rootHint=null;oldest.readHints=null;stores.delete(oldest);}}

export function kernelMetrics(){return {...Object.fromEntries(Object.entries(pointDependencyMetrics(residentKernel)).map(([key,value])=>["resident_"+key,value])),...readAheadMetrics(readerKernel),...pointDependencyMetrics(readerKernel),reader_pool_slots:readerKernel.slots.length,reader_pool_queued:readerKernel.pending.length+(readerKernel.externalPending??0),reader_pool_queue_limit:readerKernel.maxQueued,reader_pool_reserved_bytes:readerKernel.reservedBytes,reader_pool_limit_bytes:readerKernel.maxReservedBytes,reader_pool_linear_bytes:readerKernel.memoryBytes(),resident_pool_slots:residentKernel.slots.length,resident_pool_queued:residentKernel.pending.length,resident_pool_reserved_bytes:residentKernel.reservedBytes,resident_pool_limit_bytes:residentKernel.slots.length?residentKernel.maxReservedBytes:0,resident_pool_linear_bytes:residentKernel.memoryBytes(),observation_pool_linear_bytes:snapshotKernel.memoryBytes(),observation_pool_slots:snapshotKernel.slots.length,writer_pool_slots:writerKernel.slots.length,writer_pool_limit_bytes:writerKernel.maxReservedBytes,writer_pool_reserved_bytes:writerKernel.reservedBytes,writer_pool_linear_bytes:writerKernel.memoryBytes(),wasm_pool_linear_bytes:[...new Set([readerKernel,standaloneMetadataKernel,standaloneBuildKernel,snapshotKernel,writerKernel,residentKernel])].reduce((bytes,pool)=>bytes+pool.memoryBytes(),0),execution_pool_shared:residentKernel===writerKernel||residentKernel===snapshotKernel,runtime_role:runtimeRole,wasm_pool_limit_bytes:roles[runtimeRole][5]*1024*1024,shared_cache_bytes:sharedCache.retainedBytes(),shared_metadata_cache_bytes:sharedCache.metadataBytes(),shared_data_cache_bytes:sharedCache.dataBytes(),decoded_row_charge_bytes:[...stores.keys()].reduce((n,store)=>n+store.rowBytes+(store.hintBytes??0)+(store.cursorHintBytes??0)+(store.servingTailBytes??0),0)};}
