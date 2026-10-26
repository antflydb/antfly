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
import {createImmutableReadCache} from "../../zig/lib/objectstore/wasm/cache.mjs";
import {RequestPool} from "./request-pool.mjs";
export const sharedCache=createImmutableReadCache({includeCommitLog:true,maxEntries:1024,writeThrough:true});
export const readerKernel=new RequestPool(job=>instantiateHost(job.hostKey==="observe"?observe:compact,null),2,{maxReservedBytes:32*1024*1024,reservationBytes:16*1024*1024,defaultKernelVariant:"compact"});
const standaloneMetadataKernel=new RequestPool(()=>instantiateHost(compact,null),1);
export let metadataKernel=standaloneMetadataKernel;
const snapshotKernel=new RequestPool(()=>instantiateHost(observe,null),0,{maxReservedBytes:16*1024*1024,reservationBytes:8*1024*1024,recycleBytes:7*1024*1024});
export function readObservationKernel(){return snapshotKernel.slots.length?snapshotKernel:metadataKernel;}
const standaloneBuildKernel=new RequestPool(job=>instantiateHost(job.options.kernelVariant==="compact"?compact:full,null),1,{recycleBytes:24*1024*1024,maxReservedBytes:32*1024*1024,reservationBytes:32*1024*1024});
export let buildKernel=standaloneBuildKernel;
const writerKernel=new RequestPool(job=>instantiateHost(job.hostKey==="full"?full:compact,null),0,{maxReservedBytes:48*1024*1024,reservationBytes:16*1024*1024,defaultKernelVariant:"compact",recycleBytes:24*1024*1024});
let runtimeRole="shared";
const roles={shared:[2,1,1,0,0,80],writer:[0,0,0,2,3,64],reader:[4,0,0,0,0,32],builder:[0,0,1,0,0,32],app:[0,0,0,0,0,0]};
export function configureKernels(role){
 const layout=roles[role];if(!layout)throw new Error("InvalidRuntimeRole");if(runtimeRole!=="shared"&&runtimeRole!==role)throw new Error("RuntimeRoleMismatch");
 for(const[pool,index]of[[readerKernel,0],[standaloneMetadataKernel,1],[standaloneBuildKernel,2],[snapshotKernel,3],[writerKernel,4]]){if(pool.pending.length||pool.slots.some(slot=>slot.host||slot.busy))throw new Error("RuntimeAlreadyStarted");pool.slots=Array.from({length:layout[index]},()=>({host:null,busy:false}));}
 metadataKernel=role==="writer"?writerKernel:standaloneMetadataKernel;buildKernel=role==="writer"?writerKernel:standaloneBuildKernel;
 runtimeRole=role;
}
const stores=new Map();
export function boundRows(store){stores.delete(store);stores.set(store,true);while(stores.size>8){const oldest=stores.keys().next().value;oldest.rows.clear();oldest.rowBytes=0;stores.delete(oldest);}}

export function kernelMetrics(){return {reader_pool_slots:readerKernel.slots.length,reader_pool_reserved_bytes:readerKernel.reservedBytes,reader_pool_limit_bytes:readerKernel.maxReservedBytes,reader_pool_linear_bytes:readerKernel.memoryBytes(),observation_pool_linear_bytes:snapshotKernel.memoryBytes(),observation_pool_slots:snapshotKernel.slots.length,writer_pool_slots:writerKernel.slots.length,writer_pool_limit_bytes:writerKernel.maxReservedBytes,writer_pool_reserved_bytes:writerKernel.reservedBytes,writer_pool_linear_bytes:writerKernel.memoryBytes(),wasm_pool_linear_bytes:readerKernel.memoryBytes()+standaloneMetadataKernel.memoryBytes()+standaloneBuildKernel.memoryBytes()+snapshotKernel.memoryBytes()+writerKernel.memoryBytes(),runtime_role:runtimeRole,wasm_pool_limit_bytes:roles[runtimeRole][5]*1024*1024,shared_cache_bytes:sharedCache.retainedBytes(),decoded_row_charge_bytes:[...stores.keys()].reduce((n,store)=>n+store.rowBytes,0)};}
