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

import {ObjectStoreError} from "../../zig/lib/objectstore/wasm/r2.mjs";

const KiB=1024,MiB=1024*KiB;
// The small module has an actual eight-MiB WebAssembly.Memory maximum. Reserve
// that maximum, not its observed high-water mark. Bound completions before
// Asyncify copies them into native memory, as well as native live allocations.
export async function withPointReadLane(kernel,run,options={}){
 let capacity=false,workingSetLimited=false,usedOperations=0,usedBytes=0,planned=false,operationLimit=1024,transferLimit=32*MiB;
 const accounting=(host,input,requestOptions)=>{
  if(!planned)return host.request(input,requestOptions);
  if(usedOperations>=operationLimit||usedBytes>=transferLimit)throw Error("ObjectIoBudgetExceeded");
  const observe=requestOptions.onMetrics;
  return host.request(input,{...requestOptions,maxOperations:Math.min(requestOptions.maxOperations??1024,operationLimit-usedOperations),maxTransferBytes:Math.min(requestOptions.maxTransferBytes??32*MiB,transferLimit-usedBytes),onMetrics:sample=>{usedOperations+=sample.operations;usedBytes+=sample.transferred_bytes;observe?.(sample);}});
 };
 try{
  return await kernel.withHost(host=>{
   let transferred=0;
   const limited={...host,request:(input,requestOptions={})=>{
    if(input.op!=="lookup")throw Error("PointReadOperationUnsupported");
    planned=input.native_read_plan_mode===true;
    operationLimit=requestOptions.maxOperations??1024;transferLimit=requestOptions.maxTransferBytes??32*MiB;
    workingSetLimited=input.working_set_bytes==null||input.working_set_bytes>128*KiB;
    return accounting(host,{...input,working_set_bytes:Math.min(input.working_set_bytes??128*KiB,128*KiB)},{...requestOptions,maxTransferBytes:Math.min(requestOptions.maxTransferBytes??MiB,MiB),transport:async(request,body)=>{
    if(!["get","head","bucket_exists","read_view_barrier"].includes(request.op))throw Error("PointReadWriteForbidden");
    const result=await requestOptions.transport(request,body),bytes=(result.body?.byteLength??0)+new TextEncoder().encode(JSON.stringify(result.json??{})).byteLength;
    if(bytes>128*KiB||transferred+bytes>512*KiB){capacity=true;throw new ObjectStoreError(8,"PointReadNeedsCapacity");}
    transferred+=bytes;return result;
   }});}};
   return run(limited);
  },{...options,kernelVariant:"observe",memoryReservationBytes:8*MiB});
 }catch(error){
  // A normal capacity promotion is safe only for the immutable point-read
  // operation and its original admitted snapshot. Never replay traps, OOM,
  // integrity/transport failures, deadlines or arbitrary database errors.
  if(!(capacity&&error?.message==="ObjectIoBudgetExceeded")&&!(workingSetLimited&&error?.message==="HostedWorkingSetBudgetExceeded"))throw error;
  await options.beforePromote?.();
  options.onPromote?.();
  return kernel.withHost(host=>run({...host,request:(input,requestOptions={})=>accounting(host,input,requestOptions)}),options);
 }
}
