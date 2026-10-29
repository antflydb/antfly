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

import{unsupportedRpc}from"./performance.mjs";
import{bindReadLease,readLeaseRemaining}from"../../zig/lib/objectstore/wasm/read-view-lease.mjs";

// Latest reads always authenticate fresh metadata. An explicit minimum fence
// may use a certified snapshot without resetting its original GC deadline.
export async function prepareReadAdmission(env,input,trace,readViews,{background,onPrepared,onAdmitted,deadlineMs}={}){
 if(env.COORDINATOR_BACKEND!=="r2"||input.cursor||!["lookup","list","count","search","status","latest"].includes(input.op))return;
 const physical={rpc_calls:0},started=performance.now(),wallStarted=Date.now();let admission;
 const options={trace_id:trace.id,required_visibility:input.required_visibility,allow_cached:env.FENCED_READ_LEASES!=="false"&&input.fresh!==true&&!input.exact_state&&['lookup','list','count','search'].includes(input.op)&&typeof input.required_visibility==='string',...(deadlineMs!==undefined?{deadlineMs}:{}),...(input.exact_state?{version:input.exact_state.generation}:{})};
 const onLease=sample=>Object.assign(physical,sample);
 const admitted=view=>{const result={view,started,physical,invalidate:()=>readViews?.invalidate?.()};if(bindReadLease(view,{started})){const remaining=readLeaseRemaining(view);result.deadlineMs=Math.min(deadlineMs??Infinity,wallStarted+30000,Date.now()+remaining);result.confirm=token=>{if(readLeaseRemaining(view)<=0)throw Error('ManifestReadLeaseExpired');readViews?.confirm?.(view,token);};}return result;};
 const rpc=async()=>{onAdmitted?.();physical.rpc_calls++;return trace.span('rpc:openReadView',()=>env.COMMITS.getByName(env.ANTFLY_INCARNATION).openReadView(options));};
 try{
  let view;
  if(readViews&&env.READ_ONLY_SNAPSHOTS!=="false"){
   try{view=await trace.span('native:openReadView',()=>readViews.open({...options,background,onAdmitted,onLease,onPrepared:view=>{physical.read_admission_ready_ms=performance.now()-started;onPrepared?.(admitted(view));}}));physical.native_metadata_views=1;}
   catch(error){if(!/^(ReadSnapshotNeeds(?:Admission|Pin)|CoordinatorAuthorityMissing|CoordinatorRecovering)$/.test(error?.message))throw error;physical.read_admission_fallback_reason=error.message;}
  }else physical.read_admission_fallback_reason=env.READ_ONLY_SNAPSHOTS==='false'?'ReadSnapshotsDisabled':'LocalReadObserverUnavailable';
  if(!view)view=readViews?.admit?await readViews.admit({...options,onLease},rpc):await rpc();
  physical.read_admission_immutable_hints=view.immutable?.length??0;admission=admitted(view);
 }catch(error){if(!unsupportedRpc(error,'openReadView'))throw error;physical.read_admission_fallback_reason='LegacyReadViewUnsupported';admission={legacy:true,started,physical};}
 physical.read_admission_ms=performance.now()-started;
 if(physical.rpc_calls)physical.rpc_wait_ms=physical.read_admission_ms;
 return admission;
}
