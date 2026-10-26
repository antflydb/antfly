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

// This view belongs to one admitted request. Preparation may overlap queued
// native work, but neither its admission time nor its deadline is reset.
export async function prepareReadAdmission(env,input,trace,readViews){
 if(env.COORDINATOR_BACKEND!=="r2"||input.cursor||!["lookup","list","count","search","status","latest"].includes(input.op))return;
 const physical={rpc_calls:0},started=performance.now();let admission;
 const options={trace_id:trace.id,...(input.exact_state?{version:input.exact_state.generation}:{})};
 try{
  let view;
  if(readViews&&env.READ_ONLY_SNAPSHOTS!=="false"){
   try{view=await trace.span("native:openReadView",()=>readViews.open(options));physical.native_metadata_views=1;}
   catch(error){if(!/^(ReadSnapshotNeeds(?:Admission|Pin)|CoordinatorAuthorityMissing)$/.test(error?.message))throw error;}
  }
  if(!view){physical.rpc_calls++;view=await trace.span("rpc:openReadView",()=>env.COMMITS.getByName(env.ANTFLY_INCARNATION).openReadView(options));}
  physical.read_admission_immutable_hints=view.immutable?.length??0;
  admission={view,started,physical};
 }catch(error){if(!unsupportedRpc(error,"openReadView"))throw error;admission={legacy:true,started,physical};}
 physical.read_admission_ms=performance.now()-started;
 if(physical.rpc_calls)physical.rpc_wait_ms=physical.read_admission_ms;
 return admission;
}
