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

// Explicit, read-only qualification probes. Hints never stand in for observed
// geography. RPC probes instantiate their compute identities; do not reuse them
// as a physical-cold population.
import{callR2Binding}from'../../zig/lib/objectstore/wasm/r2.mjs';
import{runtimeName}from'./runtime-routing.mjs';
export async function placementStorageProbe(env,role,jurisdiction=null,{clock=()=>performance.now()}={}){
 if(env.QUALIFICATION_PLACEMENT_PROBE!=='true')throw Error('PlacementProbeDisabled');
 if(env.COORDINATOR_BACKEND!=='r2'||!env.DATABASE||!env.ANTFLY_INCARNATION)throw Error('PlacementProbeUnavailable');
 const hash=new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(env.ANTFLY_INCARNATION))),id=Array.from(hash,b=>b.toString(16).padStart(2,'0')).join(''),start=clock();
 const result=await callR2Binding(()=>env.DATABASE.head('antfly/authority-v1/'+id+'/CURRENT'));
 return{role,storage_head_ms:clock()-start,authority_present:result!=null,worker_version:env.WORKER_VERSION?.id??null,jurisdiction,actual_actor_region:null};
}
export async function placementProbe(request,env,{clock=()=>performance.now()}={}){
 if(env.QUALIFICATION_PLACEMENT_PROBE!=='true')throw Error('PlacementProbeDisabled');
 const samples=[{...await placementStorageProbe(env,'app',null,{clock}),rpc_ms:null}];
 for(const[role,binding,name]of [['writer',env.COMMITS,env.ANTFLY_INCARNATION],['reader',env.READERS,runtimeName(env.ANTFLY_INCARNATION,0)],['builder',env.BUILDERS,runtimeName(env.ANTFLY_INCARNATION,0)]]){
  if(!binding)throw Error('PlacementProbeUnavailable');const start=clock(),result=await binding.getByName(name).placementProbe();samples.push({...result,rpc_ms:clock()-start});
 }
 return{qualification:false,consumes_compute_identities:true,app_colo:typeof request.cf?.colo==='string'?request.cf.colo:null,runtime_generation:env.RUNTIME_GENERATION??null,bucket_location_hint:env.R2_LOCATION_HINT??null,compute_location_hint:env.DO_LOCATION_HINT??env.R2_LOCATION_HINT??null,actual_bucket_region:null,samples};
}
