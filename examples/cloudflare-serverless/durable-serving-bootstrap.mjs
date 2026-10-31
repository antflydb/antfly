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

import{conditionalReadLeasePolicy}from'../../zig/lib/objectstore/wasm/read-view-lease.mjs';
// The provider's actor-local durability is an explicit part of protocol v3.
// Opaque capsules are native-owned; no application rows or SQL schema live here.
export class DurableServingBootstrap {
 constructor(storage,env,owner,{onError=()=>{}}={}){this.onError=onError;if(!['get','put','sync'].every(k=>typeof storage?.[k]==='function'))throw Error('DurableBootstrapStorageRequired');Object.assign(this,{storage,env,owner});this.key='__antfly_native_serving_v1';this.loaded=false;}
 validate(authority){const d=authority?.descriptor;if(d?.serving?.owner_id!==this.owner)throw Error("ResidentServingOwnerMismatch");if(d?.format!==2||d.namespace!==this.env.ANTFLY_INCARNATION||d.mode!=='r2-bounded'||d.storage_id!==(this.env.COORDINATOR_STORAGE_ID??d.namespace)||d.serving?.protocol!=='serialized-owner-v3'||d.serving.owner_id!==this.owner||d.serving.authority_storage!=='provider-native-v1'||Object.keys(conditionalReadLeasePolicy).some(k=>d.read_lease_policy?.[k]!==conditionalReadLeasePolicy[k])||Object.keys(d.read_lease_policy??{}).length!==Object.keys(conditionalReadLeasePolicy).length)throw Error('DurableBootstrapAuthorityMismatch');return authority;}
 async storageCall(phase,...args){const method=phase.split("_")[0],started=performance.now();try{return await this.storage[method](...args);}catch(error){try{this.onError({event:"antfly.durable_storage_error",namespace:this.env.ANTFLY_INCARNATION,phase,elapsed_ms:performance.now()-started,error_message:String(error.message??error).slice(0,2048)});}catch{}throw error;}}
 async authority(){
  const entry=await this.storageCall("get",this.key);if(entry==null)return null;
  if(entry.format!==1||JSON.stringify(entry).length>98304||typeof entry.dirty!=='boolean'||entry.capsule!=null&&(typeof entry.capsule!=='string'||entry.capsule.length>65536))throw Error('InvalidDurableBootstrap');
  if(entry.blocked)throw Error('CoordinatorMigrationRequired');this.entry=entry;this.dirtyConfirmed=entry.dirty;return this.validate(entry.authority);
 }
 async adopt(authority){this.validate(authority);if(this.entry)return;await this.persist({format:1,authority,dirty:true});}
 async persist(entry){this.dirtyConfirmed=false;await this.storageCall(entry.dirty?"put_intent":"put_clean",this.key,entry);await this.storageCall(entry.dirty?"sync_intent":"sync_clean");this.entry=entry;this.dirtyConfirmed=entry.dirty;}
 async restore(codec){if(this.loaded)return null;this.loaded=true;if(!this.entry||this.entry.dirty||!this.entry.capsule)return null;const capsule=this.entry.capsule;let result;try{result=await codec('serving_import',{serving_capsule:capsule});}catch(error){if(!['InvalidServingCapsule','SyntaxError','UnexpectedToken','MissingField','InvalidCoordinatorRoot'].includes(error.message))throw error;await this.begin();return null;}this.entry={...this.entry,capsule:undefined};return result;}
 async begin(){this.lastActivity=performance.now();if(this.entry?.blocked)throw Error('CoordinatorMigrationRequired');if(!this.entry)throw Error('DurableBootstrapAuthorityMissing');if(this.dirtyConfirmed)return;await this.persist({format:1,authority:this.entry.authority,dirty:true});}
 async finish(state,codec){const result=await codec('serving_export',{state,owner:state.owner});if(typeof result.serving_capsule!=='string'||result.serving_capsule.length>65536)throw Error('InvalidDurableBootstrap');await this.persist({format:1,authority:this.entry.authority,dirty:false,capsule:result.serving_capsule});this.entry={...this.entry,capsule:undefined};}
 async freeze(){if(!this.entry)throw Error('DurableBootstrapAuthorityMissing');await this.persist({format:1,authority:this.entry.authority,dirty:true,blocked:true});}
}
