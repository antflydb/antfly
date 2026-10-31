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

export const managedMaintenance=new Set(["collect","collect_metadata","compact","reindex"]);
export class MaintenanceTickets {
 constructor(store,clock=Date.now){this.store=store;this.clock=clock;}
 key(kind){if(!managedMaintenance.has(kind))throw new Error("InvalidMaintenanceKind");return "__antfly_maintenance/jobs/"+kind;}
 request(kind,jobId){const key=this.key(kind);return this.store.transaction(async tx=>{
  const prior=await tx.get(key);if(prior?.id&&prior.expires>this.clock())return {enqueue:false};
  const state={id:crypto.randomUUID(),step:0,job_id:jobId??null,expires:this.clock()+120000};tx.put(key,state);return {...state,enqueue:true};
 });}
 valid(kind,ticket,jobId){const key=this.key(kind);return this.store.transaction(async tx=>{const state=await tx.get(key);if(state?.id!==ticket?.id||state.step!==ticket.step||state.job_id!==(jobId??null))return false;if(state.expires-this.clock()<60000){state.expires=this.clock()+120000;tx.put(key,state);}return true;});}
 advance(kind,ticket,jobId){const key=this.key(kind);return this.store.transaction(async tx=>{
  const state=await tx.get(key);if(state?.id!==ticket?.id||state.step!==ticket.step)return {enqueue:false};
  if(!Number.isSafeInteger(state.step+1))throw new Error("MaintenanceStepExhausted");
  state.step++;state.job_id=jobId??null;state.expires=this.clock()+120000;tx.put(key,state);return {...state,enqueue:true};
 });}
 finish(kind,ticket){const key=this.key(kind);return this.store.transaction(async tx=>{const state=await tx.get(key);if(state?.id===ticket?.id&&state.step===ticket.step)tx.put(key,{id:null,expires:0,step:state.step});});}
 abandon(kind,ticket){return this.finish(kind,ticket);}
}
