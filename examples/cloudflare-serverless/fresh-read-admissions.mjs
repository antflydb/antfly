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

// Cohorts close before native observation. Queued callers may share a pending
// admission only when its owner closes it before the first freshness-bearing
// I/O. Separate limits preserve mutation-metadata admission under read load.
const waiters={read:0,commit:0},limits={read:64,commit:16};
export class FreshReadAdmissions {
 constructor({delayMs=2,maxGroups=64,maxMembers=64}={}){if(!Number.isInteger(delayMs)||delayMs<0||delayMs>10)throw Error("InvalidReadAdmissionDelay");if(!Number.isInteger(maxMembers)||maxMembers<1||maxMembers>64)throw Error("InvalidReadAdmissionLimit");this.delayMs=delayMs;this.maxGroups=maxGroups;this.maxMembers=maxMembers;this.pending=new Map();}
 run(key,read,traceId,{untilAdmission=false,admissionClass="read",onPrepared}={}){
  if(!Object.hasOwn(limits,admissionClass)||waiters[admissionClass]>=limits[admissionClass])return Promise.reject(Error("CoordinatorCommitBusy"));
  let group=this.pending.get(key);
  if(group&&group.members>=this.maxMembers||!group&&this.pending.size>=this.maxGroups)return Promise.reject(Error("CoordinatorCommitBusy"));
  if(!group){
   group={traceIds:new Set(),ids:[],members:0,callbacks:[]};this.pending.set(key,group);
   const close=()=>{if(this.pending.get(key)===group)this.pending.delete(key);};
   group.promise=new Promise((resolve,reject)=>setTimeout(()=>{
    if(!untilAdmission)close();
    Promise.resolve().then(()=>read(group.ids,close,view=>{for(const publish of group.callbacks)publish(view);})).then(view=>{close();resolve(view);},error=>{close();reject(error);});
   },this.delayMs));
  }
  // Register the continuation in this submitting request, never in the
  // cohort owner's context. Only promise resolution crosses that boundary.
  const prepared={error:null};let publish,notified=false;
  const early=onPrepared?new Promise(resolve=>publish=resolve).then(view=>{try{onPrepared(view);}catch(error){prepared.error=error;}}):null;
  if(onPrepared)group.callbacks.push(view=>{notified=true;publish(structuredClone(view));});
  group.members++;waiters[admissionClass]++;
  if(traceId&&!group.traceIds.has(traceId)){group.traceIds.add(traceId);group.ids.push(traceId);}
  return group.promise.then(async view=>{if(early&&notified)await early;if(prepared.error)throw prepared.error;return structuredClone(view);}).finally(()=>{waiters[admissionClass]--;});
 }
}
