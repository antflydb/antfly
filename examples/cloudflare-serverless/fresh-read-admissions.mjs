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

// A cohort closes BEFORE observation starts. No frontier or failed promise is
// retained for callers arriving after that boundary.
export class FreshReadAdmissions {
 constructor({delayMs=2,maxGroups=64}={}){if(!Number.isInteger(delayMs)||delayMs<0||delayMs>10)throw Error('InvalidReadAdmissionDelay');this.delayMs=delayMs;this.maxGroups=maxGroups;this.pending=new Map();}
 run(key,read,traceId){
  let group=this.pending.get(key);
  if(!group){
   if(this.pending.size>=this.maxGroups)return Promise.reject(Error('CoordinatorCommitBusy'));
   group={traceIds:new Set()};this.pending.set(key,group);
   group.promise=new Promise((resolve,reject)=>setTimeout(()=>{this.pending.delete(key);Promise.resolve().then(()=>read([...group.traceIds])).then(resolve,reject);},this.delayMs));
  }
  if(traceId)group.traceIds.add(traceId);
  return group.promise.then(view=>structuredClone(view));
 }
}
