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

// Constructors perform no remote I/O under blockConcurrencyWhile. At most one
// authority load is in flight; timed-out callers leave no promise reactions or
// input references behind. Late success is validated by the supplied loader.
export class AuthorityAdmission {
 constructor(load,{timeoutMs=5000,maxWaiters=64}={}){
  if(typeof load!=="function"||!Number.isInteger(timeoutMs)||timeoutMs<1||timeoutMs>5000||!Number.isInteger(maxWaiters)||maxWaiters<1||maxWaiters>64)throw Error("InvalidAuthorityAdmission");
  this.load=load;this.timeoutMs=timeoutMs;this.maxWaiters=maxWaiters;this.waiters=new Set();this.loading=false;this.value=null;
 }
 get(){
  if(this.value)return Promise.resolve(this.value);
  if(this.waiters.size>=this.maxWaiters)return Promise.reject(Error("CoordinatorCommitBusy"));
  return new Promise((resolve,reject)=>{
   const waiter={resolve,reject};waiter.timer=setTimeout(()=>{this.waiters.delete(waiter);reject(Error("CoordinatorRecovering"));},this.timeoutMs);this.waiters.add(waiter);
   if(!this.loading){this.loading=true;Promise.resolve().then(()=>this.load()).then(value=>this.complete(true,value),error=>this.complete(false,error));}
  });
 }
 complete(ok,value){
  this.loading=false;if(ok)this.value=value;
  for(const waiter of this.waiters){clearTimeout(waiter.timer);if(ok)waiter.resolve(value);else waiter.reject(value);}this.waiters.clear();
 }
}
