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

// Coalesced recovery hints never become a condition of a foreground ACK.
// The caller decides whether checkpoint work may overlap foreground work.
export function replayCheckpointDelay({commits=0,bytes=0,waves=0,ageMs=0}={}){
 if(![commits,bytes,waves,ageMs].every(v=>Number.isFinite(v)&&v>=0))throw Error('InvalidCheckpointReplayCost');
 // A maximum age bounds low traffic; replay work advances the repair earlier.
 // This is scheduling only. Native proof/CAS/owner checks remain mandatory.
 return commits>=8||bytes>=64*1024||waves>=4?0:Math.max(0,1000-ageMs);
}
export class IdleCheckpoint {
 constructor({revision,isIdle,checkpoint,background,replayCost,onError=()=>{},every=1,delay=250,retryDelay=1100,maxAttempts=4,timeoutMs=10000,clock=Date.now,monotonic=()=>performance.now(),sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms))}){
  if(![revision,isIdle,checkpoint,background,onError,clock,monotonic,sleep].every(v=>typeof v==='function')||!Number.isInteger(maxAttempts)||maxAttempts<1||maxAttempts>8||!Number.isFinite(timeoutMs)||timeoutMs<1||timeoutMs>10000||!Number.isInteger(every)||every<1||![delay,retryDelay].every(v=>Number.isFinite(v)&&v>=0))throw Error('InvalidIdleCheckpoint');
  Object.assign(this,{revision,isIdle,checkpoint,background,replayCost,onError,every,delay,retryDelay,maxAttempts,timeoutMs,clock,monotonic,sleep});this.last=0;this.lastCheckpointAt=this.monotonic();this.pending=false;this.attempts=0;this.deferrals=0;this.failures=0;
 }
 schedule(){
  if(this.pending||this.revision()-this.last<this.every)return;
  this.pending=true;
  let advanced=false;const wall=this.clock(),mono=this.monotonic(),remaining=()=>Math.max(0,Math.min(this.timeoutMs-(this.clock()-wall),this.timeoutMs-(this.monotonic()-mono)));
  const work=(async()=>{
   for(let attempt=0;attempt<this.maxAttempts;attempt++){
    const available=remaining();if(available<=0)return;
    const initialDelay=this.replayCost?replayCheckpointDelay({...this.replayCost(),commits:Math.max(0,this.revision()-this.last),ageMs:Math.max(0,this.monotonic()-this.lastCheckpointAt)}):this.delay;
    await this.sleep(Math.min(attempt?this.retryDelay:initialDelay,available));
    if(remaining()<=0)return;
    if(!this.isIdle()){this.deferrals++;continue;}
    this.attempts++;let result;
    try{result=await this.checkpoint({deadlineMs:this.clock()+remaining()});}
    catch(error){this.failures++;try{this.onError(error);}catch{}if(/^(?:ImmutableLogFenced|CoordinatorMigrationRequired|InvalidImmutableLog|InvalidImmutableLogProof)$/.test(error?.message??''))return;continue;}
    if(remaining()<=0)return;
    if(result.done){this.last=result.revision;this.lastCheckpointAt=this.monotonic();advanced=true;return;}
    this.deferrals++;
   }
  })().catch(error=>{this.failures++;try{this.onError(error);}catch{}}).finally(()=>{this.pending=false;if(advanced&&this.revision()>this.last)this.schedule();});
  this.background(work);
 }
}
