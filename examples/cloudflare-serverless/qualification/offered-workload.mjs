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

// Shared fixed-arrival driver for qualification and capacity exploration.
// A bounded client never silently sheds load: saturation is recorded as failure.
export async function runOfferedWorkload({sides,seconds,period=30000,documents=512,maxPendingRounds=4,orderOffset=0,onProgress=async()=>{}}){
 if(!Array.isArray(sides)||!sides.length||!Number.isInteger(seconds)||seconds<1||!Number.isInteger(period)||period<1||!Number.isInteger(documents)||documents<1||!Number.isInteger(maxPendingRounds)||maxPendingRounds<1)throw Error('invalid offered workload');
 const start=Date.now(),end=start+seconds*1000,rounds=new Set(),monitors=new Set();let index=0,minute=-1;
 let offeredEnd,progressTask,nextProgress,progressError;
 const cancelled=()=>{if(progressError)throw progressError;for(const side of sides)side.abortController?.signal.throwIfAborted();};
 const checkpoint=state=>{
  if(progressTask){nextProgress=state;return;}
  const work=Promise.resolve().then(()=>onProgress(state)).catch(error=>{progressError=error;for(const side of sides)side.stop?.(error);}).finally(()=>{progressTask=null;if(nextProgress&&!progressError){const next=nextProgress;nextProgress=null;checkpoint(next);}});progressTask=work;
 };
 try{while(Date.now()<end){
  cancelled();
  const scheduled=start+index*period;
  if(Date.now()<scheduled){await new Promise(r=>setTimeout(r,Math.min(1000,scheduled-Date.now())));continue;}
  const order=(index+orderOffset)%2?[...sides].reverse():sides,round=index++;
  if(rounds.size>=maxPendingRounds){for(const side of order)side.errors.push({phase:'client-admission',round,error:'fixed-arrival workload exceeded bounded pending rounds'});}
  else{
   const work=Promise.allSettled(order.map(side=>side.round(round,documents,scheduled))).then(results=>{results.forEach((result,i)=>{if(result.status==='rejected')order[i].errors.push({phase:'offered-round',round,error:String(result.reason)});});}).finally(()=>rounds.delete(work));rounds.add(work);
  }
  const nextMinute=Math.floor((Date.now()-start)/60000);
  if(nextMinute!==minute){
   minute=nextMinute;const monitor=Promise.allSettled(order.map(side=>side.progress(minute))).then(results=>{results.forEach((result,i)=>{if(result.status==='rejected')order[i].errors.push({phase:'monitor',minute,error:String(result.reason)});});}).finally(()=>monitors.delete(monitor));monitors.add(monitor);
   checkpoint({minute,pending_rounds:rounds.size,offered_rounds:index,started_at:start});
  }
 }
 }finally{offeredEnd=Date.now();await Promise.allSettled([...rounds,...monitors]);while(progressTask)await progressTask;}
 cancelled();
 return{started_at:start,offered_end_at:offeredEnd,finished_at:Date.now(),period_ms:period,rounds:index,requested_ms:seconds*1000};
}
