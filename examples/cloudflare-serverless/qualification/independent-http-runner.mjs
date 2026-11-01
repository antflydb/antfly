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

import{performance}from'node:perf_hooks';import{attemptIdentity,rayIdentity}from'./invocation-accounting.mjs';import{runPlan,validatePlan}from'./http-trial-driver.mjs';
const uuid=()=>crypto.randomUUID(),lsn=s=>BigInt('0x'+s.slice(-16));
// Request offsets are fixed before the first I/O. Slow responses do not move
// future arrivals; every dropped/late/failed attempt remains in the population.
export function offeredSchedule({reads=256,rps=32,writeEvery=16,documents=64,sequence=0}={}){
 if(!Number.isInteger(reads)||reads<1||reads>4096||!Number.isFinite(rps)||rps<=0||rps>500||!Number.isInteger(writeEvery)||writeEvery<0||writeEvery>4096||![64,128,512].includes(documents)||!Number.isInteger(sequence)||sequence<0||sequence+reads>1000000)throw Error('IndependentScheduleInvalid');
 const tasks=[];for(let i=0;i<reads;i++){const tenant=['a','b','c','d'][i%4],offset_ms=i*1000/rps;tasks.push({attempt_id:uuid(),offset_ms,step:{op:'lookup',tenant,id:1+(Math.floor(i/4)*37)%(documents-1)}});if(writeEvery&&(i+1)%writeEvery===0)tasks.push({attempt_id:uuid(),offset_ms,step:{op:'mutate',tenant:['a','b','c','d'][Math.floor(i/writeEvery)%4],sequence:sequence+i}});}
 return tasks;
}
export async function runIndependent({plan,tasks,receipts,transport,maxInFlight=64,maxDispatchLagMs=25,intent=async()=>{},terminal=async()=>{},startAtMono,startAtWall,clock=()=>performance.now(),wall=()=>Date.now(),sleep=ms=>new Promise(r=>setTimeout(r,ms))}){
 validatePlan(plan);if(!Array.isArray(tasks)||!tasks.length||tasks.length>8192||!Number.isInteger(maxInFlight)||maxInFlight<1||maxInFlight>128||!Number.isFinite(maxDispatchLagMs)||maxDispatchLagMs<0||maxDispatchLagMs>1000||tasks.some((t,i)=>!Number.isFinite(t.offset_ms)||t.offset_ms<0||t.offset_ms>3600000||i&&t.offset_ms<tasks[i-1].offset_ms)||new Set(tasks.map(t=>t.attempt_id)).size!==tasks.length)throw Error('IndependentScheduleInvalid');
 const fences={...receipts},samples=[],pending=new Set(),ledger=[],evidence_errors=[];for(const t of tasks){validatePlan({...plan,rounds:1,steps:[{...t.step,attempt_id:t.attempt_id,...(['lookup','mutate'].includes(t.step.op)?{visibility:fences[t.step.tenant]}:{})}]});await intent(t);}const began=startAtMono??clock(),started_at=startAtWall??wall();let peak=0;
 const record=async sample=>{samples.push(sample);try{await terminal(sample);}catch{evidence_errors.push('IndependentTerminalWriteFailed');}};
 const launch=task=>{let promise;promise=(async()=>{
  const dispatch=clock(),lag=Math.max(0,dispatch-began-task.offset_ms),step={...task.step,attempt_id:task.attempt_id,...(['lookup','mutate'].includes(task.step.op)?{visibility:fences[task.step.tenant]}:{})},row={id:task.attempt_id,phase:plan.phase??'read',op:step.op,tenant:step.tenant,started_at:wall(),response_at:null,finished_at:null,status:null,ray_id_sha256:null,version_id:null,transport_error:false};ledger.push(row);
  const response=await runPlan({...plan,rounds:1,steps:[step]},{...transport,onResponse:(id,response)=>{row.response_at=wall();row.status=response.status;row.ray_id_sha256=rayIdentity(response.headers.get('CF-Ray'));row.version_id=response.headers.get('X-Antfly-Version');}});const s=response.samples[0];row.finished_at=wall();row.transport_error=s.status==null||s.outcome==='error'&&/^(TrialTransport|Timeout|Abort)/.test(s.error??'');
  if(s.op==='mutate'&&s.outcome==='ok'&&(!fences[s.tenant]||lsn(s.visibility)>lsn(fences[s.tenant])))fences[s.tenant]=s.visibility;
  await record({...s,scheduled_at:started_at+task.offset_ms,dispatch_lag_ms:lag,service_ms:s.ms,ms:Math.max(0,clock()-began-task.offset_ms)});
 })().catch(async()=>{await record({attempt_id:task.attempt_id,tenant:task.step.tenant,op:task.step.op,outcome:'error',error:'IndependentDispatchFailed',ms:Math.max(0,clock()-began-task.offset_ms)});}).finally(()=>pending.delete(promise));pending.add(promise);peak=Math.max(peak,pending.size);};
 for(const task of tasks){while(clock()-began<task.offset_ms)await sleep(Math.max(1,task.offset_ms-(clock()-began)));if(pending.size>=maxInFlight){await record({attempt_id:task.attempt_id,tenant:task.step.tenant,op:task.step.op,outcome:'error',error:'IndependentClientOverloaded',dispatch_lag_ms:Math.max(0,clock()-began-task.offset_ms),ms:Math.max(0,clock()-began-task.offset_ms)});}else launch(task);}
 await Promise.allSettled([...pending]);const metrics=transport.report(),completed=samples.length===tasks.length,failures=[];if(!completed)failures.push('IndependentPopulationIncomplete');if(samples.some(s=>s.outcome!=='ok'))failures.push('IndependentRequestFailed');if(samples.some(s=>s.dispatch_lag_ms>maxDispatchLagMs))failures.push('IndependentSchedulingLate');if(metrics.peak_queued)failures.push('IndependentTransportQueued');if(evidence_errors.length)failures.push(...evidence_errors);
 return{schema:1,qualification:false,kind:'independent-node-offered-http',passed:failures.length===0,failures:[...new Set(failures)],started_at,finished_at:wall(),max_dispatch_lag_ms:maxDispatchLagMs,offered:tasks.length,completed,peak_in_flight:peak,transport:metrics,receipts:fences,samples,invocation_ledger:{schema:1,overflow:false,attempts:ledger},unsent_attempt_ids:tasks.filter(t=>!ledger.some(l=>l.id===t.attempt_id)).map(t=>attemptIdentity(t.attempt_id))};
}
