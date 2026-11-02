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

import{baselineOps}from'./platform-baseline-common.mjs';
import{percentile}from'./gate-policy.mjs';
import{resourceFailureCode}from'../performance.mjs';
const stats=values=>({n:values.length,p50:percentile(values,.5),p95:percentile(values,.95),p99:percentile(values,.99),min:values.length?Math.min(...values):null});
export async function runPlatformBaseline({origin,token,fetch:send=fetch,samples=24,rates=[16,64],ops=baselineOps,maxInFlight=16,maxDispatchLagMs=25,onIntent=async()=>{},onPrepared=async()=>{},onSample=async()=>{},onCase=async()=>{},freshOwner=false,version,privateVersion,signal,clock=()=>performance.now(),sleep=ms=>new Promise(r=>setTimeout(r,ms))}={}){
 if(!Number.isInteger(samples)||samples<8||samples>256||!Array.isArray(rates)||rates.length>4||rates.some(r=>![8,16,32,64,128].includes(r))||!ops?.length||ops.some(op=>!baselineOps.includes(op))||new Set(ops).size!==ops.length||!Number.isInteger(maxInFlight)||maxInFlight<1||maxInFlight>16||freshOwner&&(ops.length!==1||ops[0]!=='owner_echo'))throw Error('BaselinePlanInvalid');
 const report={schema:1,qualification:false,full_gate_passed:false,kind:'platform primitives; empirical baseline, not an inherent floor',complete:false,passed:false,cases:[],verifications:[],started_at:Date.now()},all=[];let expectedVersion=version,expectedPrivateVersion=privateVersion,expectedSettings,expectedInstance;
 const attempt=async(task,started,rate)=>{
  const dispatch=clock(),row={id:task.id,op:task.op,dispatch_lag_ms:rate?Math.max(0,dispatch-started-task.offset_ms):0,outcome:'error'};
  try{signal?.throwIfAborted();const response=await send(new URL('/probe',origin),{method:'POST',redirect:'manual',headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},body:JSON.stringify({id:task.id,op:task.op,padding:'x'.repeat(1024),fresh:freshOwner}),signal:signal?AbortSignal.any([signal,AbortSignal.timeout(30000)]):AbortSignal.timeout(30000)});row.status=response.status;row.headers_ms=clock()-dispatch;if(response.status>=300&&response.status<400){await response.body?.cancel();throw Error('BaselineRedirectRejected');}const value=await response.json();row.service_ms=clock()-dispatch;row.ms=rate?Math.max(0,clock()-started-task.offset_ms):row.service_ms;
   if(!response.ok)throw Error(resourceFailureCode(value.error)??'BaselineHTTP'+response.status);
   if(value.schema!==1||value.id!==task.id||value.op!==task.op||value.payload!=='x'.repeat(1024)||!value.version||!Number.isFinite(value.http_ms)||value.http_ms<0||value.http_ms>row.service_ms+5)throw Error('BaselineResponseMismatch');
   expectedVersion??=value.version;if(value.version!==expectedVersion)throw Error('BaselineVersionChanged');if(task.op==='service_echo'||task.op.startsWith('owner_')){if(!value.private_version)throw Error('BaselineVersionMissing');expectedPrivateVersion??=value.private_version;if(value.private_version!==expectedPrivateVersion)throw Error('BaselineVersionChanged');}
   const settings=JSON.stringify(value.settings);expectedSettings??=settings;if(settings!==expectedSettings)throw Error('BaselineSettingsChanged');
   if(task.op.startsWith('owner_')){if(!value.instance)throw Error('BaselineInstanceMissing');if(!freshOwner){expectedInstance??=value.instance;if(value.instance!==expectedInstance)throw Error('BaselineOwnerReconstructed');}else if(all.some(s=>s.instance===value.instance))throw Error('BaselineFreshOwnerReused');row.instance=value.instance;}
   // Same-request durations only. This residual includes ingress/egress,
   // response construction, scheduling and body transit; it is NOT pure network.
   row.outside_http_ms=Math.max(0,row.service_ms-value.http_ms);row.http_ms=value.http_ms;row.owner_ms=value.owner_ms??null;row.http_stages=value.http_stages;row.owner_stages=value.stages;row.memory=value.memory??null;row.app_colo=value.app_colo;row.actual_owner_region=null;row.outcome='ok';
  }catch(error){row.error=resourceFailureCode(error)??(/^Baseline[A-Za-z0-9]+$/.test(error.message)?error.message:'BaselineTransportFailed');row.ms??=Math.max(0,clock()-started-(rate?task.offset_ms:0));}
  all.push(row);try{await onSample(row);}catch{row.journal_error=true;}return row;
 };
 try{for(const op of ops)for(const rate of[0,...rates]){
  signal?.throwIfAborted();const tasks=Array.from({length:samples},(_,i)=>({id:crypto.randomUUID(),op,offset_ms:rate?i*1000/rate:0}));for(const task of tasks)await onIntent({event:'intent',rate,...task});await onPrepared({op,rate});const started=clock(),pending=new Set(),rows=[];
  let interruption;for(const task of tasks){try{if(!rate){rows.push(await attempt(task,started,rate));continue;}while(clock()-started<task.offset_ms){signal?.throwIfAborted();await sleep(Math.max(1,task.offset_ms-(clock()-started)));}if(pending.size>=maxInFlight){const row={id:task.id,op,outcome:'error',error:'BaselineClientOverloaded',ms:Math.max(0,clock()-started-task.offset_ms)};rows.push(row);all.push(row);await onSample(row);continue;}let promise;promise=attempt(task,started,rate).then(row=>rows.push(row)).finally(()=>pending.delete(promise));pending.add(promise);}catch(error){interruption=error;break;}}
  await Promise.allSettled([...pending]);const good=rows.filter(r=>r.outcome==='ok'),failures=[];if(interruption||rows.length!==samples||good.length!==samples)failures.push('BaselineRequestsFailed');if(rows.some(r=>r.dispatch_lag_ms>maxDispatchLagMs))failures.push('BaselineDispatchLate');if(rows.some(r=>r.journal_error))failures.push('BaselineJournalFailed');if(['owner_put','r2_put','owner_r2_put'].includes(op)){for(let at=0;at<good.length;at+=32){const ids=good.slice(at,at+32).map(r=>r.id),row={id:crypto.randomUUID(),phase:'verify',op,ids,outcome:'error'};try{await onIntent({event:'intent',...row});const response=await send(new URL('/verify',origin),{method:'POST',redirect:'manual',headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},body:JSON.stringify({op,ids}),signal:signal?AbortSignal.any([signal,AbortSignal.timeout(30000)]):AbortSignal.timeout(30000)});if(response.status>=300&&response.status<400){await response.body?.cancel();throw Error('BaselineRedirectRejected');}const value=await response.json();if(!response.ok||value.verified!==true||value.count!==ids.length)throw Error(resourceFailureCode(value.error)??'BaselineVerificationFailed');row.outcome='ok';}catch(error){row.error=resourceFailureCode(error)??'BaselineVerificationFailed';failures.push('BaselineVerificationFailed');}report.verifications.push(row);try{await onSample(row);}catch{failures.push('BaselineJournalFailed');}}}
 report.cases.push({op,rate,arrival:rate?'fixed offered arrivals':'unloaded serial',fresh_owner:freshOwner,passed:!failures.length,failures,samples:rows,latency:stats(rows.map(r=>r.ms)),successful_latency:stats(good.map(r=>r.ms)),http:stats(good.map(r=>r.http_ms)),outside_http:stats(good.map(r=>r.outside_http_ms))});await onCase(report.cases.at(-1));
 }}catch(error){report.failure=resourceFailureCode(error)??'BaselineExecutionInterrupted';}finally{report.finished_at=Date.now();report.complete=report.cases.length===ops.length*(rates.length+1);report.passed=!report.failure&&report.complete&&report.cases.every(c=>c.passed);report.version=expectedVersion;report.private_version=expectedPrivateVersion;report.settings=expectedSettings?JSON.parse(expectedSettings):null;}
 return report;
}
