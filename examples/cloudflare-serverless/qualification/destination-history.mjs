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

import {readDestinationStatus,evaluateDestinationHealth} from './destination-health.mjs';
// Provider jobStatus.last_error can be cleared by a later successful delivery.
// Retain every observed status; polling does not prove absence between samples.
export function evaluateDestinationHistory(samples,scope){
 const failures=[],valid=Array.isArray(samples)&&samples.length>=2&&samples.length<=256&&Number.isSafeInteger(scope?.from)&&Number.isSafeInteger(scope?.to)&&scope.from<=scope.to;
 if(!valid)return {qualification:false,complete:false,history_checked:true,failures:['ExportDeliveryHistoryInvalid']};
 let previous=null;
 for(const s of samples){
  if(!Number.isSafeInteger(s?.requested_at)||!Number.isSafeInteger(s?.at)||s.at<s.requested_at||s.at-s.requested_at>75000||previous&&(s.requested_at<previous.at||s.requested_at-previous.requested_at>75000)){failures.push('ExportDeliveryHistoryGap');continue;}
  previous=s;
  if(!s.status){failures.push('ExportDeliveryStatusUnavailable');continue;}
  const checked=evaluateDestinationHealth(s.status,{...scope,to:Math.max(scope.from,Math.min(scope.to,s.status.at))});
  failures.push(...checked.failures.filter(f=>f!=='ExportDeliveryNotSettled'));
 }
 if(samples[0].requested_at>scope.from+5000||samples[0].at<scope.from||samples.at(-1).at<scope.to)failures.push('ExportDeliveryHistoryGap');
 const final=evaluateDestinationHealth(samples.at(-1).status,scope);failures.push(...final.failures);
 return {schema:2,qualification:false,complete:failures.length===0,history_checked:true,failures:[...new Set(failures)],from:scope.from,to:scope.to,observations:samples.length,observed_at:samples.at(-1).at,status:final.status??null,limitations:['Observed provider delivery errors are permanent; polling cannot rule out errors cleared entirely between samples.']};
}

// Node driver only. Polls execute serially, retain failures in the report and
// collect one final status after the workload; no provider request is detached.
export async function startDestinationMonitor(path,{samples,persist,readStatus=readDestinationStatus,intervalMs=30000}={}){
 if(!Array.isArray(samples)||samples.length||typeof persist!=='function')throw Error('ExportDeliveryMonitorInvalid');
 let stopped=false,wake=null,failure=null;
 async function poll(){
  if(samples.length>=256)throw Error('ExportDeliveryHistoryBudgetExceeded');
  const row={requested_at:Date.now()};try{row.status=await readStatus(path);}catch{row.error='ExportDeliveryStatusUnavailable';}row.at=Date.now();samples.push(row);await persist();
 }
 await poll();
 const running=(async()=>{while(!stopped){await new Promise(ok=>{const timer=setTimeout(()=>{wake=null;ok();},Math.max(0,intervalMs-(Date.now()-samples.at(-1).requested_at)));wake=()=>{clearTimeout(timer);wake=null;ok();};});if(!stopped)await poll();}})().catch(error=>{failure=error;});
 return{async stop(){stopped=true;wake?.();await running;if(failure)throw failure;await poll();}};
}
