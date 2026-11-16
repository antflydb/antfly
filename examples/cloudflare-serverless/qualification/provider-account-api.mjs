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

import{readCounterToken}from'./independent-counters.mjs';
// Account authorization stays transient and local. No credential file fallback.
// A separate token keeps observability privileges out of deployment authentication.
const accountAuthorization=()=>process.env.ANTFLY_OBSERVABILITY_API_TOKEN??readCounterToken();
export function providerAccountApi(account,{authenticate=accountAuthorization,fetch:request=fetch}={}){
 if(!/^[a-f0-9]{32}$/.test(account??''))throw Error('ProviderAccountInvalid');let credential;
 return async(route,method='GET',body)=>{
  if(!/^workers\/observability\/destinations(?:\/[a-zA-Z0-9_-]+)?$/.test(route)||!['GET','POST','DELETE'].includes(method))throw Error('ProviderRouteInvalid');
  if(!credential){try{credential=await authenticate();}catch{throw Error('ProviderCredentialLookupFailed');}if(typeof credential!=='string'||credential.length<20||credential.length>4096||/\s/.test(credential))throw Error('ProviderCredentialInvalid');}
  const response=await request('https://api.cloudflare.com/client/v4/accounts/'+account+'/'+route,{method,headers:{Authorization:'Bearer '+credential,'Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{}),signal:AbortSignal.timeout(30000)});let bytes=0;const parts=[];try{for await(const part of response.body){bytes+=part.length;if(bytes>1024*1024)throw Error('ProviderResponseExceeded');parts.push(part);}}catch{throw Error('ProviderResponseInvalid');}let value;try{value=JSON.parse(Buffer.concat(parts));}catch{throw Error('ProviderResponseInvalid');}if(!response.ok||value.success!==true)throw Error('ObserverApiStatus'+response.status);return value.result;
 };
}


import{spawn}from'node:child_process';
import{resolve}from'node:path';

export function evaluateDestinationHealth(status,{id,captureId,from,to}={}){
 const failures=[],valid=Number.isSafeInteger(from)&&Number.isSafeInteger(to)&&from<=to&&Number.isSafeInteger(status?.at)&&status.at>=to&&status.id===id&&(status.observation_capture_id??null)===(captureId??null)&&status.destinations?.length===2;
 if(!valid)return {qualification:false,complete:false,failures:['ExportDeliveryScopeInvalid']};
 for(const signal of['logs','traces']){const rows=status.destinations.filter(d=>d.signal===signal);if(rows.length!==1){failures.push('ExportDeliveryScopeInvalid');continue;}const d=rows[0],complete=Date.parse(d.last_complete??''),error=d.last_error==null||d.last_error===''?null:Date.parse(d.last_error);if(d.enabled!==true||d.dataset!=='opentelemetry-'+signal)failures.push('ExportDestinationDisabledOrChanged');if(!Number.isFinite(complete)||complete<to||complete>status.at+1000)failures.push('ExportDeliveryNotSettled');if(error!==null&&(!Number.isFinite(error)||error>=from))failures.push('ExportDeliveryFailureObserved');}
 return {schema:1,qualification:false,complete:failures.length===0,failures:[...new Set(failures)],from,to,observed_at:status.at,status};
}
export async function readDestinationStatus(path){
 const lifecycle=resolve(import.meta.dirname,'paired-observer-lifecycle.mjs'),status=await new Promise((ok,bad)=>{const child=spawn(process.execPath,[lifecycle,'destination-status',path],{stdio:['ignore','pipe','pipe']}),parts=[];let size=0;const timer=setTimeout(()=>child.kill(),60000);child.stdout.on('data',p=>{size+=p.length;if(size>65536)child.kill();else parts.push(p);});child.stderr.resume();child.on('error',()=>{clearTimeout(timer);bad(Error('ExportDeliveryStatusUnavailable'));});child.on('exit',code=>{clearTimeout(timer);if(code||size>65536)return bad(Error('ExportDeliveryStatusUnavailable'));try{ok(JSON.parse(Buffer.concat(parts)));}catch{bad(Error('ExportDeliveryStatusInvalid'));}});});
 return status;
}

export async function destinationHealth(path,observer,timeframe){return evaluateDestinationHealth(await readDestinationStatus(path),{id:observer.id,captureId:observer.observation_capture_id,...timeframe});}

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
