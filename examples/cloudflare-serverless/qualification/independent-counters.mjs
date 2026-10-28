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

// Independent analytics comparison, never a substitute for invocation logs.
// Counter/event-type or identity mismatches still fail reconciliation. An empty
// adaptive result cannot establish an unsampled zero for a quiet role.
// This dataset covers Worker events, not Durable Object RPCs. A role total
// cannot reconcile mixed Worker/RPC invocation logs without a separate DO
// dataset and a validated, disjoint event-type and sampling contract.
import{spawn}from'node:child_process';import{resolve}from'node:path';
import{assertGateRun}from'./gate-scope.mjs';
export async function readCounterToken(){
 return new Promise((ok,bad)=>{
  const child=spawn(process.execPath,[resolve('node_modules/wrangler/bin/wrangler.js'),'auth','token','--json'],{timeout:30000,stdio:['ignore','pipe','ignore'],env:{...process.env,WRANGLER_SEND_METRICS:'false'}});let text='',overflow=false;
  child.stdout.on('data',part=>{if(overflow)return;text+=part;if(text.length>65536){overflow=true;text='';child.kill();}});
  child.on('error',()=>bad(Error('CounterCredentialLookupFailed')));child.on('exit',code=>{if(code!==0||overflow)return bad(Error('CounterCredentialLookupFailed'));try{const auth=JSON.parse(text);if(!['oauth','api_token'].includes(auth.type)||typeof auth.token!=='string')throw Error();ok(auth.token);}catch{bad(Error('CounterCredentialLookupFailed'));}});
 });
}
export function counterQuery(run,role,timeframe){
 assertGateRun(run);if(!['app','writer','reader','builder'].includes(role)||!Number.isSafeInteger(timeframe?.from)||!Number.isSafeInteger(timeframe?.to)||timeframe.from<0||timeframe.to<=timeframe.from||timeframe.to-timeframe.from>60000)throw Error('CounterScopeMismatch');
 const worker=run.id+(role==='app'?'':'-'+role),from=new Date(timeframe.from).toISOString(),to=new Date(timeframe.to).toISOString();
 return{query:`{viewer{accounts(filter:{accountTag:${JSON.stringify(run.account)}}){workersInvocationsAdaptive(limit:1,filter:{scriptName:${JSON.stringify(worker)},datetime_geq:${JSON.stringify(from)},datetime_leq:${JSON.stringify(to)}}){sum{requests errors} avg{sampleInterval}}}}}`};
}
export function parseCounter(value,run,role,timeframe){
 counterQuery(run,role,timeframe);
 if(value?.errors!=null&&(!Array.isArray(value.errors)||value.errors.length)||!Array.isArray(value?.data?.viewer?.accounts)||value.data.viewer.accounts.length!==1)throw Error('IndependentCounterQueryFailed');
 const rows=value.data.viewer.accounts[0].workersInvocationsAdaptive;
 if(!Array.isArray(rows)||rows.length>1)throw Error('IndependentCounterSchemaUnknown');
 const base={worker:run.id+(role==='app'?'':'-'+role),timeframe:{...timeframe},source:'independent-platform-counter',dataset:'workersInvocationsAdaptive'};
 if(rows.length===0)return{...base,count:0,sample_interval:null,complete:false,reason:'EmptyCounterSamplingUnproven'};
 const count=rows[0]?.sum?.requests,errors=rows[0]?.sum?.errors,interval=rows[0]?.avg?.sampleInterval;
 if(!Number.isSafeInteger(count)||count<0||!Number.isSafeInteger(errors)||errors<0||errors>count||!Number.isFinite(interval)||interval<1)throw Error('IndependentCounterSchemaUnknown');
 return{...base,count,errors,sample_interval:interval,complete:interval===1,...(interval!==1?{reason:'IndependentCounterSampled'}:{})};
}
export async function collectIndependentCounters(run,token,timeframe,{fetch:request=fetch}={}){
 assertGateRun(run);if(typeof token!=='string'||token.length<20||token.length>4096)throw Error('CounterCredentialInvalid');
 const result={schema:1,qualification:false,dataset:'workersInvocationsAdaptive',roles:{},failures:[],limitations:['Adaptive empty results do not establish unsampled zero invocation counts.','Counter event-type coverage is tested by reconciliation, not assumed.']};
 for(const role of['app','writer','reader','builder']){
  const body=JSON.stringify(counterQuery(run,role,timeframe));
  try{
   const response=await request('https://api.cloudflare.com/client/v4/graphql',{method:'POST',headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},body,signal:AbortSignal.timeout(30000)});
   if(!response.ok){await response.body?.cancel();throw Error('IndependentCounterQueryFailed');}
   let bytes=0;const chunks=[];for await(const chunk of response.body){bytes+=chunk.byteLength;if(bytes>65536)throw Error('IndependentCounterResponseExceeded');chunks.push(chunk);}
   let value;try{value=JSON.parse(Buffer.concat(chunks));}catch{throw Error('IndependentCounterSchemaUnknown');}
   const count=parseCounter(value,run,role,timeframe);result.roles[role]=count;if(!count.complete)result.failures.push({role,reason:count.reason});
  }catch(error){const allowed=['IndependentCounterQueryFailed','IndependentCounterSchemaUnknown','IndependentCounterResponseExceeded'];result.failures.push({role,reason:allowed.includes(error?.message)?error.message:'IndependentCounterTransportFailed'});}
 }
 return result;
}
