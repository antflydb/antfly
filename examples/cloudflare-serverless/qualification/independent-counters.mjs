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
// Fixed taxonomy only: provider messages can contain arbitrary strings.
export function counterFailureDiagnostic(value,status=200){
 const messages=(Array.isArray(value?.errors)?value.errors:[]).slice(0,16).map(e=>String(e?.message??'')).join(' ');
 const kind=status===401||status===403?'authorization':status===429||/rate.?limit|quota|too many|limit.*exceed/i.test(messages)?'rate_limit':/unknown field|cannot query|not defined|does not exist|invalid.*field/i.test(messages)?'schema':/timeout|timed out|deadline/i.test(messages)?'timeout':'provider_error';
 return{kind,http_status:status,error_codes:(Array.isArray(value?.errors)?value.errors:[]).slice(0,16).map(e=>e.code).filter(Number.isSafeInteger)};
}
function counterQueryFailure(value,status){return Object.assign(Error('IndependentCounterQueryFailed'),{diagnostic:counterFailureDiagnostic(value,status)});}
export function counterQuery(run,role,timeframe){
 assertGateRun(run);if(!['app','writer','reader','builder'].includes(role)||!Number.isSafeInteger(timeframe?.from)||!Number.isSafeInteger(timeframe?.to)||timeframe.from<0||timeframe.to<=timeframe.from||timeframe.to-timeframe.from>60000)throw Error('CounterScopeMismatch');
 const worker=run.id+(role==='app'?'':'-'+role),from=new Date(timeframe.from).toISOString(),to=new Date(timeframe.to).toISOString();
 return{query:`{viewer{accounts(filter:{accountTag:${JSON.stringify(run.account)}}){workersInvocationsAdaptive(limit:1,filter:{scriptName:${JSON.stringify(worker)},datetime_geq:${JSON.stringify(from)},datetime_leq:${JSON.stringify(to)}}){sum{requests errors} avg{sampleInterval}}}}}`};
}
export function parseCounter(value,run,role,timeframe){
 counterQuery(run,role,timeframe);
 if(value?.errors!=null&&(!Array.isArray(value.errors)||value.errors.length)||!Array.isArray(value?.data?.viewer?.accounts)||value.data.viewer.accounts.length!==1)throw counterQueryFailure(value,200);
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
   if(!response.ok){await response.body?.cancel();throw counterQueryFailure(null,response.status);}
   let bytes=0;const chunks=[];for await(const chunk of response.body){bytes+=chunk.byteLength;if(bytes>65536)throw Error('IndependentCounterResponseExceeded');chunks.push(chunk);}
   let value;try{value=JSON.parse(Buffer.concat(chunks));}catch{throw Error('IndependentCounterSchemaUnknown');}
   const count=parseCounter(value,run,role,timeframe);result.roles[role]=count;if(!count.complete)result.failures.push({role,reason:count.reason});
  }catch(error){const allowed=['IndependentCounterQueryFailed','IndependentCounterSchemaUnknown','IndependentCounterResponseExceeded'];result.failures.push({role,reason:allowed.includes(error?.message)?error.message:'IndependentCounterTransportFailed',...(error.diagnostic?{diagnostic:error.diagnostic}:{})});}
 }
 return result;
}

const workerTypes=['fetch','queue','scheduled','cron','email','tail'],durableTypes=['jsrpc','alarm'];
export function durableCounterQuery(run,role,namespace,timeframe){
 counterQuery(run,role,timeframe);
 if(role==='app'||!/^[a-f0-9]{32}$/.test(namespace??''))throw Error('CounterScopeMismatch');
 const ids=[namespace,namespace.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/,'$1-$2-$3-$4-$5')];
 return{query:`{viewer{accounts(filter:{accountTag:${JSON.stringify(run.account)}}){durableObjectsInvocationsAdaptiveGroups(limit:1,filter:{namespaceId_in:${JSON.stringify(ids)},datetime_geq:${JSON.stringify(new Date(timeframe.from).toISOString())},datetime_leq:${JSON.stringify(new Date(timeframe.to).toISOString())}}){sum{requests errors} avg{sampleInterval} confidence(level:0.99){sum{requests{estimate lower upper sampleSize}}}}}}}`};
}
export function parseDurableCounter(value,run,role,binding,timeframe){
 durableCounterQuery(run,role,binding?.id,timeframe);
 const worker=run.id+'-'+role;
 if(binding.worker!==worker||binding.source!=='platform-namespace-api')throw Error('CounterScopeMismatch');
 if(value?.errors!=null&&(!Array.isArray(value.errors)||value.errors.length)||value?.data?.viewer?.accounts?.length!==1)throw counterQueryFailure(value,200);
 const rows=value.data.viewer.accounts[0].durableObjectsInvocationsAdaptiveGroups;
 if(!Array.isArray(rows)||rows.length>1)throw Error('IndependentCounterSchemaUnknown');
 const base={worker,timeframe:{...timeframe},source:'independent-platform-counter',dataset:'durableObjectsInvocationsAdaptiveGroups',namespace_binding:{...binding},event_types:[...durableTypes]};
 if(!rows.length)return{...base,count:0,sample_interval:null,complete:false,reason:'EmptyCounterSamplingUnproven'};
 const count=rows[0]?.sum?.requests,errors=rows[0]?.sum?.errors,interval=rows[0]?.avg?.sampleInterval;
 if(!Number.isSafeInteger(count)||count<0||!Number.isSafeInteger(errors)||errors<0||errors>count||!Number.isFinite(interval)||interval<1)throw Error('IndependentCounterSchemaUnknown');
 const result={...base,count,errors,sample_interval:interval,complete:interval===1,...(interval!==1?{reason:'IndependentCounterSampled'}:{})},confidence=rows[0]?.confidence?.sum?.requests;
 if(confidence&&['estimate','lower','upper','sampleSize'].every(key=>Number.isFinite(confidence[key])&&confidence[key]>=0))result.confidence=Object.fromEntries(['estimate','lower','upper','sampleSize'].map(key=>[key,confidence[key]]));
 return result;
}
async function counterJson(request,url,token,body){
 const response=await request(url,{method:body?'POST':'GET',headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{}),signal:AbortSignal.timeout(30000)});
 if(!response.ok){await response.body?.cancel();throw counterQueryFailure(null,response.status);}
 let size=0;const parts=[];for await(const part of response.body){size+=part.byteLength;if(size>65536)throw Error('IndependentCounterResponseExceeded');parts.push(part);}
 try{return JSON.parse(Buffer.concat(parts));}catch{throw Error('IndependentCounterSchemaUnknown');}
}
// Dataset populations are disjoint. No addition of a Worker total to a DO total
// until both scopes, event types, sampling and the stored rows reconcile.
export async function collectInvocationPopulations(run,token,timeframe,{fetch:request=fetch}={}){
 const workers=await collectIndependentCounters(run,token,timeframe,{fetch:request}),result={schema:2,qualification:false,roles:{},failures:[],limitations:['Empty adaptive datasets cannot prove an unsampled zero.','Unknown invocation types, missing namespace bindings and overlapping populations fail closed.','Confidence intervals corroborate counts; only explicit sampling metadata can establish an unsampled population.']};
 for(const role of['app','writer','reader','builder']){const worker=workers.roles[role];result.roles[role]={worker:run.id+(role==='app'?'':'-'+role),timeframe:{...timeframe},populations:worker?[{...worker,event_types:[...workerTypes]}]:[]};}
 result.failures.push(...workers.failures.map(f=>({...f,population:'worker'})));
 const bindings=new Map();
 try{
  for(let page=1;page<=16;page++){
   const value=await counterJson(request,`https://api.cloudflare.com/client/v4/accounts/${run.account}/workers/durable_objects/namespaces?per_page=100&page=${page}`,token);
   if(value.success!==true||!Array.isArray(value.result)||value.result.length>100)throw Error('NamespaceBindingUnknown');
   for(const row of value.result)for(const role of['writer','reader','builder'])if(row.script===run.id+'-'+role){const id=typeof row.id==='string'?row.id.replaceAll('-',''):'';if(!/^[a-f0-9]{32}$/.test(id)||bindings.has(role))throw Error('NamespaceBindingUnknown');bindings.set(role,{id,worker:row.script,source:'platform-namespace-api'});}
   const total=value.result_info?.total_pages;
   if(total!=null&&(!Number.isSafeInteger(total)||total<1||total>16))throw Error('NamespaceBindingUnknown');
   if(total!=null?page>=total:value.result.length<100)break;
   if(page===16)throw Error('NamespaceBindingUnknown');
  }
 }catch{result.failures.push({population:'durable',reason:'NamespaceBindingUnknown'});return result;}
 for(const role of['writer','reader','builder'])try{
  const binding=bindings.get(role);if(!binding)throw Error('NamespaceBindingUnknown');
  const value=await counterJson(request,'https://api.cloudflare.com/client/v4/graphql',token,durableCounterQuery(run,role,binding.id,timeframe)),counter=parseDurableCounter(value,run,role,binding,timeframe);
  result.roles[role].populations.push(counter);if(!counter.complete)result.failures.push({role,population:'durable',reason:counter.reason});
 }catch(error){const known=['NamespaceBindingUnknown','IndependentCounterQueryFailed','IndependentCounterSchemaUnknown','IndependentCounterResponseExceeded'];result.failures.push({role,population:'durable',reason:known.includes(error?.message)?error.message:'IndependentCounterTransportFailed',...(error.diagnostic?{diagnostic:error.diagnostic}:{})});}
 return result;
}
export function reconcileCounterPopulations(run,role,rows,counter,timeframe){
 const failures=[],covered=new Set(),datasets=new Set(),worker=run.id+(role==='app'?'':'-'+role),sameRange=value=>value?.from===timeframe?.from&&value?.to===timeframe?.to;
 if(counter?.worker!==worker||!sameRange(counter?.timeframe)||!Array.isArray(counter?.populations)||!counter.populations.length)return['IndependentInvocationCountMissing'];
 for(const population of counter.populations){
  const expected=population.dataset==='workersInvocationsAdaptive'?workerTypes:population.dataset==='durableObjectsInvocationsAdaptiveGroups'&&role!=='app'?durableTypes:null;
  if(!expected||datasets.has(population.dataset)||population.source!=='independent-platform-counter'||population.worker!==worker||!sameRange(population.timeframe)||JSON.stringify(population.event_types)!==JSON.stringify(expected)){failures.push('InvocationPopulationInvalid');continue;}
  datasets.add(population.dataset);
  if(population.dataset==='durableObjectsInvocationsAdaptiveGroups'&&(population.namespace_binding?.worker!==worker||population.namespace_binding?.source!=='platform-namespace-api'||!/^[a-f0-9]{32}$/.test(population.namespace_binding?.id??'')))failures.push('InvocationPopulationInvalid');
  for(const type of expected){if(covered.has(type))failures.push('InvocationPopulationOverlap');covered.add(type);}
  if(population.complete!==true||population.sample_interval!==1||!Number.isSafeInteger(population.count)||population.count<0)failures.push('IndependentInvocationCountMissing');
  else if(rows.filter(row=>expected.includes(row.event_type)).length!==population.count)failures.push('InvocationCountMismatch');
 }
 if(!datasets.has('workersInvocationsAdaptive')||role!=='app'&&!datasets.has('durableObjectsInvocationsAdaptiveGroups'))failures.push('IndependentInvocationCountMissing');
 if(rows.some(row=>!covered.has(row.event_type)))failures.push('InvocationPopulationUncovered');
 return[...new Set(failures)];
}
