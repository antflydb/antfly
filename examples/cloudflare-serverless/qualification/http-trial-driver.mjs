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

import{attemptParameter,versionIdentity}from'./attempt-token.mjs';
const tenants=new Set(['a','b','c','d']),ops=new Set(['lookup','mutate','count','status','publish']);
const visibility=v=>typeof v==='string'&&/^[a-f0-9]{82}$/.test(v);
export function validatePlan(plan){
 if(!plan||!['baseline','candidate'].includes(plan.side)||!/^[-a-z0-9]{1,64}$/.test(plan.id??'')||!/^[a-f0-9-]{36}$/.test(plan.version??'')||!Array.isArray(plan.steps)||plan.steps.length<1||plan.steps.length>16)throw Error('InvalidTrialPlan');
 const documents=plan.documents??64;if(![64,128,512].includes(documents))throw Error('InvalidTrialPlan');
 const rounds=plan.rounds??1;if(!Number.isInteger(rounds)||rounds<1||rounds>8)throw Error('InvalidTrialPlan');
 if(new Set(plan.steps.filter(s=>s.attempt_id).map(s=>s.attempt_id)).size!==plan.steps.filter(s=>s.attempt_id).length)throw Error('InvalidTrialStep');
 for(const s of plan.steps){if(s.attempt_id!=null&&(!versionIdentity(s.attempt_id)||rounds!==1))throw Error('InvalidTrialStep');if(!tenants.has(s.tenant)||!ops.has(s.op)||s.op==='lookup'&&(!Number.isInteger(s.id)||s.id<0||s.id>=documents||!visibility(s.visibility))||s.op==='mutate'&&(!Number.isSafeInteger(s.sequence)||s.sequence<0||s.sequence+rounds-1>1000000)||s.op==='count'&&!visibility(s.visibility)||s.revision!=null&&(!Number.isSafeInteger(s.revision)||s.revision<0||s.revision>1000000))throw Error('InvalidTrialStep');}
 return plan;
}
export function documentBody(tenant,id,revision){return JSON.stringify({tenant,i:id,payload:'x'.repeat(1024),...(revision==null?{}:{revision})});}
export async function runPlan(plan,{origin,token,fetch:send=fetch,onResponse,clock=()=>performance.now(),kind='Cloudflare-to-public-Worker HTTP; excludes internet-client transit',clockDescription='I/O-advanced Worker wall clock around awaited HTTP'}={}){
 validatePlan(plan);const url=new URL(origin);if(url.protocol!=='https:'||!/^antfly-1278-cold-[a-z0-9]+-[a-f0-9]{4}\.[a-z0-9-]+\.workers\.dev$/.test(url.hostname)||url.pathname!=='/'||url.search||url.hash||url.port||url.username||url.password||!/^[a-f0-9]{64}$/.test(token??''))throw Error('InvalidTrialDestination');
 const samples=await Promise.all(plan.steps.map(async(s,index)=>{
  const started=clock(),sample={attempt_id:s.attempt_id??crypto.randomUUID(),index,tenant:s.tenant,op:s.op,id:s.id,sequence:s.sequence,outcome:'error'};
  const input=s.op==='lookup'?{op:'lookup',doc_id:'doc/'+s.id,required_visibility:s.visibility,fresh:true}:s.op==='mutate'?{op:'mutate',operation_id:'paired/'+plan.id+'/'+s.tenant+'/'+s.sequence,mutations:[{kind:'upsert',doc_id:'doc/0',body:documentBody(s.tenant,0,s.sequence)}]}:s.op==='count'?{op:'count',required_visibility:s.visibility}:{op:s.op};
  try{
   const response=await send(new URL('/api?__antfly_qualification_attempt='+attemptParameter(sample.attempt_id),origin),{method:'POST',redirect:'manual',cache:'no-store',headers:{Authorization:'Bearer '+token,'Content-Type':'application/json','X-Antfly-Tenant':s.tenant},body:JSON.stringify(input),signal:AbortSignal.timeout(60000)});await onResponse?.(sample.attempt_id,response);sample.status=response.status;sample.version=response.headers.get('X-Antfly-Version');sample.trace_id=response.headers.get('X-Antfly-Trace-Id');sample.colo=/\b([A-Z]{3})$/.exec(response.headers.get('CF-Ray')??'')?.[1]??null;
   if(response.status>=300&&response.status<400){await response.body?.cancel();throw Error('TrialRedirectRejected');}
   const value=await response.json();sample.ms=Math.max(0,clock()-started);
   if(!response.ok){sample.error=/\b(SQLITE_NOMEM|OutOfMemory|MemoryLimitExceeded|CPULimitExceeded|CoordinatorRecovering|CoordinatorCommitBusy|DeadlineExceeded|ManifestReadLeaseExpired)\b/.exec(String(value.error??''))?.[1]??'HTTP'+response.status;return sample;}
   if(sample.version!==plan.version)throw Error('TrialVersionMismatch');
   if(s.op==='lookup'){if(value.body!==documentBody(s.tenant,s.id,s.revision))throw Error('TrialBodyMismatch');if(!visibility(value.visibility)||value.visibility.slice(0,66)!==s.visibility.slice(0,66)||BigInt('0x'+value.visibility.slice(-16))<BigInt('0x'+s.visibility.slice(-16)))throw Error('TrialFenceMismatch');}
   if(s.op==='mutate'&&(value.committed!==true||!visibility(value.visibility)))throw Error('TrialDurabilityMismatch');
   if(s.op==='mutate'){const expected=plan.steps.find(p=>p.tenant===s.tenant&&p.op==='lookup')?.visibility??s.visibility;if(expected&&value.visibility.slice(0,66)!==expected.slice(0,66))throw Error('TrialFenceMismatch');}
   if(s.op==='count'&&value.count!==String(plan.documents??64))throw Error('TrialCountMismatch');
   if(visibility(value.visibility))sample.visibility=value.visibility;if(s.op==='publish')sample.has_more=value.has_more===true;sample.outcome='ok';
  }catch(error){sample.ms=Math.max(0,clock()-started);sample.error=/^(Trial\w+|TimeoutError|AbortError)$/.test(error.message??'')?error.message:['TimeoutError','AbortError'].includes(error.name)?error.name:'TrialTransportError';}
  return sample;
 }));return{qualification:false,kind,clock:clockDescription,side:plan.side,id:plan.id,samples,passed:samples.every(s=>s.outcome==='ok')};
}
export async function runRounds(plan,options){
 validatePlan(plan);const rounds=plan.rounds??1;if(!Number.isInteger(rounds)||rounds<1||rounds>8)throw Error('InvalidTrialPlan');const fences=new Map(),samples=[];
 for(let round=0;round<rounds;round++){
  const steps=plan.steps.map(s=>({...s,...(s.op==='mutate'?{sequence:s.sequence+round}:{}),...(s.op==='lookup'&&fences.has(s.tenant)?{visibility:fences.get(s.tenant)}:{})})),result=await runPlan({...plan,rounds:1,steps},options);
  for(const sample of result.samples){try{options?.onSample?.({...sample,round});}catch{}samples.push({...sample,round});if(sample.op==='mutate'&&sample.outcome==='ok'){const prior=fences.get(sample.tenant);if(!prior||BigInt('0x'+sample.visibility.slice(-16))>BigInt('0x'+prior.slice(-16)))fences.set(sample.tenant,sample.visibility);}}
  if(!result.passed)return{...result,rounds_completed:round+1,samples};
 }
 return{qualification:false,kind:options?.kind??'Cloudflare-to-public-Worker HTTP; excludes internet-client transit',clock:options?.clockDescription??'I/O-advanced Worker wall clock around awaited HTTP',side:plan.side,id:plan.id,rounds_completed:rounds,samples,passed:true};
}
let active=false;
export default{async fetch(request,env){
 if(request.method!=='POST'||new URL(request.url).pathname!=='/run')return new Response('not found',{status:404});
 if(!env.API_TOKEN||request.headers.get('Authorization')!=='Bearer '+env.API_TOKEN)return new Response('unauthorized',{status:401});if(active)return Response.json({error:'DriverBusy'},{status:503});
 active=true;try{if(Number(request.headers.get('Content-Length')??0)>16384)throw Error('InvalidTrialPlan');const parts=[];let bytes=0;const reader=request.body?.getReader();if(!reader)throw Error('InvalidTrialPlan');try{for(;;){const{done,value}=await reader.read();if(done)break;bytes+=value.length;if(bytes>16384)throw Error('InvalidTrialPlan');parts.push(value);}}finally{await reader.cancel();}const body=new Uint8Array(bytes);let offset=0;for(const part of parts){body.set(part,offset);offset+=part.length;}const plan=validatePlan(JSON.parse(new TextDecoder().decode(body))),prefix=plan.side.toUpperCase(),result=await runRounds(plan,{origin:env[prefix+'_ORIGIN'],token:env[prefix+'_TOKEN'],onSample:({visibility,...sample})=>console.log(JSON.stringify({event:'antfly.http_trial_call',...sample}))});result.runner_colo=/^[A-Z]{3}$/.test(request.cf?.colo??'')?request.cf.colo:null;return Response.json(result);}catch{return Response.json({error:'InvalidTrialPlanOrConfiguration'},{status:400});}finally{active=false;}
}};
