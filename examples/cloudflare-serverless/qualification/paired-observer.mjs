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

// One isolated observer for an exact paired deployment. No database credentials
// or database storage bindings are installed here. Source routing is allowlisted.
import collector,{readBody}from'./logpush-probe-collector.mjs';
import{persistTailDelivery,counterWitness}from'./tail-counter.mjs';
const uuid=v=>typeof v==='string'&&/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/.test(v);
export function observerScopes(env){
 const scopes=JSON.parse(env.SOURCE_SCOPES??'null');if(!Array.isArray(scopes)||scopes.length!==8||new Set(scopes.map(s=>s.source)).size!==8||new Set(scopes.map(s=>s.capture_id)).size!==8)throw Error('ObserverScopeInvalid');
 const pairs=new Set();for(const [i,s]of scopes.entries()){const m=/^(antfly-1278-g-[a-z0-9]+-[1-3])-[bc](?:-(writer|reader|builder))?$/.exec(s.source??'');if(!m||!uuid(s.capture_id)||s.binding!=='PRODUCER_'+i)throw Error('ObserverScopeInvalid');pairs.add(m[1]);}
 if(pairs.size!==1||!['b','c'].every(side=>['','-writer','-reader','-builder'].every(role=>scopes.some(s=>s.source===[...pairs][0]+'-'+side+role))))throw Error('ObserverScopeInvalid');return scopes;
}
const scopedEnv=(env,s)=>({...env,SOURCE_WORKER:s.source,CAPTURE_ID:s.capture_id,PRODUCER:env[s.binding],COUNTER:{witness:id=>counterWitness({...env,SOURCE_WORKER:s.source,PRODUCER:env[s.binding]},id)}});
export function splitObserverEnvelope(envelope,signal,scopes){
 const key=signal==='traces'?'resourceSpans':'resourceLogs',groups=envelope?.[key];if(!Array.isArray(groups)||groups.length>128)throw Error('TelemetryOtelEnvelopeInvalid');const split=new Map();
 for(const group of groups){const attributes=group.resource?.attributes;if(!Array.isArray(attributes)||attributes.length>256)throw Error('TelemetryAttributeInvalid');const names=attributes.filter(a=>['service.name','faas.name'].includes(a?.key)).map(a=>a.value?.stringValue);if(!names.length||names.some(n=>typeof n!=='string'||n!==names[0])||!scopes.some(s=>s.source===names[0]))throw Error('TelemetrySourceMismatch');const list=split.get(names[0])??[];list.push(group);split.set(names[0],list);}
 return [...split].map(([source,groups])=>({source,envelope:{[key]:groups}}));
}
export async function persistObserverTail(events,env){
 const scopes=observerScopes(env);if(!Array.isArray(events)||events.length>1000||events.some(e=>!scopes.some(s=>s.source===e?.scriptName)))throw Error('ObserverTailScopeInvalid');
 for(const s of scopes){const selected=events.filter(e=>e.scriptName===s.source);if(selected.length)await persistTailDelivery(selected,scopedEnv(env,s));}
}
export async function fetchObserver(request,env){
 if(!env.API_TOKEN||request.headers.get('Authorization')!=='Bearer '+env.API_TOKEN)return new Response('unauthorized',{status:401});
 let scopes;try{scopes=observerScopes(env);}catch{return Response.json({error:'ObserverScopeInvalid'},{status:500});}
 const path=new URL(request.url).pathname;
 if(env.RETIRE_MODE==='true'&&path!=='/drain-all'&&!/\/(?:page|drain)$/.test(path))return new Response('retired',{status:410});
 try{
  if(path==='/health'){const p=await env.LOGS.list({prefix:'observer-v1/rejections/',limit:1});return Response.json({qualification:false,rejections_present:p.objects.length>0,scopes:scopes.map(s=>({source:s.source,capture_id:s.capture_id})),collector_version:env.WORKER_VERSION?.id??null});}
  if(path==='/drain-all'&&request.method==='POST'&&env.RETIRE_MODE==='true'){const p=await env.LOGS.list({limit:1000});if(p.objects.length)await env.LOGS.delete(p.objects.map(o=>o.key));return Response.json({done:p.objects.length===0,deleted:p.objects.length});}
  if(['/v1/traces','/v1/logs'].includes(path)&&request.method==='POST'){
   const text=new TextDecoder('utf-8',{fatal:true}).decode(await readBody(request)),signal=path.endsWith('traces')?'traces':'logs';let envelopes;try{envelopes=[JSON.parse(text)];}catch{envelopes=text.trim().split('\n').map(line=>JSON.parse(line));}if(envelopes.length>128)throw Error('TelemetryRecordBudgetExceeded');
   // Validate all resource scopes before persisting any part of a request.
   const parts=envelopes.flatMap(e=>splitObserverEnvelope(e,signal,scopes));if(parts.length>128)throw Error('TelemetryRecordBudgetExceeded');
   for(const part of parts){const s=scopes.find(s=>s.source===part.source),response=await collector.fetch(new Request('https://observer.invalid'+path,{method:'POST',headers:{Authorization:'Bearer '+env.API_TOKEN},body:JSON.stringify(part.envelope)}),scopedEnv(env,s));if(!response.ok)return response;}
   return Response.json({});
  }
  const match=/^\/source\/([^/]+)\/(page|drain|counter-witness)$/.exec(path),scope=match&&scopes.find(s=>s.source===match[1]);if(!scope||request.method!=='POST')return new Response('not found',{status:404});
  return collector.fetch(new Request('https://observer.invalid/'+match[2],request),scopedEnv(env,scope));
 }catch(error){const code=/^(?:Telemetry|Observer)[A-Za-z]+$/.test(error?.message??'')?error.message:'ObserverIngestionFailed';await env.LOGS.put('observer-v1/rejections/'+crypto.randomUUID()+'.json',JSON.stringify({at:Date.now(),error:code}));return Response.json({error:code},{status:400});}
}
