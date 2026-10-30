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

// Bounded OTLP receiver, isolated from the immutable terminal counter.
// Provider batches may contain more resources than one sanitized storage chunk.
import{resourceFailureCode}from'../performance.mjs';
import{readBody}from'./logpush-probe-collector.mjs';import{fetchObserver,observerScopes}from'./paired-observer.mjs';
export function partitionExport(envelopes,signal){
 const groupKey=signal==='traces'?'resourceSpans':'resourceLogs',scopeKey=signal==='traces'?'scopeSpans':'scopeLogs',itemKey=signal==='traces'?'spans':'logRecords',pieces=[];let records=0,resources=0;
 if(!Array.isArray(envelopes)||envelopes.length>256)throw Error('ReceiverEnvelopeBudgetExceeded');
 for(const envelope of envelopes){const groups=envelope?.[groupKey]??(envelope?.resource&&Array.isArray(envelope?.[scopeKey])?[envelope]:null);if(!Array.isArray(groups)||groups.length>4096)throw Error('ReceiverEnvelopeInvalid');resources+=groups.length;if(resources>4096)throw Error('ReceiverEnvelopeBudgetExceeded');for(const group of groups){const scopes=group?.[scopeKey];if(!Array.isArray(scopes)||scopes.length>128)throw Error('ReceiverScopeInvalid');for(const scope of scopes){const items=scope?.[itemKey];if(!Array.isArray(items))throw Error('ReceiverScopeInvalid');records+=items.length;if(records>10000)throw Error('ReceiverRecordBudgetExceeded');for(let start=0;start<items.length;start+=1000)pieces.push({group:{...group,[scopeKey]:[{...scope,[itemKey]:items.slice(start,start+1000)}]},count:Math.min(1000,items.length-start)});}}}
 const chunks=[];let groups=[],count=0;for(const piece of pieces){if(groups.length===128||count+piece.count>1000){chunks.push({[groupKey]:groups});groups=[];count=0;}groups.push(piece.group);count+=piece.count;}if(groups.length)chunks.push({[groupKey]:groups});if(chunks.length>128)throw Error('ReceiverChunkBudgetExceeded');return{chunks,resources,records};
}
export default{async fetch(request,env){
 if(!env.API_TOKEN||request.headers.get('Authorization')!=='Bearer '+env.API_TOKEN)return new Response('unauthorized',{status:401});const path=new URL(request.url).pathname;
 if(path==='/health'){const page=await env.LOGS.list({prefix:'receiver-v1/shapes/',limit:8}),shapes=await Promise.all(page.objects.map(async o=>{const object=await env.LOGS.get(o.key);return object.json();}));const rejected=await env.LOGS.list({prefix:'receiver-v1/rejections/',limit:1});return Response.json({qualification:false,version:env.WORKER_VERSION?.id??null,rejections_present:rejected.objects.length>0,shapes});}
 if(!['/v1/logs','/v1/traces'].includes(path)||request.method!=='POST')return new Response('not found',{status:404});const signal=path.endsWith('logs')?'logs':'traces',shape={qualification:false,at:Date.now(),signal,receiver_version:env.WORKER_VERSION?.id??null};let key;
 try{observerScopes(env);const bytes=await readBody(request);shape.bytes=bytes.length;const digest=Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',bytes)),b=>b.toString(16).padStart(2,'0')).join('');key='receiver-v1/shapes/'+shape.at+'-'+digest+'.json';const text=new TextDecoder('utf-8',{fatal:true}).decode(bytes);let parsed;try{parsed=JSON.parse(text);}catch{parsed=text.trim().split('\n').map(line=>JSON.parse(line));}shape.top_kind=Array.isArray(parsed)?'array':parsed===null?'null':typeof parsed;const envelopes=Array.isArray(parsed)?parsed:[parsed];shape.envelopes=envelopes.length;shape.known_top_keys=envelopes.slice(0,8).map(e=>['resourceSpans','resourceLogs','resource','scopeSpans','scopeLogs','resource_spans','resource_logs'].filter(k=>e&&Object.hasOwn(e,k)));shape.resource_counts=envelopes.slice(0,256).map(e=>Array.isArray(e?.[signal==='traces'?'resourceSpans':'resourceLogs'])?e[signal==='traces'?'resourceSpans':'resourceLogs'].length:null);const split=partitionExport(envelopes,signal);shape.records=split.records;shape.resources=split.resources;shape.chunks=split.chunks.length;
 for(const chunk of split.chunks){const response=await fetchObserver(new Request('https://receiver.invalid'+path,{method:'POST',headers:{Authorization:'Bearer '+env.API_TOKEN},body:JSON.stringify(chunk)}),env);if(!response.ok){const value=await response.json();throw Error(/^(?:Telemetry|Observer)[A-Za-z]+$/.test(value.error??'')?value.error:'ReceiverIngestionFailed');}}
 shape.accepted=true;await env.LOGS.put(key,JSON.stringify(shape),{onlyIf:{etagDoesNotMatch:'*'}});return Response.json({});
 }catch(error){shape.accepted=false;const failure=resourceFailureCode(error);if(failure)shape.failure_code=failure;shape.error=/^(?:Receiver|Telemetry|Observer)[A-Za-z]+$/.test(error.message??'')?error.message:'ReceiverEnvelopeInvalid';await env.LOGS.put(key??'receiver-v1/shapes/'+shape.at+'-'+crypto.randomUUID()+'.json',JSON.stringify(shape),{onlyIf:{etagDoesNotMatch:'*'}});await env.LOGS.put('receiver-v1/rejections/'+shape.at+'-'+crypto.randomUUID()+'.json',JSON.stringify(shape),{onlyIf:{etagDoesNotMatch:'*'}});return Response.json({error:shape.error},{status:400});}
}};
