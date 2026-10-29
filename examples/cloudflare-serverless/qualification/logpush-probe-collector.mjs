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

// Diagnostic only. Source identities and delivery completeness are not assumed.
import{sanitizeTailEvent}from'./metrics-tail.mjs';
const encoder=new TextEncoder(),uuid=value=>typeof value==='string'&&/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/.test(value);
const fail=code=>{throw Error(code);};
async function hash(bytes){return Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',bytes)),b=>b.toString(16).padStart(2,'0')).join('');}
const identity=async value=>typeof value==='string'&&value.length>0&&value.length<=256?hash(encoder.encode(value)):null;
export async function sanitizeLogpushProbe(row,source){
 if(row.ScriptName!==source)fail('TelemetrySourceMismatch');
 const timestamp=typeof row.EventTimestampMs==='string'&&/^\d{4}-/.test(row.EventTimestampMs)?Date.parse(row.EventTimestampMs):row.EventTimestampMs;
 if(!Number.isSafeInteger(timestamp)||timestamp<0)fail('TelemetryTimestampInvalid');
 const rawLogs=Array.isArray(row.Logs)?row.Logs:[],logs=rawLogs.slice(0,256).map(log=>({timestamp:log.timestamp??log.Timestamp,message:Array.isArray(log.message??log.Message)?log.message??log.Message:typeof(log.message??log.Message)==='string'?[log.message??log.Message]:[]}));
 const probes=[];
 for(const log of logs)for(const message of log.message.slice(0,32)){
  let value;try{value=typeof message==='string'?JSON.parse(message):null;}catch{continue;}
  if(value?.event==='antfly.qualification_probe'&&uuid(value.id)&&['fetch','rpc'].includes(value.kind)&&['start','late','complete'].includes(value.phase))probes.push({id:value.id,kind:value.kind,phase:value.phase});
 }
 const exceptions=(Array.isArray(row.Exceptions)?row.Exceptions:[]).map(e=>({name:e.name??e.Name,message:e.message??e.Message}));
 const event=row.Event&&typeof row.Event==='object'?row.Event:{},request=event.request&&typeof event.request==='object'?event.request:{};
 let ray;try{ray=new Headers(request.headers??{}).get('cf-ray');}catch{}
 const type=['fetch','scheduled','alarm','queue','email','worker_rpc','hibernatable_web_socket'].includes(row.EventType)?row.EventType:'unknown';
 const knownKeys=new Set(['request','response','rpcMethod','requestId','rayId','id','type','scheduledTime','cron','queue','batchSize','webSocket','rpcMethodName']);
 return{script:source,timestamp,event_type:type,entrypoint:row.Entrypoint==='Probe'?'Probe':row.Entrypoint==='default'?'default':'other',version_id:uuid(row.ScriptVersion?.id)?row.ScriptVersion.id:null,
  event_keys:Object.keys(event).filter(k=>knownKeys.has(k)).sort(),unknown_event_keys:Object.keys(event).filter(k=>!knownKeys.has(k)).length,
  identity_candidates:{event_request_id_sha256:await identity(event.requestId),event_id_sha256:await identity(event.id),event_ray_id_sha256:await identity(event.rayId),request_ray_id_sha256:await identity(ray)},probes,
  event:sanitizeTailEvent({outcome:row.Outcome,cpuTime:row.CPUTimeMs,wallTime:row.WallTimeMs,scriptVersion:row.ScriptVersion,logs,exceptions,truncated:rawLogs.length>256})??{}};
}
async function readBody(request){
 if(!request.body)fail('TelemetryBodyMissing');
 const encoding=request.headers.get('Content-Encoding');if(encoding&&encoding!=='gzip')fail('TelemetryEncodingInvalid');
 async function bounded(stream){
  const reader=stream.getReader(),chunks=[];let bytes=0;
  try{for(;;){const item=await reader.read();if(item.done)break;bytes+=item.value.byteLength;if(bytes>6*1024*1024){await reader.cancel();fail('TelemetryBodyExceeded');}chunks.push(item.value);}}
  finally{reader.releaseLock();}
  const body=new Uint8Array(bytes);let at=0;for(const chunk of chunks){body.set(chunk,at);at+=chunk.byteLength;}return body;
 }
 const bytes=await bounded(request.body);
 // Logpush uploads a gzip file; file uploads need not set Content-Encoding.
 return bytes[0]===31&&bytes[1]===139?bounded(new Blob([bytes]).stream().pipeThrough(new DecompressionStream('gzip'))):bytes;
}

export default {async fetch(request,env){
 if(!env.API_TOKEN||request.headers.get('Authorization')!=='Bearer '+env.API_TOKEN)return new Response('unauthorized',{status:401});
 const path=new URL(request.url).pathname;
 if(!uuid(env.CAPTURE_ID)||!/^antfly-1278-telemetry-[a-z0-9]+-source$/.test(env.SOURCE_WORKER??''))return Response.json({error:'TelemetryScopeInvalid'},{status:500});
 const prefix='probe-v1/'+env.CAPTURE_ID+'/';
 if(env.RETIRE_MODE==='true'&&!['/report','/drain'].includes(path))return new Response('retired',{status:410});
 let rejectionShape;
 try{
  if(path==='/ingest'&&request.method==='POST'){
   const bytes=await readBody(request),text=new TextDecoder('utf-8',{fatal:true}).decode(bytes),lines=text.trim().split('\n');
   if(bytes.length<=4096){let challenge;try{challenge=JSON.parse(text);}catch{}if(['test','tests'].includes(challenge?.content)&&Object.keys(challenge).length===1)return Response.json({validation:true,qualification:false});}
   if(lines.length>1000)fail('TelemetryRecordBudgetExceeded');
   const rows=[];for(const line of lines){if(encoder.encode(line).byteLength>256*1024)fail('TelemetryRecordBudgetExceeded');let row;try{row=JSON.parse(line);}catch{fail('TelemetryJsonInvalid');}rejectionShape={json_type:row===null?'null':Array.isArray(row)?'array':typeof row,content:row?.content==='tests'?'tests':row?.content==='test'?'test':row&&Object.hasOwn(row,'content')?'other':'absent',has_script_name:typeof row?.ScriptName==='string',keys:row&&typeof row==='object'?Object.keys(row).filter(key=>['content','test','filename','ScriptName','Event','Logs'].includes(key)):[],key_count:row&&typeof row==='object'?Object.keys(row).length:0};rows.push(await sanitizeLogpushProbe(row,env.SOURCE_WORKER));}
   const digest=await hash(bytes),value=JSON.stringify({schema:1,qualification:false,raw_batch_sha256:digest,rows});if(encoder.encode(value).byteLength>4*1024*1024)fail('TelemetrySanitizedBudgetExceeded');
   await env.LOGS.put(prefix+'batches/'+digest+'.json',value,{onlyIf:{etagDoesNotMatch:'*'},httpMetadata:{contentType:'application/json'}});
   return Response.json({stored:true,records:rows.length,qualification:false});
  }
  if(path==='/exercise'&&request.method==='POST'){
   const input=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(await readBody(request))),count=input.count??8;if(!Number.isInteger(count)||count<1||count>32)fail('TelemetryProbeBudgetInvalid');
   const expected=[];for(let i=0;i<count;i++){const id=crypto.randomUUID(),response=await env.FIXTURE.fetch('https://probe.invalid/?nonce='+id,{method:'POST'});if(!response.ok)fail('TelemetryProbeFailed');const value=await response.json();if(value.id!==id||!uuid(value.rpc))fail('TelemetryProbeIdentityInvalid');expected.push(value);}
   const id=crypto.randomUUID();await env.LOGS.put(prefix+'plans/'+id+'.json',JSON.stringify({id,expected}),{onlyIf:{etagDoesNotMatch:'*'}});return Response.json({qualification:false,id,expected});
  }
  if(path==='/report'&&request.method==='POST'){
   const page=await env.LOGS.list({prefix,limit:1000});if(page.truncated)fail('TelemetryReportBudgetExceeded');const objects=[];let total=0;
   for(const item of page.objects){total+=item.size;if(total>4*1024*1024)fail('TelemetryReportBudgetExceeded');const object=await env.LOGS.get(item.key);if(!object)fail('TelemetryReportObjectMissing');objects.push({key:item.key,value:await object.json()});}return Response.json({qualification:false,capture_id:env.CAPTURE_ID,objects});
  }
  if(path==='/drain'&&request.method==='POST'){const page=await env.LOGS.list({prefix,limit:1000});if(page.objects.length)await env.LOGS.delete(page.objects.map(o=>o.key));return Response.json({deleted:page.objects.length,done:page.objects.length===0});}
  return new Response('not found',{status:404});
 }catch(error){const code=/^Telemetry[A-Za-z]+$/.test(error?.message??'')?error.message:'TelemetryIngestionFailed';if(path==='/ingest'){try{await env.LOGS.put(prefix+'rejections/'+crypto.randomUUID()+'.json',JSON.stringify({schema:1,qualification:false,rejection:code,...(rejectionShape?{shape:rejectionShape}:{}),encoding:['gzip','identity'].includes(request.headers.get('Content-Encoding'))?request.headers.get('Content-Encoding'):request.headers.has('Content-Encoding')?'other':'absent'}));}catch{}}return Response.json({error:code},{status:code.includes('Exceeded')?413:code==='TelemetryIngestionFailed'?503:400});}
}};
