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
import{parseDiagnosticJson}from'./diagnostic-memory.mjs';
import{persistCanonicalRows}from'./canonical-windows.mjs';
import{resourceFailureCode}from'../performance.mjs';
import{sanitizeTailEvent}from'./metrics-tail.mjs';
import{sanitizeOtel}from'./otel-evidence.mjs';
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
  let value;try{value=typeof message==='string'?parseDiagnosticJson(message):null;}catch{continue;}
  if(value?.event==='antfly.qualification_probe'&&uuid(value.id)&&['fetch','rpc'].includes(value.kind)&&['start','late','complete'].includes(value.phase))probes.push({id:value.id,kind:value.kind,phase:value.phase});
 }
 const exceptions=(Array.isArray(row.Exceptions)?row.Exceptions:[]).map(e=>({name:e.name??e.Name,message:e.message??e.Message}));
 const event=row.Event&&typeof row.Event==='object'?row.Event:{},request=(event.request??event.Request)&&typeof(event.request??event.Request)==='object'?(event.request??event.Request):{};
 let ray;try{ray=new Headers(request.headers??request.Headers??{}).get('cf-ray');}catch{}
 const type=['fetch','scheduled','alarm','queue','email','worker_rpc','hibernatable_web_socket'].includes(row.EventType)?row.EventType:'unknown';
 const knownKeys=new Set(['ID','RequestID','RayID','Request','Response','RpcMethod','RpcMethodName','RequestId','RayId','Id','request','response','rpcMethod','requestId','rayId','id','type','scheduledTime','cron','queue','batchSize','webSocket','rpcMethodName']);
 return{script:source,timestamp,event_type:type,entrypoint:row.Entrypoint==='Probe'?'Probe':row.Entrypoint==='default'?'default':'other',version_id:[row.ScriptVersion?.id,row.ScriptVersion?.Id,row.ScriptVersion?.ID].find(uuid)??null,version_shape:{type:row.ScriptVersion===null?'null':typeof row.ScriptVersion,known_keys:row.ScriptVersion&&typeof row.ScriptVersion==='object'?Object.keys(row.ScriptVersion).filter(key=>['id','Id','ID','tag','Tag','message','Message'].includes(key)):[],unknown_keys:row.ScriptVersion&&typeof row.ScriptVersion==='object'?Object.keys(row.ScriptVersion).filter(key=>!['id','Id','ID','tag','Tag','message','Message'].includes(key)).length:0},
  event_keys:Object.keys(event).filter(k=>knownKeys.has(k)).sort(),unknown_event_keys:Object.keys(event).filter(k=>!knownKeys.has(k)).length,
  identity_candidates:{event_request_id_sha256:await identity(event.requestId??event.RequestId??event.RequestID),event_id_sha256:await identity(event.id??event.Id??event.ID),event_ray_id_sha256:await identity(event.rayId??event.RayId??event.RayID),request_ray_id_sha256:await identity(ray)},probes,
  event:sanitizeTailEvent({outcome:row.Outcome,cpuTime:row.CPUTimeMs,wallTime:row.WallTimeMs,scriptVersion:row.ScriptVersion,logs,exceptions,truncated:rawLogs.length>256})??{}};
}
export async function readBody(request){
 if(!request.body)fail('TelemetryBodyMissing');
 const encoding=request.headers.get('Content-Encoding');if(encoding&&encoding!=='gzip')fail('TelemetryEncodingInvalid');
 async function bounded(stream){
  const reader=stream.getReader(),chunks=[];let bytes=0;
  try{for(;;){const item=await reader.read();if(item.done)break;bytes+=item.value.byteLength;if(bytes>6*1024*1024){await reader.cancel();fail('TelemetryBodyExceeded');}chunks.push(item.value);}}
  finally{reader.releaseLock();}
  const body=new Uint8Array(bytes);let at=0;for(const chunk of chunks){body.set(chunk,at);at+=chunk.byteLength;}return body;
 }
 // Sniff file magic without retaining a second complete compressed body.
 // Enforce the original 6 MiB limits on both wire and decompressed bytes.
 const source=request.body.getReader(),first=[];let wireBytes=0,closed=false;
 const release=()=>{if(!closed){closed=true;source.releaseLock();}};
 const next=async()=>{const item=await source.read();if(!item.done){wireBytes+=item.value.byteLength;if(wireBytes>6*1024*1024){await source.cancel();fail('TelemetryBodyExceeded');}}return item;};
 try{while(wireBytes<2){const item=await next();if(item.done)break;if(item.value.byteLength)first.push(item.value);}}
 catch(error){release();throw error;}
 const prefix=first.flatMap(chunk=>Array.from(chunk.subarray(0,2))).slice(0,2),gzip=prefix[0]===31&&prefix[1]===139;
 const stream=new ReadableStream({async pull(controller){try{if(first.length){controller.enqueue(first.shift());return;}const item=await next();if(item.done){release();controller.close();}else controller.enqueue(item.value);}catch(error){release();controller.error(error);}},async cancel(reason){try{await source.cancel(reason);}finally{release();}}});
 return bounded(gzip?stream.pipeThrough(new DecompressionStream('gzip')):stream);
}

export default {async fetch(request,env){
 if(!env.API_TOKEN||request.headers.get('Authorization')!=='Bearer '+env.API_TOKEN)return new Response('unauthorized',{status:401});
 const path=new URL(request.url).pathname;
 if(!uuid(env.CAPTURE_ID)||!/^antfly-1278-(?:telemetry-[a-z0-9]+-source|g-[a-z0-9]+-[1-3]-[bc](?:-(?:writer|reader|builder))?)$/.test(env.SOURCE_WORKER??''))return Response.json({error:'TelemetryScopeInvalid'},{status:500});
 const prefix='probe-v1/'+env.CAPTURE_ID+'/';
 if(env.RETIRE_MODE==='true'&&!['/report','/page','/drain'].includes(path))return new Response('retired',{status:410});
 let rejectionShape;
 try{
  if(['/ingest','/v1/traces','/v1/logs'].includes(path)&&request.method==='POST'){
   const bytes=await readBody(request),text=new TextDecoder('utf-8',{fatal:true}).decode(bytes),lines=text.trim().split('\n');
   if(bytes.length<=4096){let challenge;try{challenge=JSON.parse(text);}catch{}if(['test','tests'].includes(challenge?.content)&&Object.keys(challenge).length===1)return Response.json({validation:true,qualification:false});}
   if(path!=='/ingest'){
    const signal=path==='/v1/traces'?'traces':'logs';let parsed;try{parsed=parseDiagnosticJson(text,{lines:true});}catch(error){if(error.message==='TelemetryJsonComplexityExceeded')throw error;fail('TelemetryJsonInvalid');}const envelopes=Array.isArray(parsed)?parsed:[parsed];
    if(envelopes.length>1000)fail('TelemetryRecordBudgetExceeded');const rows=[];for(const envelope of envelopes){rows.push(...await sanitizeOtel(envelope,env.SOURCE_WORKER,signal));if(rows.length>1000)fail('TelemetryRecordBudgetExceeded');}
    const digest=await hash(bytes),record={schema:1,qualification:false,signal,collector_version:env.WORKER_VERSION?.id??null,received_at:Date.now(),raw_batch_sha256:digest,rows},value=JSON.stringify(record);if(encoder.encode(value).byteLength>4*1024*1024)fail('TelemetrySanitizedBudgetExceeded');if(rows.length){if(env.WINDOWED_EVIDENCE==='true')await persistCanonicalRows(env,record);else await env.LOGS.put(prefix+'batches/'+signal+'-'+digest+'.json',value,{onlyIf:{etagDoesNotMatch:'*'},httpMetadata:{contentType:'application/json'}});}return Response.json({});
   }
   if(env.WINDOWED_EVIDENCE==='true')fail('TelemetryCanonicalOtelRequired');
   if(lines.length>1000)fail('TelemetryRecordBudgetExceeded');
   const rows=[];for(const line of lines){if(encoder.encode(line).byteLength>256*1024)fail('TelemetryRecordBudgetExceeded');let row;try{row=parseDiagnosticJson(line);}catch{fail('TelemetryJsonInvalid');}rejectionShape={json_type:row===null?'null':Array.isArray(row)?'array':typeof row,content:row?.content==='tests'?'tests':row?.content==='test'?'test':row&&Object.hasOwn(row,'content')?'other':'absent',has_script_name:typeof row?.ScriptName==='string',keys:row&&typeof row==='object'?Object.keys(row).filter(key=>['content','test','filename','ScriptName','Event','Logs'].includes(key)):[],key_count:row&&typeof row==='object'?Object.keys(row).length:0};rows.push(await sanitizeLogpushProbe(row,env.SOURCE_WORKER));}
   const digest=await hash(bytes),value=JSON.stringify({schema:1,qualification:false,received_at:Date.now(),raw_batch_sha256:digest,rows});if(encoder.encode(value).byteLength>4*1024*1024)fail('TelemetrySanitizedBudgetExceeded');
   await env.LOGS.put(prefix+'batches/'+digest+'.json',value,{onlyIf:{etagDoesNotMatch:'*'},httpMetadata:{contentType:'application/json'}});
   return Response.json({stored:true,records:rows.length,qualification:false});
  }
  if(path==='/counter-witness'&&request.method==='POST'){const input=parseDiagnosticJson(new TextDecoder().decode(await readBody(request)));if(!uuid(input.id)||!env.COUNTER)fail('TelemetryWitnessInvalid');return Response.json(await env.COUNTER.witness(input.id));}
  if(path==='/exercise'&&request.method==='POST'){
   const input=parseDiagnosticJson(new TextDecoder('utf-8',{fatal:true}).decode(await readBody(request))),count=input.count??8;if(!Number.isInteger(count)||count<1||count>32)fail('TelemetryProbeBudgetInvalid');
   const expected=[];for(let i=0;i<count;i++){const id=crypto.randomUUID(),response=await env.FIXTURE.fetch('https://probe.invalid/?nonce='+id,{method:'POST',headers:{Authorization:'Bearer '+env.API_TOKEN}});if(!response.ok)fail('TelemetryProbeFailed');const value=await response.json();if(value.id!==id||!uuid(value.rpc))fail('TelemetryProbeIdentityInvalid');expected.push(value);}
   const id=crypto.randomUUID();await env.LOGS.put(prefix+'plans/'+id+'.json',JSON.stringify({id,expected}),{onlyIf:{etagDoesNotMatch:'*'}});return Response.json({qualification:false,id,expected});
  }
  if(path==='/page'&&request.method==='POST'){
   const input=parseDiagnosticJson(new TextDecoder().decode(await readBody(request)));
   if(Object.keys(input).some(k=>k!=='cursor')||input.cursor!=null&&(typeof input.cursor!=='string'||input.cursor.length>4096))fail('TelemetryCursorInvalid');
   // Re-list with a smaller native limit to preserve R2's actual continuation
   // cursor. A cursor over concurrently written keys is NOT a closed snapshot.
   let limit=32,page;for(;;){page=await env.LOGS.list({prefix,limit,...(input.cursor?{cursor:input.cursor}:{})});if(page.objects.reduce((n,o)=>n+o.size,0)<=4*1024*1024)break;if(limit===1)fail('TelemetryReportBudgetExceeded');limit=Math.max(1,Math.floor(limit/2));}
   if(page.truncated&&(typeof page.cursor!=='string'||!page.cursor||page.cursor===input.cursor))fail('TelemetryCursorInvalid');
   const objects=[];for(const item of page.objects){const object=await env.LOGS.get(item.key);if(!object)fail('TelemetryReportObjectMissing');objects.push({key:item.key,value:await object.json()});}
   return Response.json({qualification:false,capture_id:env.CAPTURE_ID,collector_version:env.WORKER_VERSION?.id??null,objects,cursor:page.truncated?page.cursor:null});
  }
  if(path==='/report'&&request.method==='POST'){
   const page=await env.LOGS.list({prefix,limit:1000});if(page.truncated)fail('TelemetryReportBudgetExceeded');const objects=[];let total=0;
   for(const item of page.objects){total+=item.size;if(total>4*1024*1024)fail('TelemetryReportBudgetExceeded');const object=await env.LOGS.get(item.key);if(!object)fail('TelemetryReportObjectMissing');objects.push({key:item.key,value:await object.json()});}return Response.json({qualification:false,capture_id:env.CAPTURE_ID,collector_version:env.WORKER_VERSION?.id??null,objects});
  }
  if(path==='/drain'&&request.method==='POST'){const page=await env.LOGS.list({prefix,limit:1000});if(page.objects.length)await env.LOGS.delete(page.objects.map(o=>o.key));return Response.json({deleted:page.objects.length,done:page.objects.length===0});}
  return new Response('not found',{status:404});
 }catch(error){const code=/^Telemetry[A-Za-z]+$/.test(error?.message??'')?error.message:'TelemetryIngestionFailed';if(['/ingest','/v1/traces','/v1/logs'].includes(path)){try{await env.LOGS.put(prefix+'rejections/'+crypto.randomUUID()+'.json',JSON.stringify({schema:1,qualification:false,rejection:code,...(resourceFailureCode(error)?{failure_code:resourceFailureCode(error)}:{}),collector_version:env.WORKER_VERSION?.id??null,...(error.attribute_key?{attribute_conflict:['service.name','faas.name','faas.version','faas.invocation_id','cloudflare.handler_type','cloudflare.outcome','cloudflare.cpu_time_ms','cloudflare.wall_time_ms','cloudflare.invocation.sequence.number','cloudflare.execution_model'].includes(error.attribute_key)?error.attribute_key:'other'}:{}),...(rejectionShape?{shape:rejectionShape}:{}),encoding:['gzip','identity'].includes(request.headers.get('Content-Encoding'))?request.headers.get('Content-Encoding'):request.headers.has('Content-Encoding')?'other':'absent'}));}catch{}}return Response.json({error:code},{status:code.includes('Exceeded')?413:code==='TelemetryIngestionFailed'?503:400});}
}};
