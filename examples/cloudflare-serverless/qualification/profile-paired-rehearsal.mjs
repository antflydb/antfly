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

// Descriptive profiling of received native terminal records. This does not
// substitute application trace IDs for provider IDs or establish completeness.
import{applicationTracePhases,profilePrivateRecords}from'./private-phase-profile.mjs';
import{readFile,writeFile}from'node:fs/promises';import{resolve}from'node:path';import{createHash}from'node:crypto';import{attemptIdentity}from'./invocation-accounting.mjs';import{resourceFailureCode}from'../performance.mjs';
const[reportPath,captureDir,out]=process.argv.slice(2),base=JSON.parse(await readFile(reportPath)),pct=(v,p)=>v.length?[...v].sort((a,b)=>a-b)[Math.ceil(v.length*p)-1]:null,stats=v=>({samples:v.length,p50:pct(v,.5),p95:pct(v,.95),p99:pct(v,.99),mean:v.length?v.reduce((a,b)=>a+b,0)/v.length:null}),result={qualification:false,execution_id:base.execution_id,scope:'received native terminal records; independent canonical completeness is evaluated separately',sides:[]};
async function captureFor(source){let path=resolve(captureDir,source+'-0.json'),bytes;try{bytes=await readFile(path);}catch(error){if(error.code!=='ENOENT')throw error;const saved=(base.captures??[]).filter(c=>c.source===source).sort((a,b)=>a.pass-b.pass).at(-1);if(!saved)throw error;path=saved.path;bytes=await readFile(path);if(createHash('sha256').update(bytes).digest('hex')!==saved.sha256)throw Error('RehearsalCaptureHashChanged');}return{path,bytes,capture:JSON.parse(bytes)};}
for(const side of base.sides){const attempts=new Map(side.invocation_ledger.attempts.map(a=>[attemptIdentity(a.id),a])),loaded=await captureFor(side.run),capturePath=loaded.path,bytes=loaded.bytes,capture=loaded.capture,records=capture.objects.filter(o=>o.value.source==='provider-tail-dispatch').flatMap(o=>o.value.records),metrics=new Map(),phases={};
 const append=(key,v)=>{if(!Number.isFinite(v))return;const values=metrics.get(key)??[];values.push(v);metrics.set(key,values);};let resourceRecords=0,truncated=0,matched=0;
 for(const record of records){const attempt=attempts.get(record.attempt_id_sha256);if(!attempt)continue;matched++;const phase=attempt.phase;phases[phase]=(phases[phase]??0)+1;append(phase+'/terminal.cpu_ms',record.event.cpuTime);append(phase+'/terminal.wall_ms',record.event.wallTime);if(record.event.truncated)truncated++;let resource=['exceededMemory','exceededCpu'].includes(record.event.outcome)||record.event.exceptions?.some(e=>resourceFailureCode(e.message));
  for(const log of record.event.logs??[])for(const text of log.message??[]){let value;try{value=JSON.parse(text);}catch{continue;}if(resourceFailureCode(value.failure_code)||resourceFailureCode(value.error))resource=true;
   if(value.event==='antfly.trace'){append(phase+'/trace:'+value.scope+'/elapsed_ms',value.elapsed_ms);for(const span of value.spans??[])if(span.name)append(phase+'/trace:'+value.scope+'/'+span.name,span.duration_ms);}
   const name=value.event+(value.action?':'+value.action:'')+(value.op?':'+value.op:'');for(const field of['elapsed_ms','io_wait_ms','operations','operation_elapsed_ms','clock_refreshes','working_set_peak_bytes','read_state_cache_hits','read_state_cache_extensions','decoded_manifest_cache_hits','app_read_view_attempts','app_read_view_evictions','app_read_view_capacity_fallbacks'])append(phase+'/'+name+'/'+field,value[field]);for(const field of['rpc_calls','wait_ms','get','head','put','list','bytes_read'])append(phase+'/'+name+'/physical.'+field,value.physical?.[field]);for(const[op,n]of Object.entries(value.physical?.by_op??value.physical_r2?.by_op??{}))append(phase+'/'+name+'/physical.by_op.'+op,n);
  }if(resource)resourceRecords++;
 }
 result.sides.push({run:side.run,capture:capturePath,capture_sha256:createHash('sha256').update(bytes).digest('hex'),matched_physical_attempts:matched,ledger_attempts:attempts.size,received_native_records:records.length,received_resource_records:resourceRecords,truncated_records:truncated,phases,metrics:Object.fromEntries([...metrics].map(([k,v])=>[k,stats(v)]))});
 if(process.argv.includes('--all-roles')){const mapping=applicationTracePhases(records,attempts),roles={};for(const role of['writer','reader','builder']){const loaded=await captureFor(side.run+'-'+role),rows=loaded.capture.objects.filter(o=>o.value.source==='provider-tail-dispatch').flatMap(o=>o.value.records);roles[role]={capture:loaded.path,capture_sha256:createHash('sha256').update(loaded.bytes).digest('hex'),...profilePrivateRecords(rows,mapping.phases)};}Object.assign(result.sides.at(-1),{application_trace_conflicts:mapping.conflicts,private_roles:roles});}
}
result.limitations=['Terminal wall time can include work after the HTTP response.','Phase spans can overlap or nest; do not sum them or subtract clocks across isolates.','This is a short descriptive comparison; provider identity/export failures remain disqualifying.'];await writeFile(out,JSON.stringify(result,null,2),{mode:0o600,flag:'wx'});console.log(JSON.stringify(result.sides.map(s=>({run:s.run,matched:s.matched_physical_attempts,ledger:s.ledger_attempts,read:Object.fromEntries(Object.entries(s.metrics).filter(([k])=>k.startsWith('read/')&&(k.includes('trace:http')||k.includes('coordinator_snapshot')&&k.endsWith('/io_wait_ms')))),write:Object.fromEntries(Object.entries(s.metrics).filter(([k])=>k.startsWith('write/trace:http')))}))));
