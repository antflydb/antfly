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

import{resourceFailureCode}from'../performance.mjs';

// Application trace IDs classify received logs for descriptive profiling only.
// They are not provider invocation identities or proof of terminal completeness.
export function decodedLogs(record){const values=[];for(const log of record.event?.logs??[])for(const text of log.message??[])try{const value=JSON.parse(text);if(value&&typeof value==='object')values.push(value);}catch{}return values;}
export function applicationTracePhases(records,attempts){const phases=new Map(),conflicts=new Set();for(const r of records){const attempt=attempts.get(r.attempt_id_sha256);if(!attempt)continue;for(const log of decodedLogs(r)){const id=log.trace_id??log.trace?.trace_id;if(typeof id!=='string'||conflicts.has(id))continue;if(phases.has(id)&&phases.get(id)!==attempt.phase){phases.delete(id);conflicts.add(id);}else phases.set(id,attempt.phase);}}return{phases,conflicts:conflicts.size};}
const stats=values=>{const v=[...values].sort((a,b)=>a-b),percent=p=>v.length?v[Math.ceil(v.length*p)-1]:null;return{samples:v.length,p50:percent(.5),p95:percent(.95),p99:percent(.99),mean:v.length?v.reduce((a,b)=>a+b,0)/v.length:null};};
export function profilePrivateRecords(records,tracePhases){
 const metrics=new Map(),phases={},fallbacks={};let correlated=0,ambiguous=0,resources=0,truncated=0;
 const append=(key,value)=>{if(Number.isFinite(value)){const v=metrics.get(key)??[];v.push(value);metrics.set(key,v);}};
 for(const record of records){const logs=decodedLogs(record),classified=logs.map(log=>({log,phase:tracePhases.get(log.trace_id??log.trace?.trace_id)})).filter(v=>v.phase),recordPhases=new Set(classified.map(v=>v.phase));if(record.event?.truncated)truncated++;if(['exceededMemory','exceededCpu'].includes(record.event?.outcome)||record.event?.exceptions?.some(e=>resourceFailureCode(e.message))||logs.some(log=>resourceFailureCode(log.failure_code)||resourceFailureCode(log.error)))resources++;
  if(classified.length){correlated++;if(recordPhases.size===1){const phase=[...recordPhases][0];append(phase+'/terminal.cpu_ms',record.event?.cpuTime);append(phase+'/terminal.wall_ms',record.event?.wallTime);}else ambiguous++;}
  for(const{log,phase}of classified){phases[phase]=(phases[phase]??0)+1;const trace=log.event==='antfly.trace'?log:log.trace;if(trace){append(phase+'/trace:'+trace.scope+'/elapsed_ms',trace.elapsed_ms);for(const span of trace.spans??[])if(span.name)append(phase+'/trace:'+trace.scope+'/'+span.name,span.duration_ms);}
   const name=log.event+(log.action?':'+log.action:'')+(log.op?':'+log.op:'');for(const field of['elapsed_ms','native_ms','io_wait_ms','operations','transferred_bytes','prepare_attempts','queue_wait_max_ms','begin_ms','commit_ms','native_cpu_ms','working_set_peak_bytes','callers','records'])append(phase+'/'+name+'/'+field,log[field]);const physical=log.physical??log.physical_r2;for(const field of['calls','wait_ms','wait_union_ms','io_waves','bytes_read','bytes_written','read_view_lease_hit','read_view_lease_join','read_admission_ms','rpc_calls'])append(phase+'/'+name+'/physical.'+field,physical?.[field]);for(const[op,n]of Object.entries(physical?.by_op??{}))append(phase+'/'+name+'/physical.by_op.'+op,n);if(physical?.read_admission_fallback_reason){const key=phase+'/'+physical.read_admission_fallback_reason;fallbacks[key]=(fallbacks[key]??0)+1;}
  }
 }
 return{qualification:false,received_native_records:records.length,correlated_native_records:correlated,mixed_phase_native_records:ambiguous,received_resource_records:resources,truncated_records:truncated,classified_log_phases:phases,fallbacks,metrics:Object.fromEntries([...metrics].map(([k,v])=>[k,stats(v)]))};
}
