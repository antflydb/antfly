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

import{readPhaseProfile}from'../phase-profile.mjs';import{percentile}from'./gate-policy.mjs';
export function attributeRequestPhases(samples){
 const values=new Map(),failures=[],coverage={samples:samples.length,profiled:0,missing:0,invalid:0,shared_cohort_requests:0};
 const add=(key,value)=>{if(Number.isFinite(value)&&value>=0){const rows=values.get(key)??[];rows.push(value);values.set(key,rows);}};
 for(const sample of samples){if(!sample.profile){coverage.missing++;continue;}let p;try{p=readPhaseProfile(JSON.stringify(sample.profile));if(!p.owner)throw Error('MissingOwner');}catch{coverage.invalid++;continue;}coverage.profiled++;const op=sample.op??'unknown',service=sample.service_ms??sample.ms;
  if(!Number.isFinite(service)||service<0||p.http.elapsed_ms>service+5){failures.push('PhaseClientDurationInvalid');continue;}if(sample.outcome&&sample.outcome!=='ok')failures.push('PhaseRequestFailed');
  add(op+'/client_ms',service);add(op+'/http_ms',p.http.elapsed_ms);add(op+'/outside_http_ms',Math.max(0,service-p.http.elapsed_ms));add(op+'/owner_ms',p.owner.elapsed_ms);if(p.owner.callers>1)coverage.shared_cohort_requests++;
  const rpc=p.http.spans.find(s=>['resident_rpc','writer_rpc'].includes(s.name));if(rpc){add(op+'/owner_rpc_ms',rpc.duration_ms);add(op+'/rpc_outside_owner_ms',Math.max(0,rpc.duration_ms-p.owner.elapsed_ms));}
  for(const[role,profile]of[['http',p.http],['owner',p.owner]]){for(const span of profile.spans)add(op+'/'+role+'/'+span.name,span.duration_ms);for(const[k,v]of Object.entries(profile.physical??{}))add(op+'/'+role+'/physical/'+k,v);for(const[k,v]of Object.entries(profile.metrics??{}))add(op+'/'+role+'/native/'+k,v);}
 }
 if(coverage.missing)failures.push('PhaseCoverageMissing');if(coverage.invalid)failures.push('PhaseCoverageInvalid');
 return{schema:1,qualification:false,full_gate_passed:false,complete:failures.length===0&&coverage.profiled===coverage.samples,coverage,failures:[...new Set(failures)],metrics:Object.fromEntries([...values].map(([k,v])=>[k,{n:v.length,p50:percentile(v,.5),p95:percentile(v,.95),p99:percentile(v,.99)}])),limitations:['Subtract same-request durations only; never subtract percentiles or absolute timestamps from different isolates.','Nested spans and shared cohort/group spans are not additive and do not represent per-request CPU.','Outside-handler/RPC residual includes scheduling, routing and response work; it is not pure network latency.','This measures attribution completeness only; platform/canonical resource coverage and full gate remain separate.']};
}


import{resourceFailureCode}from'../performance.mjs';
import{policy}from'./gate-policy.mjs';

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

// Numeric readiness only. Stretch goals never change the full gate's policy.
export function httpLatencyTargets(baseline,candidate,phase){
 if(!['cold','warm','burst','mixed'].includes(phase))throw Error('HttpLatencyPhaseInvalid');const rows=[];
 for(const op of phase==='mixed'?['lookup','mutate']:['lookup']){const a=(baseline?.samples??[]).filter(s=>s.op===op),b=(candidate?.samples??[]).filter(s=>s.op===op),valid=a.length>0&&b.length>0&&[...a,...b].every(s=>s.outcome==='ok'&&Number.isFinite(s.ms)&&s.ms>=0),base=percentile(a.map(s=>s.ms),.95),next=percentile(b.map(s=>s.ms),.95),absolute=phase==='cold'?policy.cold_p95_ms:op==='mutate'?policy.write_p95_ms:policy.read_p95_ms,factor=phase==='cold'?10:2;
  rows.push({op,baseline_samples:a.length,candidate_samples:b.length,baseline_p95_ms:base,candidate_p95_ms:next,gate_limit_ms:absolute,gate_absolute_passed:valid&&next<=absolute,gate_no_regression_passed:valid&&next<=base,design_goal_factor:factor,design_goal_limit_ms:base==null?null:base/factor,design_goal_passed:valid&&next<=base/factor});
 }return{qualification:false,full_gate_passed:false,phase,rows,gate_numeric_passed:rows.every(r=>r.gate_absolute_passed&&r.gate_no_regression_passed),design_goals_passed:rows.every(r=>r.design_goal_passed)};
}

export function summarizeWorkload(side){
 const group=rows=>({samples:rows.length,p50_ms:percentile(rows.map(s=>s.ms),.5),p95_ms:percentile(rows.map(s=>s.ms),.95),p99_ms:percentile(rows.map(s=>s.ms),.99),execution_p95_ms:percentile(rows.map(s=>s.execution_ms),.95),scheduling_delay_p95_ms:percentile(rows.map(s=>s.scheduling_delay_ms),.95),retried:rows.filter(s=>s.attempts>1).length,attempts:rows.reduce((n,s)=>n+s.attempts,0)});
 const samples=side.samples??[],phases=[...new Set(samples.map(s=>s.phase))];
 return{phases:Object.fromEntries(phases.map(phase=>[phase,group(samples.filter(s=>s.phase===phase))])),tenants:Object.fromEntries(['a','b','c','d'].map(tenant=>[tenant,Object.fromEntries(['read','write','compact'].map(phase=>[phase,group(samples.filter(s=>s.tenant===tenant&&s.phase===phase))]))])),errors:side.errors??[],resource_errors:side.resource_errors??[],converged:side.converged,verification:side.verification};
}

// Short-run latency readiness uses the same numeric limits as the full gate,
// without pretending it supplies the full gate's duration or populations.
export function rehearsalLatencyTargets(baseline,candidate){
 const metrics={},checks={};for(const phase of['read','write']){const a=(candidate?.samples??[]).filter(s=>s.phase===phase).map(s=>s.ms),b=(baseline?.samples??[]).filter(s=>s.phase===phase).map(s=>s.ms),valid=a.length>0&&b.length>0&&[...a,...b].every(v=>Number.isFinite(v)&&v>=0);metrics[phase]={candidate_samples:a.length,baseline_samples:b.length,candidate_p95_ms:percentile(a,.95),baseline_p95_ms:percentile(b,.95)};checks[phase+'_target']=valid&&metrics[phase].candidate_p95_ms<=policy[phase+'_p95_ms'];checks[phase+'_not_worse_than_baseline']=valid&&metrics[phase].candidate_p95_ms<=metrics[phase].baseline_p95_ms;}return{qualification:false,metrics,checks,passed:Object.values(checks).every(Boolean)};
}
