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
