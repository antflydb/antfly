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

import{percentile,policy}from'./gate-policy.mjs';
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
