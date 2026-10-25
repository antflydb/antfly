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

// Missing evidence fails closed. A short smoke run cannot pass the soak gate.
export const policy=Object.freeze({trials:3,soak_ms:3600000,cold_samples:100,read_p95_ms:1000,write_p95_ms:3000,cold_p95_ms:5000,minimum_reads:1000,minimum_writes:300,max_publication_lag:4096,max_backlog:128,max_oldest_ms:900000});
export function percentile(values,p){const sorted=values.filter(Number.isFinite).sort((a,b)=>a-b);return sorted.length?sorted[Math.ceil(sorted.length*p)-1]:null;}
export function evaluateTrial(report){
 const failures=[],check=(ok,reason)=>{if(!ok)failures.push(reason);};
 const candidate=report.candidate,baseline=report.baseline;
 check(!report.fatal,"trial failed before completion");
 check(report.documents===512,"declared soak corpus missing or changed");
 check(report.schema===1&&candidate&&baseline,"versioned paired report missing");
 if(!candidate||!baseline)return{passed:false,failures};
 for(const[name,side]of[["baseline",baseline],["candidate",candidate]]){
  check(typeof side.version==="string"&&side.version.length===36&&side.samples?.length>0,name+" version/samples missing");
  check(side.samples?.every(s=>s.version===side.version),name+" mixed deployment versions");
  check(side.errors?.length===0,name+" failed requests");
  check(side.resource_errors?.length===0,name+" memory/resource failures");
  check(side.verification?.bodies>=2052&&side.verification?.receipts>=8&&side.verification?.passed===true,name+" exact data/receipt verification missing");
 }
 check(typeof report.execution_id==="string"&&report.execution_id.length===36&&baseline.execution_id===report.execution_id&&candidate.execution_id===report.execution_id,"fresh paired execution identity missing");
 const metrics={};
 for(const[phase,key,limit,minimum]of[["read","read_p95_ms",policy.read_p95_ms,policy.minimum_reads],["write","write_p95_ms",policy.write_p95_ms,policy.minimum_writes]]){
  const a=candidate.samples?.filter(s=>s.phase===phase).map(s=>s.ms)??[],b=baseline.samples?.filter(s=>s.phase===phase).map(s=>s.ms)??[];
  metrics[key]=percentile(a,.95);metrics["baseline_"+key]=percentile(b,.95);
  check(a.length>=minimum&&b.length>=minimum,phase+" insufficient samples");check(metrics[key]!=null&&metrics[key]<=limit,phase+" latency exceeds target");
  check(metrics[key]!=null&&metrics["baseline_"+key]!=null&&metrics[key]<=metrics["baseline_"+key],phase+" regressed against paired baseline");
 }
 check(report.soak_ms>=policy.soak_ms,"one-hour soak incomplete");
 check(report.offered_period_ms===30000,"fixed arrival cadence changed or missing");
 const work=report.workload;
 check(work?.period_ms===30000&&work.rounds===Math.ceil(report.soak_ms/30000)&&work.requested_ms===report.soak_ms&&Number.isFinite(work.started_at)&&Number.isFinite(work.offered_end_at)&&work.offered_end_at-work.started_at>=report.soak_ms,"fixed arrival execution evidence missing");
 for(const[name,side]of[["baseline",baseline],["candidate",candidate]]){
  check(side.samples?.every(s=>Number.isFinite(s.ms)&&s.ms>=0&&Number.isInteger(s.attempts)&&s.attempts>=1),name+" invalid request measurements");
  check(!side.provenance_errors?.length,name+" artifact provenance mismatch");
 }
 const lag=candidate.lag??[],queue=candidate.queue??[];
 check(lag.length>=240&&["a","b","c","d"].every(tenant=>new Set(lag.filter(s=>s.tenant===tenant).map(s=>s.minute)).size>=60),"publication-lag coverage incomplete");
 check(lag.every(s=>Number.isFinite(s.lag)&&s.lag>=0&&s.lag<=policy.max_publication_lag),"publication backlog exceeded bound");
 check(candidate.converged===true,"publication failed final convergence");
 check(queue.length>=60&&queue.every(s=>s.dead_letter_count===0&&Number.isFinite(s.backlog_count)&&s.backlog_count<=policy.max_backlog&&Number.isFinite(s.oldest_ms)&&s.oldest_ms<=policy.max_oldest_ms),"Queue backlog/age coverage or bound failed");
 const first=queue.filter(s=>s.minute>=15&&s.minute<25).map(s=>s.backlog_count),last=queue.slice(-10).map(s=>s.backlog_count);
 check(first.length===10&&last.length===10&&percentile(last,.95)<=Math.max(4,percentile(first,.95)+4),"Queue backlog grows across soak");
 const cold=candidate.cold??[];metrics.cold_p95_ms=percentile(cold.map(s=>s.ms),.95);
 check(cold.length>=policy.cold_samples&&new Set(cold.map(s=>s.epoch)).size>=policy.cold_samples&&cold.every(s=>Number.isInteger(s.epoch)&&s.epoch>=1&&s.epoch<=100&&s.version===candidate.version&&s.count===String(report.documents)&&s.attempts===1),"fresh-actor recovery coverage/retries failed");
 check(metrics.cold_p95_ms!=null&&metrics.cold_p95_ms<=policy.cold_p95_ms,"fresh-actor recovery exceeds target");
 check(report.faults?.passed===true&&report.faults?.artifact_sha256?.length===64,"failure-injection evidence missing");
 const ann=report.ann;
 const datasets=ann?.datasets??[];
 check(datasets.some(d=>d.kind==="synthetic"&&d.documents>=10000)&&datasets.some(d=>d.kind==="model-embeddings"&&d.documents>=1000&&d.fixture_sha256?.length===64),"synthetic/model ANN corpus coverage missing");
 check(datasets.every(d=>[8,32,"all"].every(probes=>d.points?.some(p=>p.probes===probes&&p.samples>=10&&p.candidate_recall>=p.baseline_recall&&p.candidate_gets<=p.baseline_gets&&p.candidate_bytes<=p.baseline_bytes))),"ANN operating point regressed");
 check(ann?.samples>=10&&ann.exact_control===true&&ann.candidate_recall>=ann.baseline_recall&&ann.candidate_gets<=ann.baseline_gets&&ann.candidate_bytes<=ann.baseline_bytes,"equal-budget ANN evidence missing or regressed");
 return{passed:failures.length===0,metrics,failures};
}
export function evaluateGate(reports){const trials=reports.map(evaluateTrial);return{policy,passed:reports.length>=policy.trials&&new Set(reports.map(r=>r.trial)).size>=policy.trials&&new Set(reports.map(r=>r.execution_id)).size>=policy.trials&&trials.every(t=>t.passed),trials};}
