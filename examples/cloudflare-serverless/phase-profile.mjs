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

const flags=['MUTATION_ELAPSED_BATCH_WINDOW','RESIDENT_SERVING','RESIDENT_DURABLE_BOOTSTRAP','RESIDENT_COLD_PIPELINE','RESIDENT_READ_COHORTS','RESIDENT_FETCH_COALESCE','NATIVE_WRITE_PLAN','NATIVE_WRITE_READ_AHEAD','NATIVE_READ_AFFINITY','WAL_DOCUMENT_INDEX'];
const counters=['calls','bytes_read','bytes_written','io_waves','wait_union_ms','concurrency_peak','resource_failures','resident_read_cache_hits','operations','transferred_bytes','working_set_peak_bytes'];
const safeNumbers=value=>Object.fromEntries(counters.filter(k=>Number.isFinite(value?.[k])&&value[k]>=0).map(k=>[k,value[k]]));
export function phaseProfile(trace,env,{physical,metrics,callers=1,traceIds=[]}={}){
 if(env.QUALIFICATION_PHASE_PROFILE!=='true')return undefined;
 const report=trace.report(),spans=report.spans.filter(s=>/^[a-zA-Z][a-zA-Z0-9_:.-]{0,63}$/.test(s.name)&&Number.isFinite(s.start_ms)&&Number.isFinite(s.duration_ms)&&s.duration_ms>=0),ids=[...new Set([trace.id,...traceIds])].filter(id=>/^[a-f0-9-]{36}$/.test(id));
 return{schema:1,trace_id:trace.id,scope:trace.scope,clock:'isolate-local-I/O-advanced; zero is not zero CPU',elapsed_ms:report.elapsed_ms,spans:spans.slice(0,64),dropped:report.dropped_spans+(report.spans.length-spans.length)+Math.max(0,spans.length-64),errors:report.errors.length,trace_ids:ids.slice(0,16),callers:Math.max(1,Math.min(16,callers)),physical:safeNumbers(physical),metrics:safeNumbers(metrics),active:Object.fromEntries(flags.map(k=>[k,env[k]==='true']))};
}
export function profileResult(result,trace,env,extra){const profile=phaseProfile(trace,env,extra);if(!profile)return result;if(Array.isArray(result.results))return{...result,results:result.results.map(r=>({...r,__antfly_profile:profile}))};return{...result,__antfly_profile:profile};}
export function phaseProfileHeaders(result,trace,env){
 const owner=result.__antfly_profile;delete result.__antfly_profile;if(env.QUALIFICATION_PHASE_PROFILE!=='true')return{};
 const profile={schema:1,http:phaseProfile(trace,env),owner:owner??null},json=JSON.stringify(profile);
 // Missing/oversized attribution stays explicit; it never fails an accepted write.
 return{'X-Antfly-Profile':json.length<=12288?json:JSON.stringify({schema:1,truncated:true,trace_id:trace.id})};
}
export function readPhaseProfile(header){
 if(header==null)return null;if(typeof header!=='string'||header.length>12288)throw Error('InvalidPhaseProfile');let p;try{p=JSON.parse(header);}catch{throw Error('InvalidPhaseProfile');}
 if(p.schema!==1||p.truncated===true)throw Error('IncompletePhaseProfile');
 for(const item of[p.http,...p.owner?[p.owner]:[]]){if(!item||item.schema!==1||!Number.isFinite(item.elapsed_ms)||item.elapsed_ms<0||!Array.isArray(item.spans)||item.spans.length>64||item.spans.some(s=>!Number.isFinite(s.start_ms)||!Number.isFinite(s.duration_ms)||s.duration_ms<0)||item.dropped!==0||item.errors!==0)throw Error('IncompletePhaseProfile');}
 if(p.owner&&!p.owner.trace_ids?.includes(p.http.trace_id))throw Error('PhaseProfileTraceMismatch');return p;
}
