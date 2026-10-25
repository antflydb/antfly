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

import {readFile,writeFile} from "node:fs/promises";
const source=await readFile(process.argv[2],"utf8"),events=[];let start=-1,depth=0,string=false,escape=false;
for(let i=0;i<source.length;i++){const c=source[i];if(start<0){if(c==="{"){start=i;depth=1;}continue;}if(string){if(escape)escape=false;else if(c==="\\")escape=true;else if(c==='"')string=false;}else if(c==='"')string=true;else if(c==="{"||c==="[")depth++;else if(c==="}"||c==="]"){if(--depth===0){events.push(JSON.parse(source.slice(start,i+1)));start=-1;}}}
const physical={},groups=[],operations=[],walGroups=[],native=[],traces=[];let partial=start>=0,dropped=0;
for(const event of events){if(event.outcome&&event.outcome!=="ok")dropped++;for(const log of event.logs??[])for(const message of log.message??[]){let sample;try{sample=typeof message==="string"?JSON.parse(message):message;}catch{continue;}if(!sample||typeof sample!=="object")continue;
 if(Number.isFinite(sample.working_set_peak_bytes))native.push(sample);
 if(sample.event==="antfly.trace")traces.push(sample);if(sample.trace?.event==="antfly.trace")traces.push(sample.trace);
 const io=sample.physical_r2??(sample.event==="antfly.operation"?sample.physical:null);
 if(io)for(const [op,count] of Object.entries(io.by_op??{}))physical[op]=(physical[op]??0)+count;
 if(sample.physical_r2)groups.push(sample);if(sample.event==="antfly.operation")operations.push(sample);if(sample.event==="antfly.wal_group")walGroups.push(sample);
}}
const percentile=(values,p)=>{const sorted=values.filter(Number.isFinite).sort((a,b)=>a-b);return sorted.length?sorted[Math.ceil(sorted.length*p)-1]:null;};
const phases=new Map();for(const trace of traces)for(const span of trace.spans??[]){if(!Number.isFinite(span.duration_ms))continue;const key=trace.scope+"/"+span.name;const values=phases.get(key)??[];values.push(span.duration_ms);phases.set(key,values);}
const criticalPath={reports:traces.length,correlation_ids:new Set(traces.map(trace=>trace.trace_id)).size,dropped_spans:traces.reduce((n,trace)=>n+(trace.dropped_spans??0),0),phases:Object.fromEntries([...phases].map(([phase,values])=>[phase,{samples:values.length,p50:percentile(values,.5),p95:percentile(values,.95),p99:percentile(values,.99)}]))};
const sum=(values,field)=>values.reduce((n,v)=>n+(v[field]??0),0),report={critical_path:criticalPath,tail_events:events.length,script_versions:[...new Set(events.map(e=>e.scriptVersion?.id).filter(Boolean))],partial_final_event:partial,non_ok_events:dropped,truncated_events:events.filter(e=>e.truncated).length,cpu_ms:{p50:percentile(events.map(e=>e.cpuTime),.5),p95:percentile(events.map(e=>e.cpuTime),.95),p99:percentile(events.map(e=>e.cpuTime),.99)},wall_ms:{p50:percentile(events.map(e=>e.wallTime),.5),p95:percentile(events.map(e=>e.wallTime),.95),p99:percentile(events.map(e=>e.wallTime),.99)},physical_r2_attempts:physical,coordinator_groups:groups.length,wal_group_sizes:walGroups.map(g=>g.callers),operation_count:operations.length,publication_samples:operations.filter(o=>o.op==="publish").map(o=>({outcome:o.operation_outcome??o.outcome,ms:o.operation_elapsed_ms??o.elapsed_ms,rpc_calls:o.physical?.rpc_calls})),native_working_peak:Math.max(0,...native.map(o=>o.working_set_peak_bytes)),wasm_instance_peak:Math.max(0,...native.map(o=>o.wasm_linear_bytes??0)),publication_rpc_calls:sum(operations.filter(o=>o.op==="publish").map(o=>o.physical??{}),"rpc_calls"),wasm_linear_peak:Math.max(0,...operations.map(o=>o.wasm_linear_total_bytes??0)),total_isolate_memory:null,billed_cost:null,limitations:["Tail can be sampled or dropped; authority, collection-helper and cleanup binding calls can be uninstrumented. These are observed attempts, not billed totals.","Phase spans can nest or overlap and use local clocks; never sum them as independent end-to-end time. WASM linear bytes exclude JS and platform memory."]};
if(process.argv[3])await writeFile(process.argv[3],JSON.stringify(report,null,2));console.log(JSON.stringify(report,null,2));
