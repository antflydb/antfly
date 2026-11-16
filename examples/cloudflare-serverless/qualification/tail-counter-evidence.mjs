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

import{minuteOf,windowPrefix}from'./canonical-windows.mjs';
import{DiagnosticCoverage}from'./live-diagnostics.mjs';
const uuid=v=>typeof v==='string'&&/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/i.test(v);
const sha=v=>typeof v==='string'&&/^[a-f0-9]{64}$/.test(v);
const worker=v=>typeof v==='string'&&/^antfly-1278-(?:telemetry-[a-z0-9]+-source|cold-[a-z0-9]+-[a-f0-9]{4}(?:-(?:writer|reader|builder))?|g-[a-z0-9]+-[1-3]-[bc](?:-(?:writer|reader|builder))?)$/.test(v);
// Exact terminal-delivery counts from an unfiltered provider Tail subscription.
// Delivery IDs are NOT invocation IDs and no sampling interval is invented.
// Empty populations require named, version-matched witnesses on both sides.
export function reconcileTailCounter(objects,roots,{source,version,counterVersion,captureId,timeframe,settledAt,witnesses}={}){
 const versions=Array.isArray(version)?version:[version];const failures=[],validWindow=Number.isSafeInteger(timeframe?.from)&&Number.isSafeInteger(timeframe?.to)&&timeframe.from<=timeframe.to&&timeframe.to-timeframe.from<=60000;
 if(!worker(source)||versions.length<1||versions.length>8||!versions.every(uuid)||new Set(versions).size!==versions.length||!uuid(counterVersion)||!uuid(captureId)||!validWindow||!Number.isSafeInteger(settledAt)||settledAt<timeframe.to+60000||!Array.isArray(objects)||objects.length>20000||!Array.isArray(roots)||roots.length>20000)return{qualification:false,complete:false,failures:['TailCounterScopeInvalid']};
 const prefix='probe-v1/'+captureId+'/counter/',seen=new Set(),records=[],all=[];
 for(const object of objects){
  const d=object.value;if(d?.source!=='provider-tail-dispatch')continue;
  const windowed=d.schema===2&&d.evidence_layout==='window-v1'&&Number.isSafeInteger(d.minute)&&d.minute>=0&&d.minute%60000===0&&uuid(d.dispatch_id)&&sha(d.archive_sha256);
  const validKey=windowed?object.key===windowPrefix(captureId,d.minute)+'counter/'+d.delivery_id+'.json':d.schema===1&&object.key===prefix+d.delivery_id+'.json';
  if(!validKey||d.qualification!==false||d.producer!==source||!uuid(d.delivery_id)||seen.has(d.delivery_id)||!Array.isArray(d.records)||d.records.length>1000||d.count!==d.records.length||!Number.isSafeInteger(d.received_at)||d.received_at>settledAt){failures.push('TailCounterDeliveryInvalid');continue;}seen.add(d.delivery_id);
  for(const r of d.records){
   if(r.script!==source||!Number.isSafeInteger(r.timestamp)||r.timestamp<0||windowed&&minuteOf(r.timestamp)!==d.minute){failures.push('TailCounterRecordInvalid');continue;}
   const selected=r.timestamp>=timeframe.from&&r.timestamp<=timeframe.to;
   if(selected||sha(r.attempt_id_sha256)){
    if(!versions.includes(r.version_id)||d.collector_version!==counterVersion)failures.push('TailCounterVersionMismatch');
    if(!['ok','exception','canceled','exceededMemory','exceededCpu'].includes(r.event?.outcome)||!Number.isFinite(r.event?.cpuTime)||!Number.isFinite(r.event?.wallTime)||r.event.cpuTime<0||r.event.wallTime<0)failures.push('TailCounterTerminalMetadataMissing');
   }
   all.push(r);if(selected)records.push(r);
  }
 }
 const ids=new Map(),canonical=[];
 for(const root of roots){if(root.script!==source||root.start<timeframe.from||root.start>timeframe.to||!root.terminal)continue;if(!versions.includes(root.version_id)||!sha(root.invocation_id_sha256))failures.push('TailCounterCanonicalIdentityInvalid');else{const old=ids.get(root.invocation_id_sha256);if(old){if(['script','version_id','start','end','handler','outcome'].some(k=>old[k]!==root[k])||JSON.stringify(old.event)!==JSON.stringify(root.event)||old.source_row_sha256!==root.source_row_sha256)failures.push('TailCounterCanonicalIdentityInvalid');}else{ids.set(root.invocation_id_sha256,root);canonical.push(root);}}}
 if(canonical.length!==records.length)failures.push('TailCounterPopulationMismatch');
 const frequencies=rows=>{const m=new Map();for(const value of rows)m.set(value,(m.get(value)??0)+1);return JSON.stringify([...m].sort((a,b)=>a[0]-b[0]));};
 if(frequencies(canonical.map(r=>r.start))!==frequencies(records.map(r=>r.timestamp)))failures.push('TailCounterTimestampPopulationMismatch');
 let witnessed=false;
 if(witnesses){
  if(!sha(witnesses.before)||!sha(witnesses.after)||witnesses.before===witnesses.after)failures.push('TailCounterWitnessInvalid');
  else {const before=all.filter(r=>r.attempt_id_sha256===witnesses.before),after=all.filter(r=>r.attempt_id_sha256===witnesses.after);witnessed=before.length===1&&after.length===1&&before[0].timestamp<timeframe.from&&after[0].timestamp>timeframe.to;if(!witnessed)failures.push('TailCounterWitnessMissing');}
 }
 if(!records.length&&!witnessed)failures.push('EmptyTailPopulationUnproven');
 const coverage=new DiagnosticCoverage([{id:source}]);for(const r of records)coverage.observe(source,'app',r.event);const observation=coverage.report().streams.find(s=>s.run===source&&s.role==='app');
 if(observation.gap)failures.push('TailCounterDiagnosticGap');
 return{schema:1,qualification:false,source:'independent-provider-tail-counter',dataset:'providerTailTerminalDeliveries',sampling_contract:'unfiltered-one-per-producer-invocation',worker:source,version_ids:versions,counter_version:counterVersion,timeframe:{...timeframe},count:records.length,canonical_count:canonical.length,resource_failures:observation.resource_failures,empty_witnessed:witnessed,complete:failures.length===0,failures:[...new Set(failures)]};
}

// Two independently re-listed closed-window observations are required. Native
// pagination alone cannot establish settlement while random keys are inserted.
export async function reconcileTailCounterSnapshots(first,second,scope){
 const failures=[],prefix='probe-v1/'+scope?.captureId+'/',window=scope?.timeframe;
 const valid=s=>s?.qualification===false&&s.capture_id===scope?.captureId&&uuid(s.collector_version)&&Number.isSafeInteger(s.snapshot_started_at)&&Number.isSafeInteger(s.snapshot_finished_at)&&s.snapshot_finished_at>=s.snapshot_started_at&&Array.isArray(s.objects)&&s.objects.length<=20000&&s.objects.every(o=>typeof o?.key==='string'&&o.key.startsWith(prefix)&&(o.value?.source!=='provider-tail-dispatch'||Array.isArray(o.value.records)&&o.value.records.length<=1000&&o.value.records.every(r=>r&&typeof r==='object'))&&(o.value?.rows==null||Array.isArray(o.value.rows)&&o.value.rows.length<=1000&&o.value.rows.every(r=>r&&typeof r==='object')))&&new Set(s.objects.map(o=>o.key)).size===s.objects.length;
 if(!valid(first)||!valid(second)||!window||first.snapshot_started_at<window.to+60000||second.snapshot_started_at<first.snapshot_finished_at+30000)return{qualification:false,complete:false,failures:['TailCounterSettlementInvalid']};
 const windowed=first.evidence_layout==='window-v1'||second.evidence_layout==='window-v1';
 if(windowed){for(const s of[first,second]){if(s.evidence_layout!=='window-v1')failures.push('TailCounterLayoutInvalid');for(const o of s.objects){if(o.value?.rejection)failures.push('TailCounterIngestionRejected');if(o.value?.rows){const d=o.value;if(d.schema!==2||d.evidence_layout!=='window-v1'||!Number.isSafeInteger(d.minute)||d.minute<0||d.minute%60000!==0||!o.key.startsWith(windowPrefix(scope.captureId,d.minute)+'batches/')||!sha(d.archive_sha256)||d.rows.some(r=>!Number.isSafeInteger(r.start)||r.start<d.minute||r.start>=d.minute+60000||!sha(r.source_row_sha256)))failures.push('TailCounterLayoutInvalid');}if(o.value?.source==='provider-tail-dispatch'&&o.value.schema!==2)failures.push('TailCounterLayoutInvalid');}}}
 const selected=s=>s.objects.flatMap(o=>o.value?.source==='provider-tail-dispatch'?o.value.records.filter(r=>r.timestamp>=window.from&&r.timestamp<=window.to||scope.witnesses&&[scope.witnesses.before,scope.witnesses.after].includes(r.attempt_id_sha256)).map(r=>JSON.stringify({key:o.key,producer:o.value.producer,collector_version:o.value.collector_version,record:r})):o.value?.rows?.filter(r=>r.script===scope.source&&r.start>=window.from&&r.start<=window.to).map(r=>JSON.stringify(r))??[]).sort();
 const digest=async v=>Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(JSON.stringify(v)))),b=>b.toString(16).padStart(2,'0')).join('');
 const hashes=[];for(const s of[first,second]){const roots=s.objects.flatMap(o=>(o.value?.rows??[]).filter(r=>r.terminal&&r.script===scope.source&&r.start>=window.from&&r.start<=window.to)),proof=reconcileTailCounter(s.objects,roots,{...scope,settledAt:s.snapshot_finished_at});failures.push(...proof.failures);hashes.push(await digest(selected(s)));}
 if(hashes[0]!==hashes[1])failures.push('TailCounterWindowUnsettled');
 return{schema:1,qualification:false,source:'independent-provider-tail-counter',complete:failures.length===0,window_sha256:hashes[1],observation_sha256:await Promise.all([first,second].map(digest)),failures:[...new Set(failures)]};
}
