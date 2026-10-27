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

// Persist only numeric diagnostics. Raw HTTP events, headers, bodies and stderr
// never reach stdout. The input frame exists only in bounded transient memory.
import{resourceFailureCode}from'../performance.mjs';
const events=new Set(['antfly.resource_failure','antfly.operation','antfly.trace','antfly.actor_started','antfly.coordinator','antfly.coordinator_group','antfly.coordinator_snapshot','antfly.coordinator_snapshot_native','antfly.checkpoint_hint','antfly.checkpoint_deferred','antfly.wal_prepare','antfly.wal_group','antfly.publication_feedback','antfly.reader_metadata','antfly.app_metadata']);
const ops=new Set(['coordinate','lookup','list','count','search','status','latest','mutate','publish','compact','reindex','enrich','gc_prepare','gc_trace','gc_wal','collect','collect_metadata']);
const spans=new Set(['parse','dispatch','kernel_queue','wasm_instantiate','wal_prepare','wal_commit','recovery','writer_rpc','queue_delivery','point_read_capacity_promotion','native:openReadView',...['objectCall','openReadView','beginPublication','openPublication','publicationCall','finalizePublication','abortPublication'].map(v=>'rpc:'+v)]);
const labels={role:new Set(['interactive','maintenance','app','writer','reader','builder']),runtime_role:new Set(['shared','app','app-reader','writer','reader','builder']),action:new Set(['begin','observe','observe_committed','read','commit','commit_wal','checkpoint','trace','retire_logs']),operation_outcome:new Set(['ok','error']),outcome:new Set(['ok','error','needs_admission']),owner_identity:new Set(['actor-version','ephemeral'])};
const stringAllowed=(key,value)=>key==='event'?events.has(value):key==='op'?ops.has(value):key==='name'?spans.has(value):key==='scope'?['http','coordinator','outbox','wal'].includes(value)||value.startsWith('runtime:')&&ops.has(value.slice(8)):labels[key]?.has(value);
const code=/\b(OutOfMemory|SQLITE_NOMEM|MemoryLimitExceeded|CPULimitExceeded|DeadlineExceeded|WorkLeaseLost|Canceled|CoordinatorRecovering|CoordinatorCommitBusy|ManifestReadLeaseExpired|IntegrityMismatch|ObjectIoBudgetExceeded|InvalidImmutableLog|ImmutableLogFenced|HostedWorkingSetBudgetExceeded)\b/;
const errorCode=value=>typeof value==='string'?(resourceFailureCode(value)??(value.includes('Queue is overloaded.')?'QueueOverloaded':value.includes('different request')?'CrossRequestIoContext':code.exec(value)?.[1]??'RedactedError')):'RedactedError';
function metric(value,depth=0){
 if(depth>4)return;
 if(typeof value==='number')return Number.isFinite(value)?value:undefined;
 if(typeof value==='boolean')return value;
 if(Array.isArray(value))return value.slice(0,64).map(v=>metric(v,depth+1)).filter(v=>v!==undefined);
 if(!value||typeof value!=='object')return;
 const out={};for(const[key,v]of Object.entries(value)){
  if(['headers','request','response','body','token','authorization','cookie','url','message','metadata'].includes(key.toLowerCase()))continue;
  if(typeof v==='string'&&stringAllowed(key,v)){out[key]=v;continue;}
  if(['trace_id','actor_instance_id'].includes(key)&&typeof v==='string'&&/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/.test(v)){out[key]=v;continue;}
  if(['error','failure_code'].includes(key)){if(v!=null)out[key]=errorCode(v);continue;}
  if(!/^[a-z_]{1,64}$/.test(key))continue;
  const safe=metric(v,depth+1);if(safe!==undefined)out[key]=safe;
 }return out;
}
export function sanitizeTailEvent(event){
 if(!event||typeof event!=='object')return;
 const safe={};if(['ok','exception','canceled','exceededMemory','exceededCpu'].includes(event.outcome))safe.outcome=event.outcome;
 const logs=[];for(const log of (event.logs??[]).slice(0,256))for(const raw of(log.message??[]).slice(0,32)){
  let value;try{value=JSON.parse(raw);}catch{const failure_code=resourceFailureCode(raw);if(failure_code)logs.push({timestamp:Number.isFinite(log.timestamp)?log.timestamp:undefined,message:[JSON.stringify({event:'antfly.resource_failure',failure_code})]});continue;}if(!events.has(value?.event))continue;
  logs.push({timestamp:Number.isFinite(log.timestamp)?log.timestamp:undefined,message:[JSON.stringify(metric(value))]});
 }
 if(logs.length)safe.logs=logs;
 const exceptions=(event.exceptions??[]).slice(0,16).map(e=>({name:['Error','TypeError','RangeError'].includes(e.name)?e.name:'Error',message:errorCode(e.message)}));if(exceptions.length)safe.exceptions=exceptions;
 return Object.keys(safe).length?safe:undefined;
}
export function tailFrames(emit,{maxBytes=1024*1024}={}){
 let depth=0,quoted=false,escaped=false,frame='',dropped=false;
 return chunk=>{for(const ch of chunk){
  if(!depth){if(ch!=='{')continue;depth=1;frame='{';quoted=false;escaped=false;dropped=false;continue;}
  if(!dropped){frame+=ch;if(frame.length>maxBytes){frame='';dropped=true;}}
  if(quoted){if(escaped)escaped=false;else if(ch==='\\')escaped=true;else if(ch==='"')quoted=false;}
  else if(ch==='"')quoted=true;else if(ch==='{')depth++;else if(ch==='}')depth--;
  if(!depth&&!dropped){try{const safe=sanitizeTailEvent(JSON.parse(frame));if(safe)emit(safe);}catch{}frame='';}
 }};
}
