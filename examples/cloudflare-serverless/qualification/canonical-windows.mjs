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

// Versioned proof projection. Verbose sanitized data is archived separately;
// resource/unknown errors, truncation and physical I/O lifetimes stay canonical.
import{diagnosticBucketIdentity}from'./diagnostic-storage.mjs';
import{parseDiagnosticJson}from'./diagnostic-memory.mjs';
const enc=new TextEncoder();
export const canonicalLayout='window-v1';
export const minuteOf=t=>{if(!Number.isSafeInteger(t)||t<0)throw Error('TelemetryCanonicalTimestampInvalid');return Math.floor(t/60000)*60000;};
export const minuteName=t=>String(minuteOf(t)).padStart(16,'0');
export const proofPrefix=id=>'probe-v1/'+id+'/';
export const windowPrefix=(id,t)=>proofPrefix(id)+'windows/'+minuteName(t)+'/';
export async function sha(value){const bytes=typeof value==='string'?enc.encode(value):value;return Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',bytes)),b=>b.toString(16).padStart(2,'0')).join('');}
function critical(v){
 if(!v||typeof v!=='object')return true;
 if(v.event==='antfly.io_settled'||v.io_lifecycle_id!=null||v.io_draining===true)return true;
 for(const[k,x]of Object.entries(v)){
  if((k==='error'||k==='failure_code')&&x!=null)return true;
  if((k==='resource_failures'||k==='edge_cache_resource_errors')&&typeof x==='number'&&x>0)return true;
  if((k==='outcome'||k==='operation_outcome')&&x==='error')return true;
  if(x&&typeof x==='object'&&criticalNested(x))return true;
 }
 return false;
}
function criticalNested(v){if(Array.isArray(v))return v.some(x=>x&&typeof x==='object'&&critical(x));return critical(v);}
export function compactEvent(event={}){
 const out={};for(const key of['outcome','cpuTime','wallTime','scriptVersion','truncated','exceptions'])if(Object.hasOwn(event,key))out[key]=structuredClone(event[key]);
 const logs=(event.logs??[]).filter(log=>(log.message??[]).some(message=>{try{return critical(parseDiagnosticJson(message));}catch{return true;}}));
 if(logs.length)out.logs=structuredClone(logs);
 return out;
}
export async function compactRows(rows){
 const out=[];
 for(const row of rows){
  minuteOf(row.start);const event=compactEvent(row.event),unsafe=row.dropped||row.warning&&(row.terminal||row.warning_type!=='span_not_ended')||event.truncated||event.exceptions?.length||event.logs?.length||['exception','exceededCpu','exceededMemory'].includes(event.outcome);
  if(!row.terminal&&!unsafe)continue;
  const value={};for(const key of['signal','script','version_id','start','end','handler','outcome','terminal','invocation_id_sha256','trace_id_sha256','span_id_sha256','parent_span_id_sha256','attempt_id_sha256','dropped','warning','warning_type'])if(Object.hasOwn(row,key))value[key]=row[key];
  value.event=event;value.source_row_sha256=await sha(JSON.stringify(row));out.push(value);
 }
 return out;
}
async function prepareImmutable(key,value){const bytes=enc.encode(JSON.stringify(value));if(bytes.length>4*1024*1024)throw Error('TelemetryCanonicalBudgetExceeded');return {key,body:bytes,sha256:await sha(bytes)};}
async function immutable(bucket,key,value){return immutablePrepared(bucket,await prepareImmutable(key,value));}
async function immutablePrepared(bucket,{key,body:bytes,sha256}){
 if(bytes.length>4*1024*1024)throw Error('TelemetryCanonicalBudgetExceeded');
 const result=await bucket.put(key,bytes,{onlyIf:{etagDoesNotMatch:'*'},httpMetadata:{contentType:'application/json'}});
 if(!result){const old=await bucket.get(key);if(!old||old.size!==bytes.length||await sha(new Uint8Array(await new Response(old.body).arrayBuffer()))!==sha256)throw Error('TelemetryCanonicalCollision');}
 return{key,bytes:bytes.length,sha256};
}
const readyLayouts=new WeakMap();
export async function ensureCanonicalLayout(env){
 if(env.WINDOWED_EVIDENCE!=='true'||!env.ARCHIVE||env.ARCHIVE===env.LOGS)throw Error('TelemetryCanonicalArchiveRequired');
 const identity=diagnosticBucketIdentity(env.LOGS),key=env.CAPTURE_ID+'/'+env.SOURCE_WORKER;let ready=readyLayouts.get(identity);if(ready?.has(key))return;
 const value={schema:1,evidence_layout:canonicalLayout,capture_id:env.CAPTURE_ID,producer:env.SOURCE_WORKER};
 await immutable(env.LOGS,proofPrefix(env.CAPTURE_ID)+'LAYOUT.json',value);
 if(!ready){ready=new Set();readyLayouts.set(identity,ready);}if(ready.size<64)ready.add(key);
}
export async function prepareCanonicalRows(env,value){
 if(env.WINDOWED_EVIDENCE!=='true'||!env.ARCHIVE||env.ARCHIVE===env.LOGS)throw Error('TelemetryCanonicalArchiveRequired');
 const rows=await compactRows(value.rows);if(new Set(rows.map(row=>minuteOf(row.start))).size>32)throw Error('TelemetryCanonicalPartitionBudgetExceeded');const archiveBytes=enc.encode(JSON.stringify(value)),archiveHash=await sha(archiveBytes);
 if(archiveBytes.length>4*1024*1024)throw Error('TelemetrySanitizedBudgetExceeded');
 const archive={key:'archive-v1/'+env.CAPTURE_ID+'/otel/'+archiveHash+'.json',body:archiveBytes,sha256:archiveHash},partitions=[];let bytes=archiveBytes.length;
 const groups=new Map();for(const row of rows){const minute=minuteOf(row.start),group=groups.get(minute)??[];group.push(row);groups.set(minute,group);}
 for(const[minute,selected]of groups){const body={schema:2,qualification:false,evidence_layout:canonicalLayout,signal:value.signal,collector_version:value.collector_version,received_at:value.received_at,raw_batch_sha256:value.raw_batch_sha256,archive_sha256:archiveHash,minute,rows:selected},digest=await sha(JSON.stringify(body)),part=await prepareImmutable(windowPrefix(env.CAPTURE_ID,minute)+'batches/'+digest+'.json',body);bytes+=part.body.length;partitions.push(part);}
 return {archive,partitions,bytes};
}
export async function publishCanonicalRows(env,prepared){await ensureCanonicalLayout(env);await immutablePrepared(env.ARCHIVE,prepared.archive);for(const part of prepared.partitions)await immutablePrepared(env.LOGS,part);}
export async function persistCanonicalRows(env,value){return publishCanonicalRows(env,await prepareCanonicalRows(env,value));}
export async function prepareCanonicalTail(env,value,witnesses=[]){
 if(env.WINDOWED_EVIDENCE!=='true'||!env.ARCHIVE||env.ARCHIVE===env.LOGS)throw Error('TelemetryCanonicalArchiveRequired');
 if(witnesses.some(v=>typeof v!=='string'||! /^[a-f0-9]{64}$/.test(v))||witnesses.length>8)throw Error('TelemetryCanonicalWitnessInvalid');
 if(new Set(value.records.map(row=>minuteOf(row.timestamp))).size>32)throw Error('TelemetryCanonicalPartitionBudgetExceeded');
 const archiveHash=await sha(JSON.stringify(value)),archive=await prepareImmutable('archive-v1/'+env.CAPTURE_ID+'/counter/'+archiveHash+'.json',value);
 const groups=new Map();for(const row of value.records){const minute=minuteOf(row.timestamp),group=groups.get(minute)??[];group.push({...row,event:compactEvent(row.event)});groups.set(minute,group);}
 const partitions=[];let bytes=archive.body.length;
 for(const[minute,records]of groups){
  const delivery_id=crypto.randomUUID(),body={...value,schema:2,evidence_layout:canonicalLayout,dispatch_id:value.delivery_id,delivery_id,archive_sha256:archiveHash,minute,count:records.length,records},key=windowPrefix(env.CAPTURE_ID,minute)+'counter/'+delivery_id+'.json',counter=await prepareImmutable(key,body),pointers=[];bytes+=counter.body.length;
  for(const record of records)if(witnesses.includes(record.attempt_id_sha256)){const pointer=await prepareImmutable(proofPrefix(env.CAPTURE_ID)+'witnesses/'+record.attempt_id_sha256+'/'+delivery_id+'.json',{schema:1,evidence_layout:canonicalLayout,capture_id:env.CAPTURE_ID,producer:env.SOURCE_WORKER,attempt_id_sha256:record.attempt_id_sha256,timestamp:record.timestamp,counter_key:key});bytes+=pointer.body.length;pointers.push(pointer);}
  partitions.push({counter,pointers});
 }
 return {archive,partitions,bytes};
}
export async function publishCanonicalTail(env,prepared){
 await ensureCanonicalLayout(env);await immutablePrepared(env.ARCHIVE,prepared.archive);
 for(const p of prepared.partitions){await immutablePrepared(env.LOGS,p.counter);for(const pointer of p.pointers)await immutablePrepared(env.LOGS,pointer);}
}
export async function persistCanonicalTail(env,value,witnesses=[]){return publishCanonicalTail(env,await prepareCanonicalTail(env,value,witnesses));}
